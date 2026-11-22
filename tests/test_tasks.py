import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest

from atlasiq.db.connection import transaction
from atlasiq.tasks.queue import LeaseLost, TaskQueue

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_enqueue_claim_renew_complete_and_replay(pool, disposable_settings, saved_document):
    async with pool.acquire() as connection:
        queue = TaskQueue(connection, disposable_settings)
        document_id = saved_document["id"]
        created = await queue.enqueue_task(document_id, "extract", "1")
        assert created["status"] == "PENDING" and created["attempts"] == 0
        assert await queue.enqueue_task(document_id, "extract", "1", max_attempts=8) == created
        assert await queue.claim_tasks("worker-a", stage="normalize") == []
        [claimed] = await queue.claim_tasks("worker-a", stage="extract")
        assert claimed["id"] == created["id"]
        assert claimed["attempts"] == 1 and claimed["lease_owner"] == "worker-a"
        with pytest.raises(LeaseLost):
            await queue.renew_lease(created["id"], uuid4())
        renewed = await queue.renew_lease(created["id"], claimed["claim_token"])
        assert renewed["lease_expires_at"] >= claimed["lease_expires_at"]
        completed = await queue.complete_task(created["id"], claimed["claim_token"])
        assert completed["status"] == "COMPLETED" and completed["lease_owner"] is None
        assert await queue.complete_task(created["id"], claimed["claim_token"]) == completed
        assert await queue.enqueue_task(document_id, "extract", "1") == completed
        assert await queue.get_task_status(created["id"]) == completed
        assert await queue.status_counts() == {
            "PENDING": 0, "CLAIMED": 0, "RETRY": 0, "FAILED": 0, "COMPLETED": 1,
        }


async def test_competing_workers_skip_locked_work(pool, disposable_settings, saved_document):
    async with pool.acquire() as first, pool.acquire() as second:
        queue_a = TaskQueue(first, disposable_settings)
        queue_b = TaskQueue(second, disposable_settings)
        document_id = saved_document["id"]
        await queue_a.enqueue_task(document_id, "extract", "1")
        await queue_a.enqueue_task(document_id, "extract", "2")
        async with first.transaction():
            [claim_a] = await queue_a.claim_tasks("worker-a")
            [claim_b] = await asyncio.wait_for(queue_b.claim_tasks("worker-b", limit=2), timeout=3)
            assert claim_a["id"] != claim_b["id"]
            assert claim_a["lease_owner"] == "worker-a" and claim_b["lease_owner"] == "worker-b"
        assert await queue_b.claim_tasks("worker-b") == []


async def test_concurrent_enqueue_and_claim_same_task(pool, disposable_settings, saved_document):
    async with pool.acquire() as first, pool.acquire() as second:
        queue_a = TaskQueue(first, disposable_settings)
        queue_b = TaskQueue(second, disposable_settings)
        document_id = saved_document["id"]
        a, b = await asyncio.gather(
            queue_a.enqueue_task(document_id, "extract", "1"),
            queue_b.enqueue_task(document_id, "extract", "1"),
        )
        assert a == b
        claims = await asyncio.gather(queue_a.claim_tasks("worker-a"), queue_b.claim_tasks("worker-b"))
        assert sorted(map(len, claims)) == [0, 1]


async def test_expired_lease_recovery_rejects_obsolete_claim(pool, disposable_settings, saved_document):
    async with pool.acquire() as connection:
        queue = TaskQueue(connection, disposable_settings)
        task = await queue.enqueue_task(saved_document["id"], "extract", "1", max_attempts=2)
        task_id = task["id"]
        [old] = await queue.claim_tasks("worker")
        assert await queue.recover_expired_leases() == []
        await connection.execute("""
            UPDATE public.ingestion_tasks SET lease_expires_at = clock_timestamp() - interval '1 second'
            WHERE id = $1
        """, task_id)
        for operation in (queue.renew_lease, queue.complete_task):
            with pytest.raises(LeaseLost):
                await operation(task_id, old["claim_token"])
        [recovered] = await queue.recover_expired_leases()
        assert recovered["status"] == "RETRY" and recovered["last_error"] == "LEASE_EXPIRED"
        [fresh] = await queue.claim_tasks("worker")
        assert fresh["claim_token"] != old["claim_token"]
        with pytest.raises(LeaseLost):
            await queue.complete_task(task_id, old["claim_token"])
        with pytest.raises(LeaseLost):
            await queue.fail_task(task_id, old["claim_token"], error_code="LATE_WORKER")
        assert fresh["attempts"] == 2
        await connection.execute("""
            UPDATE public.ingestion_tasks SET lease_expires_at = clock_timestamp() - interval '1 second'
            WHERE id = $1
        """, task_id)
        [failed] = await queue.recover_expired_leases()
        assert failed["status"] == "FAILED" and failed["next_retry_at"] is None
        assert await queue.claim_tasks("worker") == []


async def test_explicit_failures_back_off_cap_and_exhaust(pool, disposable_settings, saved_document):
    async with pool.acquire() as connection:
        queue = TaskQueue(connection, disposable_settings)
        task = await queue.enqueue_task(saved_document["id"], "extract", "1")
        for attempt, expected_delay in enumerate((2, 3, None), start=1):
            [claim] = await queue.claim_tasks("worker")
            assert claim["attempts"] == attempt
            failed = await queue.fail_task(task["id"], claim["claim_token"], error_code="UPSTREAM_TIMEOUT")
            assert failed["last_error"] == "UPSTREAM_TIMEOUT"
            assert failed["lease_owner"] is None and failed["claim_token"] is None
            assert await queue.claim_tasks("worker") == []
            if expected_delay is None:
                assert failed["status"] == "FAILED" and failed["next_retry_at"] is None
            else:
                assert failed["status"] == "RETRY"
                delay = failed["next_retry_at"] - failed["updated_at"]
                assert timedelta(seconds=expected_delay - 0.1) <= delay <= timedelta(seconds=expected_delay)
                await connection.execute("""
                    UPDATE public.ingestion_tasks SET next_retry_at = clock_timestamp() WHERE id = $1
                """, task["id"])


async def test_claim_and_complete_rollback_with_caller_transaction(pool, disposable_settings, saved_document):
    async with pool.acquire() as connection:
        queue = TaskQueue(connection, disposable_settings)
        task = await queue.enqueue_task(saved_document["id"], "extract", "1")
    with pytest.raises(RuntimeError, match="rollback"):
        async with transaction(pool) as connection:
            await TaskQueue(connection, disposable_settings).claim_tasks("worker")
            raise RuntimeError("rollback")
    async with pool.acquire() as connection:
        queue = TaskQueue(connection, disposable_settings)
        [claimed] = await queue.claim_tasks("worker")
        assert claimed["attempts"] == 1
    with pytest.raises(RuntimeError, match="rollback"):
        async with transaction(pool) as connection:
            await TaskQueue(connection, disposable_settings).complete_task(task["id"], claimed["claim_token"])
            raise RuntimeError("rollback")
    async with pool.acquire() as connection:
        assert (await TaskQueue(connection, disposable_settings).get_task_status(task["id"]))["status"] == "CLAIMED"
