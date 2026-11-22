import pytest
from pydantic import ValidationError

from atlasiq.db.models import SourceCategory
from atlasiq.sources.registry import SourceAlreadyRegistered, SourceConfig, SourceNotFound, SourceRegistry


@pytest.mark.parametrize("overrides", [
    {"category": "OTHER"}, {"id": "Bad ID"}, {"rate_limit_config": {"requests_per_minute": 0}},
    {"rate_limit_config": {"requests_per_minute": "1"}}, {"base_url": "https://user:pw@example.org"},
    {"enabled": "false"}, {"schedule_config": []},
])
def test_invalid_registration(source_config, overrides):
    with pytest.raises(ValidationError):
        SourceConfig.model_validate(source_config | overrides)


def test_valid_sample_config():
    from pathlib import Path

    from pydantic import TypeAdapter

    sample_path = Path(__file__).resolve().parents[1] / "docs" / "example_sources.json"
    sources = TypeAdapter(list[SourceConfig]).validate_json(sample_path.read_bytes())
    assert all(not source.enabled for source in sources)
    assert SourceCategory.RESEARCH_PAPER in {source.category for source in sources}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_registry_workflow(pool, source_config):
    async with pool.acquire() as connection:
        registry = SourceRegistry(connection)
        source = SourceConfig.model_validate(source_config)
        row = await registry.register(source)
        assert row["rate_limit_config"] == {"requests_per_minute": 4, "max_concurrency": 1}
        assert await registry.get(source.id) == row
        with pytest.raises(SourceAlreadyRegistered):
            await registry.register(source)
        with pytest.raises(SourceAlreadyRegistered):
            await registry.register(SourceConfig.model_validate(source_config | {
                "id": "duplicate-name", "name": source.name.upper(),
            }))
        second = SourceConfig.model_validate(source_config | {
            "id": "test-news", "name": "Fixture News", "category": "NEWS", "enabled": True,
        })
        await registry.register(second)
        assert [item["id"] for item in await registry.list()] == [second.id, source.id]
        assert await registry.list(category=SourceCategory.JOB) == []
        assert await registry.list(category=source.category, enabled=False) == [row]
        enabled = await registry.set_enabled(source.id, True)
        assert enabled["enabled"] and enabled["updated_at"] >= row["updated_at"]
        assert await registry.set_enabled(source.id, True) == enabled
        assert not (await registry.set_enabled(source.id, False))["enabled"]
        with pytest.raises(SourceNotFound):
            await registry.set_enabled("missing", True)
