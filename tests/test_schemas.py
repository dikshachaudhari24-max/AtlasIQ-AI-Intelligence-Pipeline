import json
from datetime import UTC

import pytest
from pydantic import ValidationError

from atlasiq.schemas.entities import Job, News, PricingModel, Product, ResearchPaper, Startup
from atlasiq.utils.urls import canonicalize_url


def test_startup_json_and_provenance(startup_json):
    record = Startup.model_validate_json(json.dumps(startup_json))
    assert record.source.url == startup_json["source"]["url"]
    assert record.content.data.employeeCount == 0
    assert record.collectedAt.tzinfo is UTC
    assert record.model_dump(mode="json")["collectedAt"] == "2026-01-10T08:30:00Z"


@pytest.mark.parametrize("value", [None, "0", True, -1])
def test_startup_employee_count_types(startup_json, value):
    startup_json["content"]["data"]["employeeCount"] = value
    if value is None:
        assert Startup.model_validate(startup_json).content.data.employeeCount is None
    else:
        with pytest.raises(ValidationError):
            Startup.model_validate(startup_json)


@pytest.mark.parametrize("key,value", [
    ("recordType", "PRODUCT"), ("schemaVersion", 1.0), ("schemaVersion", "3.0"),
    ("collectedAt", "2026-01-10T14:00:00"), ("collectedAt", "2026-99-10T14:00:00Z"),
    ("collectedAt", "2026-01-10T14:00:00+01:80"),
    ("collectedAt", 1768032000), ("collectedAt", "1768032000"),
])
def test_invalid_startup_contract(startup_json, key, value):
    startup_json[key] = value
    with pytest.raises(ValidationError):
        Startup.model_validate(startup_json)


def test_missing_provenance(startup_json):
    startup_json.pop("source")
    with pytest.raises(ValidationError, match="source"):
        Startup.model_validate(startup_json)


@pytest.mark.parametrize("url", [
    "ftp://example.org/paper", "https://name:secret@example.org/", "https://example.org/with space", "/item",
    "https:example.org",
])
def test_invalid_source_url(startup_json, url):
    startup_json["source"]["url"] = url
    with pytest.raises(ValidationError):
        Startup.model_validate(startup_json)


def test_url_identity_preserves_query_order_and_drops_fragment():
    assert canonicalize_url("HTTPS://EXAMPLE.ORG:443/a?b=2&a=1#x") == "https://example.org/a?b=2&a=1"


@pytest.mark.parametrize("pricing", list(PricingModel))
def test_product_pricing_enum(startup_json, pricing):
    obj = startup_json | {
        "recordType": "PRODUCT",
        "content": {"startupName": "Fixture Co.", "pricingModel": pricing.value},
    }
    assert Product.model_validate(obj).content.pricingModel is pricing


@pytest.mark.parametrize("value", [None, "unknown", True])
def test_product_needs_verified_pricing(startup_json, value):
    obj = startup_json | {"recordType": "PRODUCT", "content": {"startupName": "Fixture", "pricingModel": value}}
    with pytest.raises(ValidationError):
        Product.model_validate(obj)


def test_optional_paper_metrics_distinguish_zero_and_missing(startup_json):
    content = {
        "title": "Test paper", "authors": ["Test Author"], "paper_url": "https://example.org/paper",
        "published_date": startup_json["collectedAt"],
    }
    obj = {"schemaVersion": "1.0", "recordType": "RESEARCH_PAPER", "source": startup_json["source"], "content": content}
    assert ResearchPaper.model_validate(obj).content.github_stars is None
    content["github_stars"] = 0
    with pytest.raises(ValidationError, match="requires a GitHub repository"):
        ResearchPaper.model_validate(obj)
    content.update(github_url="https://github.com/example/example", github_stars=0)
    assert ResearchPaper.model_validate(obj).content.github_stars == 0


def test_job_and_news_publication_types(startup_json):
    date = startup_json["collectedAt"]
    base = {"schemaVersion": "1.0", "source": startup_json["source"]}
    job_content = {"company": "Fixture", "role_family": "Research", "date": date, "is_remote": False}
    job = base | {"recordType": "JOB", "content": job_content}
    assert Job.model_validate(job).content.is_remote is False
    for value in (None, "false", 0):
        job_content["is_remote"] = value
        with pytest.raises(ValidationError):
            Job.model_validate(job)
    news = base | {
        "recordType": "NEWS", "content": {"title": "Fixture", "fullText": "Text", "publishedAt": date},
        "collectedAt": date,
    }
    assert News.model_validate(news).content.publishedAt.tzinfo is UTC
