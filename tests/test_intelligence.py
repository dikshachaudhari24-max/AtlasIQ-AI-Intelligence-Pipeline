import json
import pytest
from multidict import CIMultiDict
from pydantic import SecretStr, ValidationError

from atlasiq.config.settings import Settings
from atlasiq.intelligence.contracts import InvalidOutput, checked_facts, merge_facts
from atlasiq.intelligence.evidence import semantic_chunks, source_text
from atlasiq.intelligence.providers import APIProvider, ProviderConfig, ProviderError, configured_providers

DB = "postgresql://test@localhost/testdb"


def response(*facts, kind="RESEARCH_PAPER"):
    return json.dumps({"schema_version": "1.0", "record_type": kind, "facts": facts})


def fact(field="technology", value="NLP", quote="NLP enriches source data.", confidence=None):
    return {"field": field, "value": value, "quote": quote, "confidence": confidence}


def test_structured_extraction_and_canonical_technology_merge():
    first = checked_facts(response(fact(confidence=0.95)), "NLP  enriches source data.", "RESEARCH_PAPER")
    second = checked_facts(response(fact(value="natural language processing",
                                         quote="They use natural language processing.")),
                           "They use natural language processing.", "RESEARCH_PAPER")
    [merged] = merge_facts([first, second])
    assert merged["normalized"] == "natural language processing"
    assert merged["evidence_status"] == "source_quote_matched"
    assert merged["model_confidence"] == 0.95
    assert merged["quote"] == "NLP enriches source data."


@pytest.mark.parametrize("payload, code", [
    ("{", "INVALID_STRUCTURED_OUTPUT"),
    (response(fact(confidence=1.5)), "INVALID_STRUCTURED_OUTPUT"),
    (response(fact(), kind="NEWS"), "RECORD_TYPE_MISMATCH"),
    (response(fact(value="robots")), "UNSUPPORTED_EVIDENCE"),
    (response(fact(value="AI", quote="ai")), "UNSUPPORTED_EVIDENCE"),
    (response(fact(value="USD 9 million", quote="NLP enriches source data.", confidence=1)), "UNSUPPORTED_EVIDENCE"),
])
def test_invalid_or_fabricated_facts_fail_even_with_confidence(payload, code):
    with pytest.raises(InvalidOutput, match=code):
        checked_facts(payload, "NLP enriches source data. It is a paid service.", "RESEARCH_PAPER")


def test_missing_facts_remain_empty_and_category_is_controlled():
    assert checked_facts(response(), "A paper title", "RESEARCH_PAPER") == []
    [category] = checked_facts(response(fact("category", "Unlisted", "Unlisted label")),
                              "Unlisted label", "RESEARCH_PAPER")
    assert category["normalized"] is None


def test_utf8_chunk_limits_and_evidence_projection():
    raw = {"description": "Two sentences. The next one.", "access": "sensitive fixture token"}
    evidence = source_text({"kind": "STARTUP", "title": "Test startup", "raw_content": json.dumps(raw)})
    assert "Two sentences" in evidence and "sensitive" not in evidence
    original = "界" * 250
    chunks = semantic_chunks(original, 128, 8)
    assert "".join(chunks) == original and all(len(chunk.encode()) <= 128 for chunk in chunks)
    with pytest.raises(ValueError, match="CHUNK_BUDGET_EXCEEDED"):
        semantic_chunks(original, 128, 2)


def test_provider_order_and_missing_credentials(clean_env):
    settings = Settings(database_url=DB)
    assert configured_providers(settings) == []
    ordered = Settings(database_url=DB, llm_provider_order=" DeepSeek , GROQ, gemini ",
                       groq_api_key="fixture-groq", gemini_api_key="fixture-gemini")
    providers = configured_providers(ordered)
    assert [provider.name for provider in providers] == ["groq", "gemini"]
    assert "fixture-groq" not in repr(providers)
    with pytest.raises(ValidationError, match="distinct providers"):
        Settings(database_url=DB, llm_provider_order="groq,groq")


class MockPayload:
    def __init__(self, result):
        self.result = result

    async def iter_chunked(self, size):
        yield json.dumps(self.result).encode()


class MockResponse:
    def __init__(self, status=200, headers=None, result=None):
        self.status = status
        self.headers = CIMultiDict(headers or {})
        self.content = MockPayload(result)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        pass


class MockSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, url, **request):
        self.calls.append((url, request))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["gemini", "groq", "deepseek"])
async def test_provider_wire_formats_and_token_metadata(name):
    payload = (
        {"candidates": [{"content": {"parts": [{"text": "{}"}]}}],
         "usageMetadata": {"promptTokenCount": 12, "candidatesTokenCount": 3}} if name == "gemini"
        else {"choices": [{"message": {"content": "{}"}}],
              "usage": {"prompt_tokens": 12, "completion_tokens": 3}}
    )
    session = MockSession(MockResponse(result=payload))
    provider = APIProvider(session, ProviderConfig(name, "test-model", SecretStr("mock-secret")),
                           Settings(_env_file=None, database_url=DB))
    reply = await provider.generate("RESEARCH_PAPER", "untrusted fixture")
    assert (reply.text, reply.input_tokens, reply.output_tokens) == ("{}", 12, 3)
    url, args = session.calls[0]
    assert "mock-secret" not in url and args["allow_redirects"] is False
    data = json.loads(args["data"])
    if name == "gemini":
        assert data["generationConfig"]["responseMimeType"] == "application/json"
        assert args["headers"]["x-goog-api-key"] == "mock-secret"
    else:
        assert args["headers"]["Authorization"] == "Bearer mock-secret"
        assert data["response_format"] == {"type": "json_object"}


@pytest.mark.asyncio
@pytest.mark.parametrize("reply,code,status,delay", [
    (MockResponse(429, {"Retry-After": "90"}), "PROVIDER_HTTP_429", 429, 90),
    (MockResponse(413), "PROVIDER_HTTP_413", 413, 0),
    (MockResponse(result={"bad": "shape"}), "INVALID_PROVIDER_RESPONSE", 200, 0),
    (TimeoutError(), "PROVIDER_TIMEOUT", None, 0),
])
async def test_provider_status_timeout_and_payload_classification(reply, code, status, delay):
    provider = APIProvider(MockSession(reply), ProviderConfig("groq", "model", SecretStr("fixture")),
                           Settings(_env_file=None, database_url=DB))
    with pytest.raises(ProviderError, match=code) as error:
        await provider.generate("NEWS", "Fixture only")
    assert (error.value.status, error.value.delay) == (status, delay)


@pytest.mark.asyncio
async def test_local_request_budget_prevents_provider_call():
    session = MockSession(None)
    provider = APIProvider(session, ProviderConfig("gemini", "model", SecretStr("fixture")),
                           Settings(_env_file=None, database_url=DB, llm_request_bytes=4096))
    with pytest.raises(ProviderError, match="LOCAL_PAYLOAD_TOO_LARGE"):
        await provider.generate("NEWS", "é" * 4000)
    assert not session.calls
