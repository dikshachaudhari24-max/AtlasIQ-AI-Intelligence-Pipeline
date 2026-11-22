import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field, StringConstraints, model_validator

from atlasiq.utils.urls import validate_source_url

TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
                       r"(?:\.[0-9]{1,6})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])")


def aware_utc(value: datetime) -> datetime:
    if value.utcoffset() is None:
        raise ValueError("timestamp requires a UTC offset")
    try:
        return value.astimezone(UTC)
    except (OverflowError, ValueError):
        raise ValueError("timestamp cannot be represented in UTC") from None


def parse_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        return aware_utc(value)
    if not isinstance(value, str) or not TIMESTAMP.fullmatch(value):
        raise ValueError("use an ISO 8601 timestamp with a UTC offset")
    return aware_utc(datetime.fromisoformat(value))


Timestamp = Annotated[datetime, BeforeValidator(parse_timestamp)]
SourceUrl = Annotated[str, AfterValidator(validate_source_url)]
Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, pattern=r"^[^\x00]*$")]
Count = Annotated[int, Field(ge=0, le=9_223_372_036_854_775_807)]


class Contract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", hide_input_in_errors=True)


class SourceMetadata(Contract):
    name: Text
    url: SourceUrl


class CanonicalRecord(Contract):
    schemaVersion: Literal["1.0"]
    source: SourceMetadata


class StartupData(Contract):
    employeeCount: Count | None = None


class StartupContent(Contract):
    entityName: Text
    data: StartupData


class Startup(CanonicalRecord):
    recordType: Literal["STARTUP"]
    content: StartupContent
    collectedAt: Timestamp


class PricingModel(StrEnum):
    FREE = "FREE"
    FREEMIUM = "FREEMIUM"
    PAID = "PAID"
    ENTERPRISE = "ENTERPRISE"


class ProductContent(Contract):
    startupName: Text
    pricingModel: PricingModel = Field(strict=False)


class Product(CanonicalRecord):
    recordType: Literal["PRODUCT"]
    content: ProductContent
    collectedAt: Timestamp


class ResearchPaperContent(Contract):
    title: Text
    authors: list[Text]
    paper_url: SourceUrl
    github_url: SourceUrl | None = None
    github_stars: Count | None = None
    published_date: Timestamp

    @model_validator(mode="after")
    def require_repository_for_stars(self) -> "ResearchPaperContent":
        if self.github_stars is not None and self.github_url is None:
            raise ValueError("github_stars requires a GitHub repository URL")
        return self


class ResearchPaper(CanonicalRecord):
    recordType: Literal["RESEARCH_PAPER"]
    content: ResearchPaperContent


class JobContent(Contract):
    company: Text
    role_family: Text
    date: Timestamp
    is_remote: bool


class Job(CanonicalRecord):
    recordType: Literal["JOB"]
    content: JobContent


class NewsContent(Contract):
    title: Text
    fullText: Text
    publishedAt: Timestamp


class News(CanonicalRecord):
    recordType: Literal["NEWS"]
    content: NewsContent
    collectedAt: Timestamp
