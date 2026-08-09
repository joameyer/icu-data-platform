class ASICPipelineError(Exception):
    """Base exception for expected pipeline failures."""


class ConfigurationError(ASICPipelineError):
    """Raised when executable configuration is invalid."""


class InventoryError(ASICPipelineError):
    """Raised when raw inventory evidence cannot be collected safely."""


class IngestionError(ASICPipelineError):
    """Raised when lossless raw ingestion cannot proceed safely."""


class SchemaTokenError(ASICPipelineError):
    """Raised when schema/token evidence cannot be inventoried safely."""


class HarmonizationError(ASICPipelineError):
    """Raised when a harmonization rule or gate cannot be applied safely."""


class PublicationBlockedError(ASICPipelineError):
    """Raised when a requested publication has unresolved blocking findings."""
