"""Pipeline-specific exceptions with concise user-facing messages."""


class ASICPipelineError(Exception):
    """Base class for expected pipeline failures."""


class ConfigurationError(ASICPipelineError):
    """Raised when a pipeline configuration is incomplete or unsafe."""


class ContractError(ASICPipelineError):
    """Raised when an input-contract definition is invalid."""


class InputReadError(ASICPipelineError):
    """Raised when a pooled input artifact cannot be opened."""

