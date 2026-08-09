"""Read-only validation of translated artifacts before downstream use."""

from asic_pipeline.translated_input.config import (
    TranslatedInputConfig,
    load_translated_input_config,
)

__all__ = ["TranslatedInputConfig", "load_translated_input_config"]
