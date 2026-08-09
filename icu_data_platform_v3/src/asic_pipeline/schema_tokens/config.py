from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError
from asic_pipeline.ingestion.config import IngestionConfig, load_ingestion_config


@dataclass(frozen=True)
class SchemaTokenConfig:
    ingestion: IngestionConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.ingestion.dataset_context


def load_schema_token_config(path: str | Path) -> SchemaTokenConfig:
    config_path = Path(path).expanduser().resolve()
    ingestion = load_ingestion_config(config_path)
    raw = load_yaml_mapping(config_path, "Schema-token inventory configuration")
    policy_value = raw.get("schema_token_policy")
    if policy_value is None:
        raise ConfigurationError("config.schema_token_policy is required")
    policy_path = resolve_path(
        required_string(raw, "schema_token_policy", "config"),
        config_path,
    )
    return SchemaTokenConfig(ingestion=ingestion, policy_path=policy_path)

