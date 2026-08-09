from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from mimic_iv_pipeline.errors import MIMICPipelineError


@dataclass(frozen=True)
class VariableSpec:
    name: str
    role: str
    family: str | None
    unit: str
    aggregations: tuple[str, ...]
    eligibility: str


@dataclass(frozen=True)
class PlausibilityRange:
    minimum: float | None = None
    maximum: float | None = None
    minimum_inclusive: bool = True
    maximum_inclusive: bool = True

    def contains(self, value: float) -> bool:
        if self.minimum is not None:
            if value < self.minimum or (
                value == self.minimum and not self.minimum_inclusive
            ):
                return False
        if self.maximum is not None:
            if value > self.maximum or (
                value == self.maximum and not self.maximum_inclusive
            ):
                return False
        return True


@dataclass(frozen=True)
class LoadedProfile:
    profile_path: Path
    profile: dict[str, Any]
    contracts: dict[str, dict[str, Any]]
    registries: dict[str, dict[str, Any]]
    variables: dict[str, VariableSpec]
    ranges: dict[str, PlausibilityRange]


def load_yaml(path: Path) -> dict[str, Any]:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise MIMICPipelineError(f"Cannot load YAML contract: {path}") from exc
    if not isinstance(value, dict):
        raise MIMICPipelineError(f"YAML contract must be a mapping: {path}")
    return value


def _resolved_files(
    profile_path: Path, mapping: object, label: str
) -> dict[str, dict[str, Any]]:
    if not isinstance(mapping, dict) or not mapping:
        raise MIMICPipelineError(f"Profile {label} must be a non-empty mapping")
    output: dict[str, dict[str, Any]] = {}
    for name, relative in mapping.items():
        if not isinstance(name, str) or not isinstance(relative, str):
            raise MIMICPipelineError(f"Profile {label} paths must be strings")
        path = (profile_path.parent / relative).resolve()
        output[name] = load_yaml(path)
    return output


def _variable_specs(registry: dict[str, Any]) -> dict[str, VariableSpec]:
    output: dict[str, VariableSpec] = {}
    for scope in ("static", "dynamic"):
        rows = registry.get(scope)
        if not isinstance(rows, dict):
            raise MIMICPipelineError(f"Variable registry is missing {scope}")
        for name, raw in rows.items():
            if not isinstance(raw, dict):
                raise MIMICPipelineError(f"Invalid variable specification: {name}")
            output[name] = VariableSpec(
                name=name,
                role=str(raw.get("role")),
                family=raw.get("family"),
                unit=str(raw.get("unit")),
                aggregations=tuple(raw.get("aggregation", ())),
                eligibility=str(raw.get("eligibility")),
            )
    return output


def _ranges(registry: dict[str, Any]) -> dict[str, PlausibilityRange]:
    rows = registry.get("ranges")
    if not isinstance(rows, dict):
        raise MIMICPipelineError("Plausibility registry is missing ranges")
    output: dict[str, PlausibilityRange] = {}
    for name, raw in rows.items():
        if not isinstance(raw, dict):
            raise MIMICPipelineError(f"Invalid plausibility range: {name}")
        output[name] = PlausibilityRange(
            minimum=raw.get("minimum"),
            maximum=raw.get("maximum"),
            minimum_inclusive=bool(raw.get("minimum_inclusive", True)),
            maximum_inclusive=bool(raw.get("maximum_inclusive", True)),
        )
    return output


def load_profile(path: Path) -> LoadedProfile:
    profile_path = path.resolve()
    profile = load_yaml(profile_path)
    if profile.get("status") not in {
        "approved_for_local_synthetic_implementation_only",
        "approved_for_operator_executed_production_candidate",
    }:
        raise MIMICPipelineError("Profile has no recognized execution approval")
    contracts = _resolved_files(profile_path, profile.get("contracts"), "contracts")
    registries = _resolved_files(profile_path, profile.get("registries"), "registries")
    variables = _variable_specs(registries["variables"])
    ranges = _ranges(registries["ranges"])

    expected_sources = {
        name for name, spec in variables.items() if spec.family is not None
    }
    expected_sources.update({"height_cm", "weight_kg"})
    sourced = set(registries["item_sources"].get("chartevents", {}))
    sourced.update(registries["item_sources"].get("labevents", {}))
    sourced.update(registries["item_sources"].get("derived", {}))
    if expected_sources != sourced:
        raise MIMICPipelineError(
            "Selected source coverage mismatch: "
            f"missing={sorted(expected_sources-sourced)}, "
            f"extra={sorted(sourced-expected_sources)}"
        )
    for resolution in ("blocked_15m", "blocked_8h"):
        contract = contracts[resolution]
        if contract.get("source_artifact") != "canonical_selected_events_0_1":
            raise MIMICPipelineError(f"{resolution} is not bound to canonical events")
        if "blocked_15m" not in contract.get("prohibited_sources", []):
            raise MIMICPipelineError(f"{resolution} does not prohibit 15-minute input")
    return LoadedProfile(
        profile_path=profile_path,
        profile=profile,
        contracts=contracts,
        registries=registries,
        variables=variables,
        ranges=ranges,
    )
