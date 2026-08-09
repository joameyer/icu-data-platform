from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from asic_pipeline.inventory.policy import InventoryPolicy


@dataclass(frozen=True)
class FileCandidate:
    absolute_path: Path
    relative_path: str
    hospital_folder: str | None
    source_filename: str
    classification: str
    filename_pattern_scope: str | None
    filename_stay_id: str | None


@dataclass(frozen=True)
class DiscoveryResult:
    files: tuple[FileCandidate, ...]
    checkpoint_directories_pruned: tuple[str, ...]
    untrusted_pooled_directories_pruned: tuple[str, ...]
    unknown_directories: tuple[str, ...]
    observed_hospital_folders: tuple[str, ...]


def _relative(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _classify_file(
    path: Path,
    root: Path,
    hospital_folder: str | None,
    policy: InventoryPolicy,
) -> FileCandidate:
    filename = path.name
    classification = "unknown_file"
    scope: str | None = None
    stay_id: str | None = None
    if hospital_folder is None:
        if filename in policy.known_root_metadata_files:
            classification = "known_metadata"
    elif filename == policy.default_static_filename:
        classification = "selected_static_candidate"
    elif filename in policy.additional_static_candidate_filenames:
        classification = "additional_static_candidate"
    elif path.suffix.casefold() == ".zip":
        classification = "archive_candidate"
    elif path.suffix.casefold() == ".csv":
        pattern = policy.dynamic_pattern_for(hospital_folder)
        match = pattern.fullmatch(filename)
        if match is None:
            classification = "unclassified_data_file"
        else:
            classification = "dynamic_candidate"
            scope = (
                hospital_folder
                if hospital_folder in dict(policy.dynamic_filename_patterns)
                else "default"
            )
            stay_id = match.group("stay_id")
    return FileCandidate(
        absolute_path=path,
        relative_path=_relative(path, root),
        hospital_folder=hospital_folder,
        source_filename=filename,
        classification=classification,
        filename_pattern_scope=scope,
        filename_stay_id=stay_id,
    )


def discover_raw_files(root: Path, policy: InventoryPolicy) -> DiscoveryResult:
    checkpoint_pruned: list[str] = []
    pooled_pruned: list[str] = []
    unknown_directories: list[str] = []
    files: list[FileCandidate] = []
    observed_hospitals: set[str] = set()

    for current_text, directory_names, filenames in os.walk(root, topdown=True):
        current = Path(current_text)
        relative_current = current.relative_to(root)
        depth = len(relative_current.parts)

        retained_directories: list[str] = []
        for directory_name in sorted(directory_names):
            candidate = current / directory_name
            relative_candidate = _relative(candidate, root)
            if directory_name == policy.checkpoint_directory_name:
                checkpoint_pruned.append(relative_candidate)
                continue
            if depth == 0 and directory_name == policy.untrusted_pooled_directory_name:
                pooled_pruned.append(relative_candidate)
                continue
            if candidate.is_symlink():
                unknown_directories.append(relative_candidate)
                continue
            if depth == 0 and directory_name in policy.expected_hospital_folders:
                observed_hospitals.add(directory_name)
                retained_directories.append(directory_name)
                continue
            unknown_directories.append(relative_candidate)
        directory_names[:] = retained_directories

        hospital_folder = relative_current.parts[0] if depth == 1 else None
        for filename in sorted(filenames):
            path = current / filename
            files.append(
                _classify_file(path, root, hospital_folder, policy)
            )

    return DiscoveryResult(
        files=tuple(files),
        checkpoint_directories_pruned=tuple(sorted(checkpoint_pruned)),
        untrusted_pooled_directories_pruned=tuple(sorted(pooled_pruned)),
        unknown_directories=tuple(sorted(unknown_directories)),
        observed_hospital_folders=tuple(sorted(observed_hospitals)),
    )
