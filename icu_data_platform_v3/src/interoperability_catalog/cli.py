from __future__ import annotations

import argparse
from pathlib import Path

from interoperability_catalog.contract import CatalogPaths, validate_catalog
from interoperability_catalog.evidence import write_mapping_evidence
from interoperability_catalog.handoff import export_analysis_alignment_handoff
from interoperability_catalog.render import write_alignment_table


def main() -> None:
    parser = argparse.ArgumentParser(description="ASIC--MIMIC interoperability catalog tools")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--project-root", type=Path, required=True)
    render = subparsers.add_parser("render")
    render.add_argument("--project-root", type=Path, required=True)
    evidence = subparsers.add_parser("audit-mimic-evidence")
    evidence.add_argument("--project-root", type=Path, required=True)
    evidence.add_argument("--mimic-root", type=Path, required=True)
    evidence.add_argument("--private-output", type=Path, required=True)
    evidence.add_argument("--public-output", type=Path, required=True)
    evidence.add_argument("--dictionary-only", action="store_true")
    handoff = subparsers.add_parser("export-analysis-handoff")
    handoff.add_argument("--project-root", type=Path, required=True)
    handoff.add_argument("--output-directory", type=Path, required=True)
    handoff.add_argument("--asic-release-manifest", type=Path)
    handoff.add_argument("--mimic-input-manifest", type=Path)
    handoff.add_argument("--mimic-d-items", type=Path)
    handoff.add_argument("--mimic-d-labitems", type=Path)
    handoff.add_argument("--stage-a-evidence", type=Path)
    handoff.add_argument("--stage-a-run-id")
    args = parser.parse_args()
    project_root = args.project_root.resolve()
    if args.command == "validate":
        print(validate_catalog(CatalogPaths(project_root)))
    elif args.command == "render":
        print(write_alignment_table(project_root))
    elif args.command == "audit-mimic-evidence":
        write_mapping_evidence(
            mimic_root=args.mimic_root.resolve(),
            mapping_csv=project_root / "interoperability/config/mimic_source_mappings_0_1.csv",
            private_output=args.private_output.resolve(),
            public_output=args.public_output.resolve(),
            scan_event_units=not args.dictionary_only,
        )
        print("alignment_evidence_completed=true")
    else:
        result = export_analysis_alignment_handoff(
            project_root=project_root,
            output_directory=args.output_directory.resolve(),
            asic_release_manifest=args.asic_release_manifest.resolve() if args.asic_release_manifest else None,
            mimic_input_manifest=args.mimic_input_manifest.resolve() if args.mimic_input_manifest else None,
            mimic_d_items=args.mimic_d_items.resolve() if args.mimic_d_items else None,
            mimic_d_labitems=args.mimic_d_labitems.resolve() if args.mimic_d_labitems else None,
            stage_a_evidence=args.stage_a_evidence.resolve() if args.stage_a_evidence else None,
            stage_a_run_id=args.stage_a_run_id,
        )
        print(f"handoff_review_status={result.review_status}")
        print(f"handoff_sha256={result.handoff_sha256}")
        print(f"unresolved_analysis_concept_count={len(result.unresolved_analysis_concepts)}")


if __name__ == "__main__":
    main()
