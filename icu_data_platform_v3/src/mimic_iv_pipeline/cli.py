from __future__ import annotations

import argparse
from pathlib import Path

from mimic_iv_pipeline.contracts import load_profile
from mimic_iv_pipeline.errors import MIMICPipelineError
from mimic_iv_pipeline.production import (
    audit_production_candidate,
    build_production_candidate,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mimic-iv-pipeline",
        description="MIMIC-IV selected-variable contract utilities",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser(
        "validate-contracts",
        help="Validate the local profile without reading clinical data",
    )
    validate.add_argument("--profile", type=Path, required=True)
    build = commands.add_parser(
        "build-production-candidate",
        help="Build a private, run-scoped MIMIC-IV candidate on the authorized cluster",
    )
    build.add_argument("--config", type=Path, required=True)
    build.add_argument("--run-id", required=True)
    audit = commands.add_parser(
        "audit-production-candidate",
        help="Independently audit a private run-scoped candidate",
    )
    audit.add_argument("--config", type=Path, required=True)
    audit.add_argument("--run-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "validate-contracts":
            loaded = load_profile(args.profile)
            print(
                f"PASS {loaded.profile['profile_id']} "
                f"variables={len(loaded.variables)} ranges={len(loaded.ranges)}"
            )
            return 0
        if args.command == "build-production-candidate":
            path = build_production_candidate(args.config, run_id=args.run_id)
            print(f"PASS candidate={path}")
            return 0
        if args.command == "audit-production-candidate":
            path = audit_production_candidate(args.config, run_id=args.run_id)
            print(f"PASS audit={path}")
            return 0
    except MIMICPipelineError as exc:
        print(f"ERROR: {exc}")
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
