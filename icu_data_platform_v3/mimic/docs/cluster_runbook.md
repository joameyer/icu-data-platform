# MIMIC-IV production candidate runbook

This workflow reads `/hpcwork/jrc_combine/joana/mimic/data/{hosp,icu}` and
writes a new private candidate and independent audit below the deployed v3
repository. It creates no release or pointer.

From the login shell, after deploying this exact verified source snapshot:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
MIMIC_CANDIDATE_RUN_ID=${RUN_ID} \
  bash mimic/slurm/run_production_candidate_and_audit.sh
```

The wrapper validates all required MIMIC files and refuses an existing run ID
before submitting a `c23ms` Slurm job with two CPUs, 32 GiB, and a 24-hour
limit. The standard output/error files are private under
`mimic/runs/production/`.

Monitor using the job ID returned by `sbatch`:

```bash
squeue -j JOB_ID
tail -n 50 mimic/runs/production/mimic-layer-JOB_ID.out
tail -n 50 mimic/runs/production/mimic-layer-JOB_ID.err
```

After successful completion, inspect only the aggregate audit status:

```bash
RUN_ID=THE_EXACT_RUN_ID
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -c \
  'import json,sys; p="mimic/data/production_audits/"+sys.argv[1]+"/audit.json"; d=json.load(open(p)); print(json.dumps({k:d[k] for k in ("run_id","status","canonical_block_eligible_event_count","blocked_observation_counts","findings")},indent=2))' \
  "${RUN_ID}"
```

Expected status is `PASS`, both blocked observation counts equal the canonical
eligible-event count, and `findings` is empty. Do not copy Parquet files,
manifests, source paths, timestamps, identifiers, or row-level evidence off the
cluster. A passing audit remains a non-release candidate; promotion and pointer
operations are intentionally absent.
