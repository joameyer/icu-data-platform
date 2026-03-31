# HPC ASIC Bundle

This folder is a self-contained upload bundle for running the ASIC harmonization
pipeline and the generic 8-hour blocking pipeline on the HPC cluster.

Important: full-ASIC runs are now a two-phase workflow on the cluster.

1. Run harmonization first.
2. Run blocking afterwards from the written harmonized artifacts.

This split is intentional and avoids the OOM issue that happened when both
steps were executed together in one job.

It includes:
- the ASIC extraction, harmonization, QC, stay-level, and 8-hour blocking code
- the bundled ASIC raw-to-canonical translation JSON
- a Python launcher that works without installing this repo as a package
- a shell wrapper and a Slurm submission template

It does not include raw ASIC data. Point the run command at the raw ASIC root
already available on the cluster.

## Expected Raw Data Layout

The `--raw-dir` path should contain hospital folders like:

```text
/path/to/raw_asic/
  UK_01/
    static/andere_variablen_kds_patienten.csv
    dynamic_filtered/*.csv
```

The loader also supports `dynamic/*.csv` if a hospital does not have
`dynamic_filtered/`.

## Quick Start

For full ASIC on the cluster, use the two Slurm jobs in this order.

### Step 1: Harmonization

```bash
cd hpc-icu-data-platform
sbatch run.sh
```

This writes:
- `static/`
- `dynamic/`
- `qc/`

and intentionally skips:
- `cohort/`
- `blocked/`

because `run.sh` defaults to harmonization-only.

### Step 2: Blocking

After the harmonization job finishes successfully, run:

```bash
cd hpc-icu-data-platform
sbatch run_blocking.sh
```

This reads the existing harmonized artifacts and then writes:
- `cohort/`
- `blocked/`

The blocking job uses a per-hospital pipeline so it is much lighter on memory
than the old all-in-one run.

## Local CLI Example

```bash
cd hpc-icu-data-platform
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run_asic_harmonization.py \
  --raw-dir /path/to/raw_asic \
  --output-dir /path/to/asic_harmonized_full \
  --format csv \
  --skip-standardized
python run_asic_standardized_from_harmonized.py \
  --input-dir /path/to/asic_harmonized_full \
  --output-dir /path/to/asic_harmonized_full \
  --input-format csv \
  --output-format csv
```

If you want parquet output for the harmonization-only step, install `pyarrow`
first and replace `--format csv` with `--format parquet`.

At the moment, the separate phase-2 blocking command
`run_asic_standardized_from_harmonized.py` supports CSV input and CSV output
only, so the recommended full-ASIC cluster workflow is still CSV for both
steps.

## RWTH Run Scripts

### Harmonization job: `run.sh`

The main place to save the cluster path to the full ASIC data is:

```bash
RAW_DIR="/path/to/full/asic/raw"
```

inside [run.sh](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/run.sh).

You will usually also want to set:

```bash
OUTPUT_DIR="${project_root}/artifacts/asic_harmonized_full"
VENV_PATH="/home/am861154/mypyenv"
```

Then submit from inside `hpc-icu-data-platform/`:

```bash
sbatch run.sh
```

Notes:
- `run.sh` currently defaults to `BUILD_STANDARDIZED=0`.
- That means it runs harmonization only and passes `--skip-standardized`.
- For full ASIC, this is the recommended default.
- Do not set `BUILD_STANDARDIZED=1` unless you explicitly want to try the old
  monolithic behavior again.

### Blocking job: `run_blocking.sh`

After `run.sh` has finished and written harmonized artifacts, submit:

```bash
sbatch run_blocking.sh
```

The main variables in [run_blocking.sh](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/run_blocking.sh) are:

```bash
INPUT_DIR="${project_root}/artifacts/asic_harmonized_full"
OUTPUT_DIR="${INPUT_DIR}"
INPUT_FORMAT="csv"
OUTPUT_FORMAT="csv"
DYNAMIC_CHUNKSIZE="250000"
VENV_PATH="/home/am861154/mypyenv"
```

Use the same `INPUT_DIR` as the `OUTPUT_DIR` from harmonization unless you want
the blocked outputs written somewhere else.

## Slurm Templates

### Harmonization template

```bash
cd hpc-icu-data-platform
sbatch \
  --export=ALL,PROJECT_DIR=$PWD,RAW_DIR=/path/to/raw_asic,OUTPUT_DIR=/path/to/asic_harmonized_full,FORMAT=csv,BUILD_STANDARDIZED=0 \
  slurm/submit_asic_harmonization.slurm
```

### Blocking template

```bash
cd hpc-icu-data-platform
sbatch \
  --export=ALL,PROJECT_DIR=$PWD,INPUT_DIR=/path/to/asic_harmonized_full,OUTPUT_DIR=/path/to/asic_harmonized_full,INPUT_FORMAT=csv,OUTPUT_FORMAT=csv,DYNAMIC_CHUNKSIZE=250000 \
  slurm/submit_asic_standardized_from_harmonized.slurm
```

## Outputs

### After harmonization (`sbatch run.sh`)

- `static/`
- `dynamic/`
- `qc/`

### After blocking (`sbatch run_blocking.sh`)

- `static/`
- `dynamic/`
- `qc/`
- `cohort/`
- `blocked/`

The blocking job does not rebuild `static/`, `dynamic/`, or `qc/`; it reads the
existing harmonized artifacts and adds `cohort/` and `blocked/`.

## Main Commands

`run_asic_harmonization.py` forwards directly to the ASIC build CLI, so the
full argument surface is available:

```bash
python run_asic_harmonization.py --help
```

Important options:
- `--raw-dir`: cluster path containing the full ASIC `UK_*` folders
- `--output-dir`: directory where harmonized artifacts will be written
- `--format`: `csv` or `parquet`
- `--translation-path`: optional override; defaults to the bundled JSON
- `--min-non-null`: default `20`
- `--min-hospitals`: default `4`
- `--fence-factor`: default `1.5`
- `--skip-standardized`: skip stay-level and 8-hour blocking outputs

The phase-2 blocking command is:

```bash
python run_asic_standardized_from_harmonized.py --help
```

Important options:
- `--input-dir`: directory containing existing harmonized ASIC outputs
- `--output-dir`: directory where `cohort/` and `blocked/` will be written
- `--input-format`: currently `csv`
- `--output-format`: currently `csv`
- `--dynamic-chunksize`: chunk size used while streaming the harmonized dynamic CSV

## Notes

- The default output format is CSV, so `pandas` is the only required Python dependency.
- If your cluster uses environment modules, load Python before activating or creating the virtual environment.
- The launcher inserts `src/` into `sys.path`, so you do not need to set `PYTHONPATH`.
- Recommended full-ASIC order on the cluster:
  1. `sbatch run.sh`
  2. wait for success
  3. `sbatch run_blocking.sh`
