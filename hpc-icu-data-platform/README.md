# HPC ASIC Bundle

This folder is a self-contained upload bundle for running the ASIC harmonization
pipeline and the generic 8-hour blocking pipeline on the HPC cluster.

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

```bash
cd hpc-icu-data-platform
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run_asic_harmonization.py \
  --raw-dir /path/to/raw_asic \
  --output-dir /path/to/asic_harmonized_full \
  --format csv
```

If you want parquet output instead of CSV, install `pyarrow` first and replace
`--format csv` with `--format parquet`.

## RWTH Run Script

If you want a single job script like your previous setup, use
`hpc-icu-data-platform/run.sh`.

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

## Slurm Example

```bash
cd hpc-icu-data-platform
sbatch \
  --export=ALL,PROJECT_DIR=$PWD,RAW_DIR=/path/to/raw_asic,OUTPUT_DIR=/path/to/asic_harmonized_full,FORMAT=csv \
  slurm/submit_asic_harmonization.slurm
```

## Outputs

The run writes the same artifact groups as the local build:

- `static/`
- `dynamic/`
- `qc/`
- `cohort/`
- `blocked/`

## Main Run Command

`run_asic_harmonization.py` forwards directly to the ASIC build CLI, so the
full argument surface is available:

```bash
python run_asic_harmonization.py --help
```

Important options:
- `--raw-dir`: cluster path containing the full ASIC `UK_*` folders
- `--output-dir`: directory where harmonized and blocked artifacts will be written
- `--format`: `csv` or `parquet`
- `--translation-path`: optional override; defaults to the bundled JSON
- `--min-non-null`: default `20`
- `--min-hospitals`: default `4`
- `--fence-factor`: default `1.5`

## Notes

- The default output format is CSV, so `pandas` is the only required Python dependency.
- If your cluster uses environment modules, load Python before activating or creating the virtual environment.
- The launcher inserts `src/` into `sys.path`, so you do not need to set `PYTHONPATH`.
