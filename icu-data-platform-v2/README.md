# ICU data platform v2

This repository currently contains the ASIC ICU-data pipeline. All ASIC-owned
configuration, documentation, notebooks, data, and generated reports live
together under [`asic/`](asic/).

Start with the [ASIC pipeline guide](asic/README.md).

Python packaging and tests remain at repository root by convention:

```text
icu-data-platform-v2/
├── asic/                 # all ASIC-owned material
├── src/asic_pipeline/    # installable implementation
├── tests/                # synthetic tests; no protected data
├── pyproject.toml
└── README.md
```

Install and test from this repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python -m pytest -q
```
