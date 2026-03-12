# Scripts

This directory contains scripts used to generate and validate the PVLens-derived
SIDER replacement files described in the AMIA 2026 poster.

## Setup

Create a Python environment and install dependencies:

```bash
pip install -r requirements.txt
```


## Scripts

### build_pvlens_sider_replacements.py
Main production script for generating SIDER-compatible replacement files from
PVLens data.

This script:

- loads ATC mappings from SIDER
- extracts adverse events and indications from the PVLens database
- maps them to MedDRA terms
- matches drugs using ATC overlap
- produces SIDER-compatible TSV files

### compare_atc_coverage.py
Utility script used during development to compare ATC coverage between PVLens
and SIDER.

## Expected Inputs

The scripts assume the following inputs:

- **Local SIDER 4.1 dataset**
- **PVLens MySQL database**
- **credentials.txt** containing database credentials

Example credentials file:

```
db_user=myuser
db_pass=mypassword
```

## Typical Usage

From the repository root:

```bash
python scripts/build_pvlens_sider_replacements.py \
  --sider-dir ../sider_4.1 \
  --credentials credentials.txt \
  --output-dir output_sider_replacements
```

## Output

Generated files include SIDER-compatible tables:

- `meddra_all_label_indications.tsv`
- `meddra_all_indications.tsv`
- `meddra_all_label_se.tsv`
- `meddra_all_se.tsv`

Additional metadata files:

- `matched_flat_cids.csv`
- `matched_atcs.csv`
