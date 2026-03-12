# PVLens SIDER Update

This directory contains code and documentation for generating SIDER-compatible replacement files from PVLens.
The primary script used to generate the replacement files is:

`scripts/build_pvlens_sider_replacements.py`

## Purpose

SIDER 4.1 remains widely used in pharmacovigilance research, but its underlying data have not been updated since 2015. This project generates replacement files for a subset of SIDER drugs using current PVLens data while preserving SIDER flat and stereo STITCH/CID identifiers for ATC-matched drugs.

## What is updated

The pipeline generates replacements for:

- `meddra_all_label_indications.tsv`
- `meddra_all_indications.tsv`
- `meddra_all_label_se.tsv`
- `meddra_all_se.tsv`

## What is not updated

The following SIDER files are intentionally left unchanged:

- `drug_atc.tsv`
- `drug_names.tsv`
- `meddra.tsv`
- `meddra_freq.tsv`

## Matching strategy

SIDER drugs are matched to PVLens products using overlapping ATC codes. The original SIDER flat CID values are preserved, and associated stereo CID values are retained for compatibility with existing SIDER-based workflows.

## Provenance rule for label-level files

For label-level files, PVLens rows are assigned the earliest available FDA SPL source file associated with each `(product, MedDRA concept)` pair. If no explicit source date is available, the smallest source file identifier is used as a proxy for earliest source.

## Main components

- `scripts/` — production scripts and QA utilities
- `notebooks/` — exploratory and comparison notebooks
- `docs/` — methodology and file format documentation
- `sample_data/` — small sample outputs for inspection
- `release/` — release notes and checksum file

## Requirements

See `requirements.txt`.
