# Release Files

This directory documents the release contents for the **PVLens-derived SIDER-compatible replacement dataset** used in the AMIA 2026 poster.

## Replacement files

The following SIDER-compatible tables are provided as **gzip-compressed TSV files**:

- `meddra_all_label_indications.tsv.gz`
- `meddra_all_indications.tsv.gz`
- `meddra_all_label_se.tsv.gz`
- `meddra_all_se.tsv.gz`

These files are drop-in replacements for the corresponding tables in **SIDER 4.1** for drugs that can be matched to PVLens via ATC codes.

## Supporting files

Additional files included in this release:

- `matched_flat_cids.csv`  
  List of SIDER **STITCH flat compound identifiers** that were matched to PVLens drugs via ATC overlap.

- `matched_atcs.csv`  
  ATC codes used to match PVLens drug products to SIDER entries.

- `sider_atcs.csv`  
  Extracted ATC codes from the SIDER dataset used during matching.

## Notes

- SIDER **flat and stereo CID identifiers are preserved** for matched drugs.
- Only the four `meddra_all_*` tables are modified.
- Original SIDER files such as `drug_atc.tsv`, `drug_names.tsv`, `meddra.tsv`, and `meddra_freq.tsv` remain unchanged.
- Label-level records are derived from **FDA Structured Product Labeling (SPL)** data processed by PVLens.

## File format

All `.tsv.gz` files are standard **tab-separated value (TSV)** tables compressed with gzip.

To decompress:

```bash
gunzip *.tsv.gz
```

or view without extracting
```bash
zcat meddra_all_label_se.tsv.gz | head
```

## Notes

- SIDER flat and stereo CID identifiers are preserved for matched drugs.
- Original `drug_atc.tsv`, `drug_names.tsv`, `meddra.tsv`, and `meddra_freq.tsv` are not modified.
- Label-level files use FDA SPL provenance from PVLens.

## Integrity

Checksums for released files are listed in `checksums.txt`.
