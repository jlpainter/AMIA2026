# Methodology

## Overview

This pipeline generates SIDER-compatible replacement files using PVLens content for drugs matched by ATC code.

## Steps

1. Load `drug_atc.tsv` from SIDER 4.1.
2. Match SIDER drugs to PVLens products by ATC code overlap.
3. Retrieve PVLens adverse events and indications linked to FDA SPL source files.
4. Normalize to MedDRA LLT/PT structure.
5. Preserve original SIDER flat CID values and associated stereo CID values.
6. Replace rows for matched drugs while leaving unmatched SIDER rows unchanged.

## MedDRA expansion

When a PVLens concept maps to an LLT, the output includes:

- the LLT row
- the PT parent row, when available

## Output strategy

Two types of files are produced:

- label-level files with source provenance
- pooled files deduplicated across label sources

