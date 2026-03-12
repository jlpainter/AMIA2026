#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of the PVLens-to-SIDER pipeline
# accompanying the AMIA 2026 poster:
# "Enabling Interoperability Between PVLens and SIDER:
#  A Pipeline for Generating SIDER-Compatible Drug–Adverse Event Data"
#
# Repository:
# https://github.com/jlpainter/AMIA2026/
#
# Copyright (c) 2026 Jeffery L. Painter
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#

"""
build_pvlens_sider_replacements.py

Build updated SIDER-compatible replacement files using PVLens content matched to
SIDER drugs via ATC code overlap.

Example:
    python build_pvlens_sider_replacements.py \
        --sider-dir ../sider_4.1 \
        --credentials credentials.txt \
        --output-dir output_sider_replacements

Outputs:
    meddra_all_label_indications.tsv
    meddra_all_indications.tsv
    meddra_all_label_se.tsv
    meddra_all_se.tsv
    matched_flat_cids.csv
    matched_atcs.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


DEFAULT_LABEL_SOURCE = "FDA_label"
TMP_SIDER_ATC_TABLE = "tmp_sider_atc"

SPL_SRCFILE_DATE_CANDIDATES = [
    "LABEL_DATE",
    "UPDATED",
    "CREATED_AT",
    "MODIFIED_AT",
    "EFFECTIVE_DATE",
    "SRC_DATE",
]

LABEL_INDICATIONS_COLS = [
    "label_source",
    "stitch_id_flat",
    "stitch_id_stereo",
    "mention_cui",
    "method",
    "mention_term",
    "meddra_level",
    "mapped_cui",
    "mapped_term",
]

ALL_INDICATIONS_COLS = [
    "stitch_id_flat",
    "mention_cui",
    "method",
    "mention_term",
    "meddra_level",
    "mapped_cui",
    "mapped_term",
]

LABEL_SE_COLS = [
    "label_source",
    "stitch_id_flat",
    "stitch_id_stereo",
    "mention_cui",
    "meddra_level",
    "mapped_cui",
    "mapped_term",
]

ALL_SE_COLS = [
    "stitch_id_flat",
    "stitch_id_stereo",
    "mention_cui",
    "meddra_level",
    "mapped_cui",
    "mapped_term",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build PVLens-derived SIDER-compatible replacement files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=(
            "Example:\n"
            "  python build_pvlens_sider_replacements.py "
            "--sider-dir ../sider_4.1 "
            "--credentials credentials.txt "
            "--output-dir output_sider_replacements\n\n"
            "If you run with no arguments, the script will look for:\n"
            "  ../sider_4.1\n"
            "  credentials.txt\n"
            "  output_sider_replacements/"
        ),
    )
    parser.add_argument(
        "--sider-dir",
        type=Path,
        default=Path("../sider_4.1"),
        help="Path to local SIDER 4.1 directory",
    )
    parser.add_argument(
        "--credentials",
        type=Path,
        default=Path("credentials.txt"),
        help="Path to credentials.txt",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output_sider_replacements"),
        help="Directory for generated replacement files",
    )
    parser.add_argument(
        "--subset-only",
        action="store_true",
        help="Write only the PVLens-derived matched subset instead of full drop-in replacement files",
    )
    parser.add_argument(
        "--db-host",
        default="localhost",
        help="MySQL host for the PVLens database",
    )
    parser.add_argument(
        "--db-name",
        default="pvlens",
        help="MySQL database name for PVLens",
    )
    return parser.parse_args()


def die_with_hint(message: str, hints: List[str] | None = None, exit_code: int = 2) -> None:
    print(f"\n[ERROR] {message}", file=sys.stderr)
    if hints:
        print("\nHints:", file=sys.stderr)
        for hint in hints:
            print(f"  - {hint}", file=sys.stderr)
    sys.exit(exit_code)


def require_file(path: Path, description: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{description} not found: {path}")


def require_sider_inputs(sider_dir: Path) -> None:
    required = [
        "drug_atc.tsv",
        "meddra_all_label_indications.tsv",
        "meddra_all_indications.tsv",
        "meddra_all_label_se.tsv",
        "meddra_all_se.tsv",
    ]
    if not sider_dir.exists():
        raise FileNotFoundError(
            f"SIDER directory not found: {sider_dir}\n"
            "Please point --sider-dir to a local copy of SIDER 4.1."
        )
    for fn in required:
        require_file(sider_dir / fn, f"Required SIDER file '{fn}'")


def validate_inputs(args: argparse.Namespace) -> None:
    missing = []

    if not args.credentials.exists():
        missing.append(f"credentials file: {args.credentials}")

    if not args.sider_dir.exists():
        missing.append(f"SIDER directory: {args.sider_dir}")

    if missing:
        die_with_hint(
            "Required inputs are missing.",
            [
                *missing,
                "Run with --help to see all options.",
                "Example: python build_pvlens_sider_replacements.py --sider-dir ../sider_4.1 --credentials credentials.txt",
                "If you intended to use the defaults, run the script from the repo directory that contains credentials.txt.",
            ],
        )

    try:
        require_sider_inputs(args.sider_dir)
    except FileNotFoundError as e:
        die_with_hint(
            str(e),
            [
                "Make sure your SIDER directory contains the five expected TSV files.",
                "Expected files include drug_atc.tsv and the four meddra_all_*.tsv files.",
            ],
        )


def load_credentials(path: Path) -> Dict[str, str]:
    require_file(path, "Credentials file")

    creds: Dict[str, str] = {}
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            creds[key.strip()] = value.strip().strip('"')

    missing = [k for k in ("db_user", "db_pass") if k not in creds]
    if missing:
        raise ValueError(
            f"Credentials file is missing required keys: {missing}\n"
            "Expected keys:\n"
            "  db_user=your_mysql_username\n"
            "  db_pass=your_mysql_password"
        )

    return creds


def get_engine(credentials_file: Path, db_host: str, db_name: str):
    creds = load_credentials(credentials_file)
    url = f"mysql+mysqlconnector://{creds['db_user']}:{creds['db_pass']}@{db_host}/{db_name}"
    return create_engine(url)


def read_tsv(path: Path, names: List[str]) -> pd.DataFrame:
    return pd.read_csv(
        path,
        sep="\t",
        header=None,
        names=names,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        quoting=csv.QUOTE_NONE,
    )


def load_sider_drug_atc(sider_dir: Path) -> pd.DataFrame:
    df = read_tsv(sider_dir / "drug_atc.tsv", ["stitch_id_flat", "atc_code"])
    df["stitch_id_flat"] = df["stitch_id_flat"].str.strip()
    df["atc_code"] = df["atc_code"].str.strip().str.upper()
    return df.drop_duplicates()


def load_original_sider_files(sider_dir: Path) -> Dict[str, pd.DataFrame]:
    return {
        "meddra_all_label_indications.tsv": read_tsv(
            sider_dir / "meddra_all_label_indications.tsv",
            LABEL_INDICATIONS_COLS,
        ),
        "meddra_all_indications.tsv": read_tsv(
            sider_dir / "meddra_all_indications.tsv",
            ALL_INDICATIONS_COLS,
        ),
        "meddra_all_label_se.tsv": read_tsv(
            sider_dir / "meddra_all_label_se.tsv",
            LABEL_SE_COLS,
        ),
        "meddra_all_se.tsv": read_tsv(
            sider_dir / "meddra_all_se.tsv",
            ALL_SE_COLS,
        ),
    }


def build_flat_to_stereo_map(original_files: Dict[str, pd.DataFrame]) -> Dict[str, List[str]]:
    stereo_sources = [
        original_files["meddra_all_label_se.tsv"][["stitch_id_flat", "stitch_id_stereo"]],
        original_files["meddra_all_se.tsv"][["stitch_id_flat", "stitch_id_stereo"]],
    ]
    df = pd.concat(stereo_sources, ignore_index=True).drop_duplicates()

    mapping: Dict[str, List[str]] = {}
    multi = []

    for flat_id, group in df.groupby("stitch_id_flat"):
        stereo_values = sorted(set(v for v in group["stitch_id_stereo"] if v))
        if len(stereo_values) > 1:
            multi.append((flat_id, stereo_values))
        mapping[flat_id] = stereo_values

    if multi:
        print("\n[INFO] Multiple stereo IDs found for some flat IDs; preserving all of them.")
        for flat_id, stereo_values in multi[:10]:
            print(f"  {flat_id}: {stereo_values}")

    return mapping


def load_tmp_sider_atc_table(engine, df_sider_atc: pd.DataFrame) -> None:
    df_codes = df_sider_atc[["atc_code"]].drop_duplicates().copy()
    df_codes["atc_code"] = df_codes["atc_code"].astype(str).str.strip().str.upper()
    df_codes = df_codes[df_codes["atc_code"] != ""]

    df_codes.to_sql(
        TMP_SIDER_ATC_TABLE,
        engine,
        if_exists="replace",
        index=False,
    )

    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"ALTER TABLE {TMP_SIDER_ATC_TABLE} MODIFY atc_code VARCHAR(32) NOT NULL"
            )
            conn.exec_driver_sql(
                f"CREATE INDEX idx_{TMP_SIDER_ATC_TABLE}_code ON {TMP_SIDER_ATC_TABLE}(atc_code)"
            )
    except Exception:
        pass


def get_spl_srcfile_columns(engine) -> Set[str]:
    df = pd.read_sql("DESCRIBE SPL_SRCFILE", engine)
    return set(df["Field"].astype(str).tolist())


def choose_spl_srcfile_date_column(engine) -> str | None:
    cols = get_spl_srcfile_columns(engine)
    for candidate in SPL_SRCFILE_DATE_CANDIDATES:
        if candidate in cols:
            return candidate
    return None


def build_sql_pvlens_ae(date_col: str | None) -> str:
    label_date_expr = f"sf.{date_col} AS label_date," if date_col else "NULL AS label_date,"

    return f"""
    SELECT DISTINCT
        pae.PRODUCT_ID,
        pae.MEDDRA_ID,
        UPPER(TRIM(a.CODE)) AS atc_code,
        sf.ID AS src_id,
        {label_date_expr}
        COALESCE(sf.XMLFILE_NAME, '{DEFAULT_LABEL_SOURCE}') AS label_source,
        m.MEDDRA_CUI AS mention_cui,
        m.MEDDRA_TERM AS mention_term,
        m.MEDDRA_TTY AS mention_tty,
        m.MEDDRA_CODE AS mention_code,
        m.MEDDRA_PTCODE AS pt_code
    FROM PRODUCT_AE pae
    JOIN PRODUCT_AE_SRC pas
        ON pas.AE_ID = pae.ID
    JOIN SPL_SRCFILE sf
        ON sf.ID = pas.SRC_ID
    JOIN MEDDRA m
        ON m.ID = pae.MEDDRA_ID
    JOIN SUBSTANCE_ATC sa
        ON sa.PRODUCT_ID = pae.PRODUCT_ID
    JOIN ATC a
        ON a.ID = sa.ATC_ID
    JOIN {TMP_SIDER_ATC_TABLE} t
        ON UPPER(TRIM(a.CODE)) = t.atc_code
    WHERE a.CODE IS NOT NULL
      AND TRIM(a.CODE) <> ''
    """


def build_sql_pvlens_ind(date_col: str | None) -> str:
    label_date_expr = f"sf.{date_col} AS label_date," if date_col else "NULL AS label_date,"

    return f"""
    SELECT DISTINCT
        pind.PRODUCT_ID,
        pind.MEDDRA_ID,
        UPPER(TRIM(a.CODE)) AS atc_code,
        sf.ID AS src_id,
        {label_date_expr}
        COALESCE(sf.XMLFILE_NAME, '{DEFAULT_LABEL_SOURCE}') AS label_source,
        m.MEDDRA_CUI AS mention_cui,
        m.MEDDRA_TERM AS mention_term,
        m.MEDDRA_TTY AS mention_tty,
        m.MEDDRA_CODE AS mention_code,
        m.MEDDRA_PTCODE AS pt_code
    FROM PRODUCT_IND pind
    JOIN PRODUCT_IND_SRC pis
        ON pis.IND_ID = pind.ID
    JOIN SPL_SRCFILE sf
        ON sf.ID = pis.SRC_ID
    JOIN MEDDRA m
        ON m.ID = pind.MEDDRA_ID
    JOIN SUBSTANCE_ATC sa
        ON sa.PRODUCT_ID = pind.PRODUCT_ID
    JOIN ATC a
        ON a.ID = sa.ATC_ID
    JOIN {TMP_SIDER_ATC_TABLE} t
        ON UPPER(TRIM(a.CODE)) = t.atc_code
    WHERE a.CODE IS NOT NULL
      AND TRIM(a.CODE) <> ''
    """


SQL_MEDDRA_LOOKUP = """
SELECT
    MEDDRA_CODE,
    MEDDRA_PTCODE,
    MEDDRA_TERM,
    MEDDRA_TTY,
    MEDDRA_CUI
FROM MEDDRA
"""


def pick_earliest_sources(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["label_date"] = pd.to_datetime(out["label_date"], errors="coerce")
    out["src_id_num"] = pd.to_numeric(out["src_id"], errors="coerce")

    out = out.sort_values(
        by=["PRODUCT_ID", "MEDDRA_ID", "atc_code", "label_date", "src_id_num"],
        ascending=[True, True, True, True, True],
        na_position="last",
    )

    out = out.drop_duplicates(subset=["PRODUCT_ID", "MEDDRA_ID", "atc_code"], keep="first")
    out = out.drop(columns=["src_id_num"])
    return out


def fetch_pvlens_ae(engine, date_col: str | None) -> pd.DataFrame:
    sql = build_sql_pvlens_ae(date_col)
    df = pd.read_sql(sql, engine)
    for col in df.columns:
        df[col] = df[col].fillna("").astype(str)
    df["atc_code"] = df["atc_code"].str.strip().str.upper()
    df["label_source"] = df["label_source"].replace({"": DEFAULT_LABEL_SOURCE})
    df["mention_tty"] = df["mention_tty"].str.strip().str.upper()
    df = pick_earliest_sources(df)
    return df


def fetch_pvlens_ind(engine, date_col: str | None) -> pd.DataFrame:
    sql = build_sql_pvlens_ind(date_col)
    df = pd.read_sql(sql, engine)
    for col in df.columns:
        df[col] = df[col].fillna("").astype(str)
    df["atc_code"] = df["atc_code"].str.strip().str.upper()
    df["label_source"] = df["label_source"].replace({"": DEFAULT_LABEL_SOURCE})
    df["mention_tty"] = df["mention_tty"].str.strip().str.upper()
    df = pick_earliest_sources(df)
    return df


def fetch_meddra_lookup(engine) -> pd.DataFrame:
    df = pd.read_sql(SQL_MEDDRA_LOOKUP, engine)
    for col in df.columns:
        df[col] = df[col].fillna("").astype(str)
    df["MEDDRA_TTY"] = df["MEDDRA_TTY"].str.strip().str.upper()
    return df


def attach_sider_flat_ids(
    df_pvlens: pd.DataFrame,
    df_sider_atc: pd.DataFrame,
) -> pd.DataFrame:
    return df_pvlens.merge(df_sider_atc, on="atc_code", how="inner").drop_duplicates()


def build_pt_lookup(df_meddra: pd.DataFrame) -> Dict[str, Dict[str, str]]:
    pt_df = df_meddra[df_meddra["MEDDRA_TTY"] == "PT"].copy()
    lookup: Dict[str, Dict[str, str]] = {}
    for _, r in pt_df.iterrows():
        lookup[r["MEDDRA_CODE"]] = {
            "pt_cui": r["MEDDRA_CUI"],
            "pt_term": r["MEDDRA_TERM"],
        }
    return lookup


def expand_to_meddra_rows(df: pd.DataFrame, pt_lookup: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    rows = []

    for _, r in df.iterrows():
        mention_cui = r["mention_cui"]
        mention_term = r["mention_term"]
        mention_tty = r["mention_tty"]
        pt_code = r["pt_code"]

        if mention_tty in {"LLT", "PT"} and mention_cui and mention_term:
            rows.append({
                **r.to_dict(),
                "meddra_level": mention_tty,
                "mapped_cui": mention_cui,
                "mapped_term": mention_term,
            })

        if mention_tty == "LLT" and pt_code and pt_code in pt_lookup:
            rows.append({
                **r.to_dict(),
                "meddra_level": "PT",
                "mapped_cui": pt_lookup[pt_code]["pt_cui"],
                "mapped_term": pt_lookup[pt_code]["pt_term"],
            })

    out = pd.DataFrame(rows).drop_duplicates()
    return out


def expand_stereo_rows(df: pd.DataFrame, flat_to_stereo: Dict[str, List[str]]) -> pd.DataFrame:
    rows = []

    for _, r in df.iterrows():
        flat_id = r["stitch_id_flat"]
        stereo_ids = flat_to_stereo.get(flat_id, [])

        if not stereo_ids:
            rr = r.to_dict()
            rr["stitch_id_stereo"] = ""
            rows.append(rr)
            continue

        for stereo_id in stereo_ids:
            rr = r.to_dict()
            rr["stitch_id_stereo"] = stereo_id
            rows.append(rr)

    return pd.DataFrame(rows).drop_duplicates()


def build_label_se(df_expanded: pd.DataFrame, flat_to_stereo: Dict[str, List[str]]) -> pd.DataFrame:
    base = df_expanded[
        ["label_source", "stitch_id_flat", "mention_cui", "meddra_level", "mapped_cui", "mapped_term"]
    ].drop_duplicates().copy()

    base = expand_stereo_rows(base, flat_to_stereo)
    base = base[
        ["label_source", "stitch_id_flat", "stitch_id_stereo", "mention_cui", "meddra_level", "mapped_cui", "mapped_term"]
    ].drop_duplicates()

    base.columns = LABEL_SE_COLS
    return base


def build_all_se(df_label_se: pd.DataFrame) -> pd.DataFrame:
    out = df_label_se[
        ["stitch_id_flat", "stitch_id_stereo", "mention_cui", "meddra_level", "mapped_cui", "mapped_term"]
    ].drop_duplicates().copy()
    out.columns = ALL_SE_COLS
    return out


def build_label_indications(df_expanded: pd.DataFrame, flat_to_stereo: Dict[str, List[str]]) -> pd.DataFrame:
    base = df_expanded[
        ["label_source", "stitch_id_flat", "mention_cui", "mention_term", "meddra_level", "mapped_cui", "mapped_term"]
    ].drop_duplicates().copy()

    base["method"] = "NLP_indication"
    base = expand_stereo_rows(base, flat_to_stereo)
    base = base[
        [
            "label_source",
            "stitch_id_flat",
            "stitch_id_stereo",
            "mention_cui",
            "method",
            "mention_term",
            "meddra_level",
            "mapped_cui",
            "mapped_term",
        ]
    ].drop_duplicates()

    base.columns = LABEL_INDICATIONS_COLS
    return base


def build_all_indications(df_label_ind: pd.DataFrame) -> pd.DataFrame:
    out = df_label_ind[
        ["stitch_id_flat", "mention_cui", "mention_term", "meddra_level", "mapped_cui", "mapped_term"]
    ].drop_duplicates().copy()
    out.insert(2, "method", "text_mention")
    out.columns = ALL_INDICATIONS_COLS
    return out


def replace_rows_by_flat_id(
    original_df: pd.DataFrame,
    replacement_df: pd.DataFrame,
    write_full_replacements: bool,
    flat_col: str = "stitch_id_flat",
) -> pd.DataFrame:
    matched_flat_ids = set(replacement_df[flat_col].dropna().unique())

    if write_full_replacements:
        kept = original_df[~original_df[flat_col].isin(matched_flat_ids)].copy()
        out = pd.concat([kept, replacement_df], ignore_index=True)
    else:
        out = replacement_df.copy()

    return out.drop_duplicates()


def write_tsv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep="\t", header=False, index=False, encoding="utf-8")


def print_summary(name: str, original_df: pd.DataFrame, replacement_df: pd.DataFrame, final_df: pd.DataFrame) -> None:
    print(f"\n{name}")
    print(f"  original rows   : {len(original_df):,}")
    print(f"  replacement rows: {len(replacement_df):,}")
    print(f"  final rows      : {len(final_df):,}")


def main() -> None:
    args = parse_args()
    validate_inputs(args)

    write_full_replacements = not args.subset_only

    print("Configuration")
    print(f"  SIDER dir       : {args.sider_dir.resolve()}")
    print(f"  credentials     : {args.credentials.resolve()}")
    print(f"  output dir      : {args.output_dir.resolve()}")
    print(f"  db host         : {args.db_host}")
    print(f"  db name         : {args.db_name}")
    print(f"  mode            : {'full replacements' if write_full_replacements else 'subset only'}")

    print("\nConnecting to PVLens...")
    engine = get_engine(args.credentials, args.db_host, args.db_name)

    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
    except SQLAlchemyError as e:
        die_with_hint(
            f"Unable to connect to MySQL database '{args.db_name}' on host '{args.db_host}'.\n{e}",
            [
                "Confirm MySQL is running.",
                "Check db_user and db_pass in your credentials file.",
                f"Confirm the database exists: {args.db_name}",
                "If needed, try --db-host and --db-name explicitly.",
            ],
        )

    print("  OK")

    print("\nLoading SIDER inputs...")
    df_sider_atc = load_sider_drug_atc(args.sider_dir)
    original_files = load_original_sider_files(args.sider_dir)
    flat_to_stereo = build_flat_to_stereo_map(original_files)

    print(f"  drug_atc.tsv rows: {len(df_sider_atc):,}")
    print(f"  flat CID count   : {len(set(df_sider_atc['stitch_id_flat'])):,}")

    print("\nLoading SIDER ATC codes into MySQL helper table...")
    load_tmp_sider_atc_table(engine, df_sider_atc)
    print("  helper table loaded")

    print("\nInspecting SPL_SRCFILE columns...")
    date_col = choose_spl_srcfile_date_column(engine)
    if date_col:
        print(f"  using SPL_SRCFILE.{date_col} to determine earliest source")
    else:
        print("  no date column found in SPL_SRCFILE; using smallest SPL_SRCFILE.ID as proxy for earliest source")

    print("\nFetching PVLens MedDRA lookup...")
    df_meddra = fetch_meddra_lookup(engine)
    pt_lookup = build_pt_lookup(df_meddra)

    print("\nFetching PVLens adverse events (restricted to SIDER ATCs)...")
    df_ae = fetch_pvlens_ae(engine, date_col)
    print(f"  raw AE rows after restriction and earliest-source selection: {len(df_ae):,}")

    print("\nFetching PVLens indications (restricted to SIDER ATCs)...")
    df_ind = fetch_pvlens_ind(engine, date_col)
    print(f"  raw indication rows after restriction and earliest-source selection: {len(df_ind):,}")

    print("\nMatching PVLens rows to SIDER flat CIDs via ATC...")
    df_ae = attach_sider_flat_ids(df_ae, df_sider_atc)
    df_ind = attach_sider_flat_ids(df_ind, df_sider_atc)
    print(f"  matched AE rows        : {len(df_ae):,}")
    print(f"  matched indication rows: {len(df_ind):,}")

    matched_flat_ids: Set[str] = set(df_ae["stitch_id_flat"]).union(set(df_ind["stitch_id_flat"]))
    matched_atcs: Set[str] = set(df_ae["atc_code"]).union(set(df_ind["atc_code"]))

    print(f"  matched flat CID count : {len(matched_flat_ids):,}")
    print(f"  matched ATC count      : {len(matched_atcs):,}")

    print("\nExpanding LLT/PT rows...")
    df_ae_expanded = expand_to_meddra_rows(df_ae, pt_lookup)
    df_ind_expanded = expand_to_meddra_rows(df_ind, pt_lookup)

    print(f"  expanded AE rows        : {len(df_ae_expanded):,}")
    print(f"  expanded indication rows: {len(df_ind_expanded):,}")

    print("\nBuilding SIDER-compatible replacement files...")
    new_label_se = build_label_se(df_ae_expanded, flat_to_stereo)
    new_all_se = build_all_se(new_label_se)

    new_label_ind = build_label_indications(df_ind_expanded, flat_to_stereo)
    new_all_ind = build_all_indications(new_label_ind)

    print(f"  new meddra_all_label_se.tsv rows          : {len(new_label_se):,}")
    print(f"  new meddra_all_se.tsv rows                : {len(new_all_se):,}")
    print(f"  new meddra_all_label_indications.tsv rows : {len(new_label_ind):,}")
    print(f"  new meddra_all_indications.tsv rows       : {len(new_all_ind):,}")

    print("\nReplacing matched drugs in original SIDER files...")
    final_label_se = replace_rows_by_flat_id(
        original_files["meddra_all_label_se.tsv"], new_label_se, write_full_replacements
    )
    final_all_se = replace_rows_by_flat_id(
        original_files["meddra_all_se.tsv"], new_all_se, write_full_replacements
    )
    final_label_ind = replace_rows_by_flat_id(
        original_files["meddra_all_label_indications.tsv"], new_label_ind, write_full_replacements
    )
    final_all_ind = replace_rows_by_flat_id(
        original_files["meddra_all_indications.tsv"], new_all_ind, write_full_replacements
    )

    print_summary("meddra_all_label_se.tsv", original_files["meddra_all_label_se.tsv"], new_label_se, final_label_se)
    print_summary("meddra_all_se.tsv", original_files["meddra_all_se.tsv"], new_all_se, final_all_se)
    print_summary("meddra_all_label_indications.tsv", original_files["meddra_all_label_indications.tsv"], new_label_ind, final_label_ind)
    print_summary("meddra_all_indications.tsv", original_files["meddra_all_indications.tsv"], new_all_ind, final_all_ind)

    print("\nWriting output files...")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    write_tsv(final_label_se, args.output_dir / "meddra_all_label_se.tsv")
    write_tsv(final_all_se, args.output_dir / "meddra_all_se.tsv")
    write_tsv(final_label_ind, args.output_dir / "meddra_all_label_indications.tsv")
    write_tsv(final_all_ind, args.output_dir / "meddra_all_indications.tsv")

    pd.DataFrame(sorted(matched_flat_ids), columns=["stitch_id_flat"]).to_csv(
        args.output_dir / "matched_flat_cids.csv", index=False
    )
    pd.DataFrame(sorted(matched_atcs), columns=["atc_code"]).to_csv(
        args.output_dir / "matched_atcs.csv", index=False
    )

    print("\nDone.")
    print(f"Output directory: {args.output_dir.resolve()}")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        die_with_hint(
            str(e),
            [
                "Check your paths for --sider-dir and --credentials.",
                "Run with --help to see available options.",
            ],
        )
    except ValueError as e:
        die_with_hint(
            str(e),
            [
                "Check the contents of your credentials file.",
                "Expected format includes lines like db_user=... and db_pass=...",
            ],
        )
    except SQLAlchemyError as e:
        die_with_hint(
            f"Database error:\n{e}",
            [
                "Confirm the PVLens MySQL schema is loaded and reachable.",
                "Verify your mysqlconnector driver is installed.",
            ],
        )
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        die_with_hint(
            f"Unexpected error:\n{type(e).__name__}: {e}",
            [
                "Re-run with the same arguments and inspect the traceback if needed.",
                "If this is reproducible, include the command and error in the GitHub issue or repo notes.",
            ],
            exit_code=1,
        )
