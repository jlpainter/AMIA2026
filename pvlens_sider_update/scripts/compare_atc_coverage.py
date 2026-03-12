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
# Copyright (c) 2026 Anthony McDonald and Jeffery L. Painter
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
#!/usr/bin/env python3
"""
compare_atc_coverage.py

Compare PVLens ATC coverage against a local copy of SIDER 4.1.

This script:
- loads drug_atc.tsv from a local SIDER directory
- connects to the local PVLens MySQL database
- compares ATC coverage
- writes summary outputs if requested

Example:
    python compare_atc_coverage.py \
        --sider-dir ../sider_4.1 \
        --credentials credentials.txt \
        --output-dir output_compare
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare PVLens ATC coverage against SIDER 4.1."
    )
    parser.add_argument(
        "--sider-dir",
        type=Path,
        default=Path("../sider_4.1"),
        help="Path to local SIDER 4.1 directory (default: ../sider_4.1)",
    )
    parser.add_argument(
        "--credentials",
        type=Path,
        default=Path("credentials.txt"),
        help="Path to database credentials file (default: credentials.txt)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional output directory for CSV results",
    )
    return parser.parse_args()


def load_credentials(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(
            f"Credentials file not found: {path}\n"
            "Create a credentials.txt file with lines like:\n"
            '  db_user="your_username"\n'
            '  db_pass="your_password"\n'
        )

    creds: dict[str, str] = {}
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
            "Expected keys: db_user, db_pass"
        )

    return creds


def get_engine(credentials_file: Path):
    creds = load_credentials(credentials_file)
    connection_url = (
        f"mysql+mysqlconnector://{creds['db_user']}:{creds['db_pass']}@localhost/pvlens"
    )
    return create_engine(connection_url)


def require_file(path: Path, description: str) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"{description} not found: {path}\n"
            "Please download/extract SIDER 4.1 locally and point --sider-dir to it."
        )


def main() -> int:
    args = parse_args()

    sider_dir = args.sider_dir
    cred_file = args.credentials
    output_dir = args.output_dir

    sider_atc_file = sider_dir / "drug_atc.tsv"

    print("Checking inputs...")
    try:
        require_file(sider_atc_file, "SIDER drug_atc.tsv")
    except FileNotFoundError as e:
        print(f"\nERROR:\n{e}")
        return 1

    print(f"  SIDER directory: {sider_dir.resolve()}")
    print(f"  Using SIDER file: {sider_atc_file.resolve()}")

    print("\nConnecting to PVLens DB...")
    try:
        engine = get_engine(cred_file)
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        print("  Database connection OK.")
    except FileNotFoundError as e:
        print(f"\nERROR:\n{e}")
        return 1
    except ValueError as e:
        print(f"\nERROR:\n{e}")
        return 1
    except SQLAlchemyError as e:
        print(
            "\nERROR:\n"
            "Could not connect to the local PVLens MySQL database 'pvlens'.\n"
            "Please verify:\n"
            "  - MySQL is running\n"
            "  - the database name is 'pvlens'\n"
            "  - credentials.txt contains valid db_user/db_pass values\n"
            f"\nUnderlying error: {e}"
        )
        return 1

    print("\nLoading SIDER ATC data...")
    df_sider = pd.read_csv(
        sider_atc_file,
        sep="\t",
        header=None,
        names=["stitch_id", "atc_code"],
        dtype=str,
    )
    print(f"  SIDER records loaded: {len(df_sider):,}")

    print("\nLoading PVLens ATC table...")
    try:
        df_pvlens = pd.read_sql("SELECT * FROM ATC", engine)
    except SQLAlchemyError as e:
        print(
            "\nERROR:\n"
            "Failed to query the ATC table from PVLens.\n"
            "Please verify that the PVLens schema is loaded and includes an ATC table.\n"
            f"\nUnderlying error: {e}"
        )
        return 1

    print(f"  PVLens ATC records loaded: {len(df_pvlens):,}")

    if "CODE" not in df_pvlens.columns:
        print(
            "\nERROR:\n"
            "The PVLens ATC table does not contain a 'CODE' column.\n"
            f"Columns found: {list(df_pvlens.columns)}"
        )
        return 1

    print("\nCleaning ATC codes...")
    df_sider["atc_code"] = df_sider["atc_code"].astype(str).str.strip().str.upper()
    df_pvlens["CODE"] = df_pvlens["CODE"].astype(str).str.strip().str.upper()

    sider_atc_set = set(df_sider["atc_code"].dropna().unique())
    pvlens_atc_set = set(df_pvlens["CODE"].dropna().unique())

    missing_in_pvlens = sider_atc_set - pvlens_atc_set
    new_in_pvlens = pvlens_atc_set - sider_atc_set

    print("\n===== ATC COVERAGE SUMMARY =====")
    print(f"Unique ATC codes in SIDER : {len(sider_atc_set):,}")
    print(f"Unique ATC codes in PVLens: {len(pvlens_atc_set):,}")

    print("\n===== BUSINESS ANSWERS =====")
    if not missing_in_pvlens:
        print("✔ PVLens covers all ATC codes found in SIDER.")
    else:
        print(f"✖ PVLens is missing {len(missing_in_pvlens):,} ATC codes found in SIDER.")

    print(f"✔ PVLens has {len(new_in_pvlens):,} ATC codes not present in SIDER.")

    common_drugs = pd.merge(
        df_pvlens,
        df_sider,
        left_on="CODE",
        right_on="atc_code",
        how="inner",
    )

    print(f"\nMatching ATC records between PVLens and SIDER: {len(common_drugs):,}")
    print("\nPreview of matched rows:")
    print(common_drugs.head().to_string(index=False))

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)

        pd.DataFrame(sorted(missing_in_pvlens), columns=["missing_atc"]).to_csv(
            output_dir / "missing_atcs.csv",
            index=False,
        )

        pd.DataFrame(sorted(new_in_pvlens), columns=["new_atc"]).to_csv(
            output_dir / "new_atcs.csv",
            index=False,
        )

        common_drugs.to_csv(output_dir / "common_drugs.csv", index=False)

        print(f"\nResults written to: {output_dir.resolve()}")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
