#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4."""

from __future__ import annotations

import pathlib
import sys
import shutil

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


def main() -> int:
    con = duckdb.connect()

    n_src_files = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src_files:,} file)")

    if DST.exists():
        shutil.rmtree(DST)

    src_rows = con.execute(f"select count(*) from read_parquet('{SRC}/*.parquet')").fetchone()[0]

    # Compact dataset:
    # 1. Partition by (event_date) -> 14 thư mục ngày (tránh small-file problem do partition quá mịn theo 650 customer).
    # 2. Order by customer_name, event_time -> gom các hàng cùng customer liền kề để tận dụng Parquet min/max statistics.
    # 3. Row group size 1000 -> cho phép row group pruning bỏ qua các block của customer khác trong cùng ngày.
    con.execute(f"""
        copy (
            select * from read_parquet('{SRC}/*.parquet')
            order by customer_name, event_time
        ) to '{DST}' (
            format parquet,
            partition_by (event_date),
            overwrite_or_ignore,
            row_group_size 1000
        )
    """)

    n_dst_files = len(list(DST.rglob("*.parquet")))
    dst_rows = con.execute(f"select count(*) from read_parquet('{DST}/*/*.parquet')").fetchone()[0]

    print(f"  đích  : {DST}  ({n_dst_files:,} file)")
    print(f"  số hàng: nguồn {src_rows:,} == đích {dst_rows:,}")
    assert src_rows == dst_rows, f"Mất hàng: {src_rows} != {dst_rows}"
    print("  compact hoàn tất thành công.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
