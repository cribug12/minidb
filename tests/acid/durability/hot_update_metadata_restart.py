#!/usr/bin/env python3
"""Regression: a HOT UPDATE must not corrupt the heap meta page across restarts.

`HeapFile::InPageReservation::commit` increments `meta_.num_tuples` and marks the
meta page dirty. It used to do that without loading the meta page first, so on a
fresh process `meta_` was still the zeroed struct from the constructor and the
clean-shutdown flush persisted `first_data_page_id = 0`. A sequential scan then
found no data page at all: every row of the table disappeared, and the next
UPDATE failed with "failed to invalidate old tuple version".

Each statement therefore runs in its own OS process, so the restart boundary is
what actually gets exercised.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lib"))
from minidb_testlib import (  # noqa: E402
    add_seed_args,
    assert_no_error,
    cleanup,
    assert_rows_equal,
    minidb_query,
    run_minidb,
    temp_db,
)

SCHEMA = "CREATE TABLE t (id INT PRIMARY KEY, v INT);"
ROWS = [(1, 10), (99, 99)]
INSERT = "INSERT INTO t VALUES " + ", ".join("(%d, %d)" % r for r in ROWS) + ";"


def find_dump_tool(repo_root: str) -> str | None:
    path = os.path.join(repo_root, "tools", "minidb_dump.py")
    return path if os.path.exists(path) else None


def heap_meta(dump_output: str) -> dict | None:
    """Parse the HeapMeta page line pair emitted by tools/minidb_dump.py."""
    text = re.sub(r"\s+", " ", dump_output)
    first = re.search(r"first_data_page=0x([0-9a-fA-F]+)", text)
    pages = re.search(r"num_data_pages=(\d+)", text)
    tuples = re.search(r"num_tuples=(\d+)", text)
    if not (first and pages and tuples):
        return None
    return {
        "first_data_page": int(first.group(1), 16),
        "num_data_pages": int(pages.group(1)),
        "num_tuples": int(tuples.group(1)),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    add_seed_args(parser)
    args = parser.parse_args()
    seed = args.seed
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))))
    db_dir = temp_db("minidb-hot-update-meta.")
    try:
        # Process 1: create the table and load rows.
        out = run_minidb(args.bin, db_dir, [SCHEMA, INSERT], timeout=30)
        assert_no_error(out, seed, "setup")

        # Process 2: HOT-eligible UPDATE (no indexed column changes) then exit,
        # which is what used to persist the zeroed meta page.
        out = run_minidb(args.bin, db_dir, ["UPDATE t SET v = 99 WHERE id = 99;"], timeout=30)
        assert_no_error(out, seed, "hot update")
        if "affected_rows" not in out:
            raise AssertionError("UPDATE produced no affected_rows column seed=%d\n%s" % (seed, out))

        # Process 3: a clean restart. Both rows must still be visible.
        rows = minidb_query(args.bin, db_dir, "SELECT id, v FROM t ORDER BY id;", seed)
        assert_rows_equal(rows, [("1", "10"), ("99", "99")], seed,
                          "SELECT after HOT UPDATE + restart")

        # Process 4: a second UPDATE must not fail with a stale version pointer.
        out = run_minidb(args.bin, db_dir, ["UPDATE t SET v = v + 1 WHERE id = 1;"], timeout=30)
        assert_no_error(out, seed, "second hot update")
        rows = minidb_query(args.bin, db_dir, "SELECT id, v FROM t ORDER BY id;", seed)
        assert_rows_equal(rows, [("1", "11"), ("99", "99")], seed,
                          "SELECT after second HOT UPDATE + restart")

        # Process 5: a following INSERT must still see the existing chain.
        out = run_minidb(args.bin, db_dir, ["INSERT INTO t VALUES (5, 50);"], timeout=30)
        assert_no_error(out, seed, "insert after hot update")
        rows = minidb_query(args.bin, db_dir, "SELECT id, v FROM t ORDER BY id;", seed)
        assert_rows_equal(rows, [("1", "11"), ("5", "50"), ("99", "99")], seed,
                          "SELECT after INSERT following HOT UPDATE")

        # The on-disk meta page is the actual regression target: a zeroed
        # first_data_page_id is what made the table read as empty.
        dump_tool = find_dump_tool(repo_root)
        if dump_tool:
            import subprocess
            proc = subprocess.run([sys.executable, dump_tool, db_dir, "--no-index"],
                                  text=True, capture_output=True, timeout=60)
            meta = heap_meta(proc.stdout)
            if meta is None:
                raise AssertionError("could not parse heap meta page seed=%d\n%s"
                                     % (seed, proc.stdout[-2000:] + proc.stderr[-2000:]))
            if meta["first_data_page"] == 0:
                raise AssertionError("meta page lost first_data_page_id seed=%d: %r" % (seed, meta))
            if meta["num_data_pages"] < 1:
                raise AssertionError("meta page lost num_data_pages seed=%d: %r" % (seed, meta))
            if meta["num_tuples"] < 2:
                raise AssertionError("meta page lost num_tuples seed=%d: %r" % (seed, meta))

        print("hot_update_metadata_restart PASS seed=%d" % seed)
        return 0
    except Exception as exc:  # noqa: BLE001
        print("hot_update_metadata_restart FAIL seed=%d: %s" % (seed, exc), file=sys.stderr)
        return 1
    finally:
        cleanup(db_dir)


if __name__ == "__main__":
    raise SystemExit(main())
