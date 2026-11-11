"""
PART 1 - Load session data (and derived daily product metrics) into HBase.

  * every session  -> one row in user_sessions (see 03_hbase_create_tables.py for the design)
  * product_metrics -> aggregated in memory while streaming sessions, written at the end:
        views     = product_detail page views that day
        cart_adds = sessions that had the product in their cart (day of session start)
        purchases/units/revenue = from SESSION-LINKED transactions of the loaded sessions only,
                                  so that view -> purchase conversion is consistent with the subset

Usage: python scripts/04_hbase_load.py [--max-sessions 200000]
"""
import argparse
import glob
import json
import os
import re
from collections import defaultdict
from datetime import datetime

from hbase_common import connect, session_row_key, metric_row_key
import config


def b(x):
    return str(x).encode()


def day_key(iso_ts):
    return iso_ts[:10].replace("-", "")          # 2026-09-01T10:00:00 -> 20260901


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-sessions", type=int, default=200000)
    ap.add_argument("--batch-size", type=int, default=1000)
    args = ap.parse_args()

    conn = connect()
    sess_table, met_table = conn.table("user_sessions"), conn.table("product_metrics")

    files = sorted(glob.glob(os.path.join(config.DATA_DIR, "sessions_*.json")),
                   key=lambda p: int(re.search(r"sessions_(\d+)", p).group(1)))
    metrics = defaultdict(lambda: {"views": 0, "cart_adds": 0, "purchases": 0, "units": 0, "revenue": 0.0})
    loaded_ids, loaded = set(), 0

    for fpath in files:
        if loaded >= args.max_sessions:
            break
        with open(fpath) as f:
            sessions = json.load(f)[: args.max_sessions - loaded]
        with sess_table.batch(batch_size=args.batch_size) as bt:
            for s in sessions:
                start = datetime.fromisoformat(s["start_time"])
                row = {
                    b"info:session_id": b(s["session_id"]), b"info:start_time": b(s["start_time"]),
                    b"info:end_time": b(s["end_time"]), b"info:duration_seconds": b(s["duration_seconds"]),
                    b"info:conversion_status": b(s["conversion_status"]), b"info:referrer": b(s["referrer"]),
                    b"device:type": b(s["device_profile"]["type"]), b"device:os": b(s["device_profile"]["os"]),
                    b"device:browser": b(s["device_profile"]["browser"]),
                    b"geo:city": b(s["geo_data"]["city"]), b"geo:state": b(s["geo_data"]["state"]),
                    b"geo:country": b(s["geo_data"]["country"]),
                }
                for i, pv in enumerate(s["page_views"]):
                    row[f"pv:{i:03d}".encode()] = "|".join([
                        pv["timestamp"], pv["page_type"], pv["product_id"] or "",
                        pv["category_id"] or "", str(pv["view_duration"])]).encode()
                    if pv["page_type"] == "product_detail" and pv["product_id"]:
                        metrics[(pv["product_id"], day_key(pv["timestamp"]))]["views"] += 1
                for pid, d in s["cart_contents"].items():
                    row[f"cart:{pid}".encode()] = f"{d['quantity']}|{d['price']}".encode()
                    metrics[(pid, day_key(s["start_time"]))]["cart_adds"] += 1
                bt.put(session_row_key(s["user_id"], start, s["session_id"]), row)
                loaded_ids.add(s["session_id"])
        loaded += len(sessions)
        print(f"sessions loaded: {loaded:,} ({os.path.basename(fpath)})")

    # purchases from session-linked transactions
    tx_path = os.path.join(config.DATA_DIR, "transactions.json")
    if os.path.exists(tx_path):
        with open(tx_path) as f:
            for t in json.load(f):
                if t["session_id"] in loaded_ids:
                    for it in t["items"]:
                        m = metrics[(it["product_id"], day_key(t["timestamp"]))]
                        m["purchases"] += 1
                        m["units"] += it["quantity"]
                        m["revenue"] += it["subtotal"]

    with met_table.batch(batch_size=5000) as bt:
        for (pid, day), m in metrics.items():
            bt.put(metric_row_key(pid, day), {
                b"m:views": b(m["views"]), b"m:cart_adds": b(m["cart_adds"]),
                b"m:purchases": b(m["purchases"]), b"m:units": b(m["units"]),
                b"m:revenue": b(round(m["revenue"], 2))})
    print(f"product_metrics rows written: {len(metrics):,}. Done.")


if __name__ == "__main__":
    main()
