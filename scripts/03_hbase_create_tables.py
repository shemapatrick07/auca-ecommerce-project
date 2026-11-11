"""
PART 1 - HBase schema (via the Thrift server / happybase). Same schema as hbase/hbase_shell_commands.txt.

TABLE user_sessions   one row per session
  row key  : <user_id>#<reverse_timestamp>#<session_id>
             - prefix scan on "<user_id>#" = all sessions of a user, NEWEST FIRST (no sort needed)
             - reverse ts = Long.MAX - epoch_ms, zero padded to 19 digits
  families : info   (session_id, start/end time, duration, conversion_status, referrer)
             device (type, os, browser)           geo (city, state, country)
             pv     (one column per page view: pv:000, pv:001 ... value = ts|page_type|product|category|seconds)
             cart   (one column per cart product: cart:<product_id> = qty|price)  -> sparse by nature

TABLE product_metrics one row per product per day
  row key  : <product_id>#<yyyymmdd>
  family   : m  (views, cart_adds, purchases, units, revenue)
"""
import sys

from hbase_common import connect

TABLES = {
    "user_sessions": {
        "info": dict(max_versions=1, bloom_filter_type="ROW"),
        "device": dict(max_versions=1),
        "geo": dict(max_versions=1),
        "pv": dict(max_versions=1),
        "cart": dict(max_versions=1),
    },
    "product_metrics": {
        "m": dict(max_versions=1),
    },
}

if __name__ == "__main__":
    conn = connect()
    existing = {t.decode() for t in conn.tables()}
    recreate = "--recreate" in sys.argv
    for name, families in TABLES.items():
        if name in existing:
            if not recreate:
                print(f"{name}: already exists (use --recreate to drop and rebuild)")
                continue
            conn.delete_table(name, disable=True)
            print(f"{name}: dropped")
        conn.create_table(name, families)
        print(f"{name}: created with families {list(families)}")
    print("tables now:", [t.decode() for t in conn.tables()])
