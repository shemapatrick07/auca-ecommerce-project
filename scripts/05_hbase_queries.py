"""
PART 1 - HBase queries (happybase). Demonstrates the access patterns the row keys were designed for:

  Q1  latest N sessions of a user            -> prefix scan on "<user_id>#"        (newest first)
  Q2  a user's sessions in a time window     -> range scan between two reverse timestamps
  Q3  a product's daily metrics + conversion -> range scan on "<product_id>#<yyyymmdd>"

Usage: python scripts/05_hbase_queries.py [--user user_000042] [--product prod_00123]
                                          [--days-back 14]  [--from 20260901 --to 20261001]
"""
import argparse
from datetime import datetime, timedelta

from hbase_common import connect, rev_ts


def dec(d):
    return {k.decode(): v.decode() for k, v in d.items()}


def show_session(key, data):
    d = dec(data)
    pv = [v for k, v in d.items() if k.startswith("pv:")]
    cart = {k[5:]: v for k, v in d.items() if k.startswith("cart:")}
    print(f"  {d.get('info:start_time')}  {d.get('info:session_id')}  {d.get('device:type'):8}"
          f"{d.get('info:duration_seconds'):>5}s  {d.get('info:conversion_status'):10} "
          f"ref={d.get('info:referrer'):14} page_views={len(pv)} cart={cart or '-'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user")
    ap.add_argument("--product")
    ap.add_argument("--limit", type=int, default=5)
    ap.add_argument("--days-back", type=int, default=14)
    ap.add_argument("--from", dest="d_from")
    ap.add_argument("--to", dest="d_to")
    args = ap.parse_args()

    conn = connect()
    sess, met = conn.table("user_sessions"), conn.table("product_metrics")

    # pick a sample user / product if none given
    if not args.user:
        first = next(sess.scan(limit=1, columns=[b"info:session_id"]), None)
        if not first:
            raise SystemExit("user_sessions is empty - run 04_hbase_load.py first")
        args.user = first[0].decode().split("#")[0]
    if not args.product:
        first = next(met.scan(limit=1, columns=[b"m:views"]), None)
        args.product = first[0].decode().split("#")[0] if first else None

    # Q1 ---------------------------------------------------------------------
    print(f"\nQ1. Latest {args.limit} sessions of {args.user} (prefix scan, newest first)")
    latest = list(sess.scan(row_prefix=f"{args.user}#".encode(), limit=args.limit))
    for k, v in latest:
        show_session(k, v)

    # Q2 ---------------------------------------------------------------------
    newest = datetime.fromisoformat(dec(latest[0][1])["info:start_time"])
    t_from, t_to = newest - timedelta(days=args.days_back), newest
    # reverse timestamps: LATER time -> SMALLER key, so start=rev(t_to), stop=rev(t_from)
    start = f"{args.user}#{rev_ts(t_to)}".encode()
    stop = f"{args.user}#{rev_ts(t_from)}~".encode()
    print(f"\nQ2. Sessions of {args.user} between {t_from:%Y-%m-%d} and {t_to:%Y-%m-%d} (range scan)")
    n = 0
    for k, v in sess.scan(row_start=start, row_stop=stop):
        show_session(k, v)
        n += 1
    print(f"  -> {n} sessions in window")

    # Q3 ---------------------------------------------------------------------
    if args.product:
        prefix = f"{args.product}#"
        row_start = f"{prefix}{args.d_from}".encode() if args.d_from else prefix.encode()
        row_stop = f"{prefix}{args.d_to}".encode() if args.d_to else f"{args.product}$".encode()
        print(f"\nQ3. Daily metrics of {args.product}")
        tot = dict(views=0, cart_adds=0, purchases=0)
        for k, v in met.scan(row_start=row_start, row_stop=row_stop):
            d = dec(v)
            day = k.decode().split("#")[1]
            print(f"  {day}  views={d.get('m:views')}  cart_adds={d.get('m:cart_adds')}  "
                  f"purchases={d.get('m:purchases')}  revenue={d.get('m:revenue')}")
            for key in tot:
                tot[key] += int(d.get(f"m:{key}", 0))
        views = tot["views"] or 1
        print(f"  TOTAL {tot} | view->cart {tot['cart_adds'] / views:.1%} | view->purchase {tot['purchases'] / views:.1%}")


if __name__ == "__main__":
    main()
