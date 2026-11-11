"""
PART 1 - MongoDB: load the generated data using the document model from the design section.

Collections
  categories   : category with EMBEDDED subcategories (small, read together, rarely changes)
  products     : product + EMBEDDED category summary + EMBEDDED price_history (read together on a product page)
  transactions : order with EMBEDDED line items; each item is denormalised with category_id
                 so revenue-by-category needs no join
  sessions     : session with EMBEDDED page_views; the dynamic-key `cart_contents` dict is converted
                 to an array `cart_items` (arrays are indexable/aggregatable, dynamic keys are not)
  users        : profile + EMBEDDED purchase_summary and engagement summary (computed at load time)

Usage:  python scripts/01_mongo_load.py --drop [--max-sessions 200000]
"""
import argparse
import glob
import json
import os
import re
from collections import defaultdict
from datetime import datetime

from pymongo import MongoClient

import config


def dt(s):
    return datetime.fromisoformat(s) if s else None


def batches(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def load_json(name):
    with open(os.path.join(config.DATA_DIR, name)) as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-sessions", type=int, default=200000, help="sessions to load into MongoDB")
    ap.add_argument("--drop", action="store_true", help="drop existing collections first")
    args = ap.parse_args()

    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    names = ["categories", "products", "transactions", "sessions", "users"]
    if args.drop:
        for n in names:
            db[n].drop()
    elif any(db[n].estimated_document_count() for n in names):
        raise SystemExit("Collections already contain data. Re-run with --drop to reload.")

    # ---- categories -------------------------------------------------------------
    categories = load_json("categories.json")
    cat_name = {c["category_id"]: c["name"] for c in categories}
    db.categories.insert_many(categories)
    print(f"categories   : {len(categories):,}")

    # ---- products ---------------------------------------------------------------
    products = load_json("products.json")
    prod_cat = {}
    for p in products:
        prod_cat[p["product_id"]] = p["category_id"]
        p["category"] = {"category_id": p["category_id"], "name": cat_name.get(p["category_id"])}
        p["creation_date"] = dt(p["creation_date"])
        for ph in p["price_history"]:
            ph["date"] = dt(ph["date"])
        p["current_price"] = p["base_price"]
    for b in batches(products, 5000):
        db.products.insert_many(b)
    print(f"products     : {len(products):,}")

    # ---- transactions (+ per-user purchase statistics) --------------------------
    txns = load_json("transactions.json")
    stats = defaultdict(lambda: {"total_orders": 0, "total_spent": 0.0, "first_purchase": None,
                                 "last_purchase": None, "cat_spend": defaultdict(float)})
    for t in txns:
        t["timestamp"] = dt(t["timestamp"])
        for it in t["items"]:
            it["category_id"] = prod_cat.get(it["product_id"])
        t["item_count"] = sum(i["quantity"] for i in t["items"])
        s = stats[t["user_id"]]
        s["total_orders"] += 1
        s["total_spent"] += t["total"]
        s["first_purchase"] = min(filter(None, [s["first_purchase"], t["timestamp"]]))
        s["last_purchase"] = max(filter(None, [s["last_purchase"], t["timestamp"]]))
        for it in t["items"]:
            s["cat_spend"][it["category_id"]] += it["subtotal"]
    for b in batches(txns, 10000):
        db.transactions.insert_many(b)
    print(f"transactions : {len(txns):,}")

    # ---- sessions (subset) ------------------------------------------------------
    files = sorted(glob.glob(os.path.join(config.DATA_DIR, "sessions_*.json")),
                   key=lambda p: int(re.search(r"sessions_(\d+)", p).group(1)))
    eng = defaultdict(lambda: {"session_count": 0, "total_seconds": 0, "converted_sessions": 0})
    loaded = 0
    for fpath in files:
        if loaded >= args.max_sessions:
            break
        with open(fpath) as f:
            sessions = json.load(f)
        sessions = sessions[: args.max_sessions - loaded]
        for s in sessions:
            s["start_time"], s["end_time"] = dt(s["start_time"]), dt(s["end_time"])
            for pv in s["page_views"]:
                pv["timestamp"] = dt(pv["timestamp"])
            s["cart_items"] = [{"product_id": k, "quantity": v["quantity"], "price": v["price"]}
                               for k, v in s.pop("cart_contents").items()]
            e = eng[s["user_id"]]
            e["session_count"] += 1
            e["total_seconds"] += s["duration_seconds"]
            e["converted_sessions"] += s["conversion_status"] == "converted"
        for b in batches(sessions, 10000):
            db.sessions.insert_many(b)
        loaded += len(sessions)
        print(f"  sessions loaded: {loaded:,} ({os.path.basename(fpath)})")

    # ---- users (+ embedded summaries) -------------------------------------------
    users = load_json("users.json")
    for u in users:
        u["registration_date"], u["last_active"] = dt(u["registration_date"]), dt(u["last_active"])
        s = stats.get(u["user_id"])
        if s:
            fav = max(s["cat_spend"], key=s["cat_spend"].get)
            u["purchase_summary"] = {
                "total_orders": s["total_orders"], "total_spent": round(s["total_spent"], 2),
                "avg_order_value": round(s["total_spent"] / s["total_orders"], 2),
                "first_purchase": s["first_purchase"], "last_purchase": s["last_purchase"],
                "favorite_category_id": fav}
        else:
            u["purchase_summary"] = {"total_orders": 0, "total_spent": 0.0}
        e = eng.get(u["user_id"])
        u["engagement"] = ({"session_count": e["session_count"],
                            "avg_session_seconds": round(e["total_seconds"] / e["session_count"], 1),
                            "converted_sessions": e["converted_sessions"]}
                           if e else {"session_count": 0})
    db.users.insert_many(users)
    print(f"users        : {len(users):,}")

    # ---- indexes ----------------------------------------------------------------
    db.categories.create_index("category_id", unique=True)
    db.products.create_index("product_id", unique=True)
    db.products.create_index("category_id")
    db.users.create_index("user_id", unique=True)
    db.transactions.create_index("transaction_id", unique=True)
    db.transactions.create_index("user_id")
    db.transactions.create_index("timestamp")
    db.transactions.create_index("items.product_id")
    db.sessions.create_index("session_id", unique=True)
    db.sessions.create_index([("user_id", 1), ("start_time", -1)])
    db.sessions.create_index("viewed_products")
    print("indexes created. Done.")


if __name__ == "__main__":
    main()
