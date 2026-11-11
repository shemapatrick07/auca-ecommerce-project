"""
PART 1 - MongoDB aggregation pipelines (4 non-trivial analytics). Results are printed and
saved as CSV in output/ (used by the visualisation step).

  1. Top-selling products  ($unwind -> $group -> $sort -> $lookup)
  2. Revenue by category and month  ($unwind -> $group on a composite key -> $lookup)
  3. User segmentation by spend quartile  (two-stage $group + $bucketAuto)
  4. Most-viewed products from sessions  ($unwind viewed_products -> $group -> $lookup)

NOTE: the dataset has no age field, so "segmentation" uses spend/purchasing behaviour instead of age groups.
Revenue below = sum of line-item subtotals (before order-level discounts).
"""
import os
import time

import pandas as pd
from pymongo import MongoClient

import config

db = MongoClient(config.MONGO_URI)[config.MONGO_DB]

TOP_PRODUCTS = [
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id",
                "units_sold": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"},
                "orders": {"$sum": 1}}},
    {"$sort": {"revenue": -1}},
    {"$limit": 200},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "product_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$project": {"_id": 0, "product_id": "$_id", "name": "$p.name", "category": "$p.category.name",
                  "units_sold": 1, "orders": 1, "revenue": {"$round": ["$revenue", 2]}}},
]

REVENUE_BY_CATEGORY_MONTH = [
    {"$unwind": "$items"},
    {"$group": {"_id": {"category_id": "$items.category_id",
                        "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}}},
                "revenue": {"$sum": "$items.subtotal"},
                "units": {"$sum": "$items.quantity"}}},
    {"$lookup": {"from": "categories", "localField": "_id.category_id",
                 "foreignField": "category_id", "as": "c"}},
    {"$unwind": "$c"},
    {"$project": {"_id": 0, "category_id": "$_id.category_id", "category": "$c.name",
                  "month": "$_id.month", "units": 1, "revenue": {"$round": ["$revenue", 2]}}},
    {"$sort": {"month": 1, "revenue": -1}},
]

USER_SEGMENTS = [
    {"$group": {"_id": "$user_id", "orders": {"$sum": 1}, "spent": {"$sum": "$total"}}},
    {"$bucketAuto": {"groupBy": "$spent", "buckets": 4,
                     "output": {"users": {"$sum": 1}, "avg_orders": {"$avg": "$orders"},
                                "avg_spent": {"$avg": "$spent"}}}},
    {"$project": {"_id": 0, "spend_min": {"$round": ["$_id.min", 2]}, "spend_max": {"$round": ["$_id.max", 2]},
                  "users": 1, "avg_orders": {"$round": ["$avg_orders", 2]},
                  "avg_spent": {"$round": ["$avg_spent", 2]}}},
]

MOST_VIEWED = [
    {"$unwind": "$viewed_products"},
    {"$group": {"_id": "$viewed_products", "views": {"$sum": 1},
                "converted_sessions": {"$sum": {"$cond": [{"$eq": ["$conversion_status", "converted"]}, 1, 0]}}}},
    {"$sort": {"views": -1}},
    {"$limit": 200},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "product_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$project": {"_id": 0, "product_id": "$_id", "name": "$p.name", "category": "$p.category.name",
                  "views": 1, "converted_sessions": 1}},
]


def run(title, collection, pipeline, csv_name, label_segments=False):
    t0 = time.perf_counter()
    rows = list(db[collection].aggregate(pipeline, allowDiskUse=True))
    secs = time.perf_counter() - t0
    df = pd.DataFrame(rows)
    if label_segments:
        df.insert(0, "segment", [f"Q{i + 1}" for i in range(len(df))])
    df.to_csv(os.path.join(config.OUT_DIR, csv_name), index=False)
    print(f"\n=== {title}  ({len(df)} rows, {secs:.2f}s) -> output/{csv_name}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    run("1. Top-selling products", "transactions", TOP_PRODUCTS, "mongo_top_products.csv")
    run("2. Revenue by category and month", "transactions", REVENUE_BY_CATEGORY_MONTH,
        "mongo_revenue_by_category_month.csv")
    run("3. User segments by spend quartile", "transactions", USER_SEGMENTS,
        "mongo_user_segments.csv", label_segments=True)
    run("4. Most-viewed products", "sessions", MOST_VIEWED, "mongo_most_viewed.csv")
