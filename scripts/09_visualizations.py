"""
PART 4 - Static visualisations (matplotlib) from the CSVs produced by scripts 02, 06, 07, 08.
A chart is skipped (with a message) if its CSV does not exist yet. Output: output/figures/*.png
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

import config

FIG = os.path.join(config.OUT_DIR, "figures")
os.makedirs(FIG, exist_ok=True)
plt.rcParams.update({"figure.dpi": 120, "axes.spines.top": False, "axes.spines.right": False})


def read(name):
    p = os.path.join(config.OUT_DIR, name)
    if not os.path.exists(p):
        print(f"[skip] {name} not found")
        return None
    return pd.read_csv(p)


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, name), dpi=150)
    plt.close(fig)
    print(f"saved figures/{name}")


# 1 revenue by category ----------------------------------------------------------------------
df = read("mongo_revenue_by_category_month.csv")
if df is not None:
    top = df.groupby("category")["revenue"].sum().nlargest(10).sort_values()
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(top.index, top.values, color="#3b6ea5")
    ax.set_title("Top 10 categories by revenue (MongoDB aggregation)")
    ax.set_xlabel("Revenue (line-item subtotals)")
    save(fig, "01_revenue_by_category.png")

# 2 daily revenue + moving average ----------------------------------------------------------
df = read("spark_daily_revenue.csv")
if df is not None:
    df["day"] = pd.to_datetime(df["day"])
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(df["day"], df["revenue"], alpha=0.35, label="Daily revenue")
    ax.plot(df["day"], df["revenue_7d_avg"], linewidth=2.2, label="7-day moving average")
    ax.set_title("Sales performance over time (Spark SQL)")
    ax.set_ylabel("Revenue")
    ax.legend()
    fig.autofmt_xdate()
    save(fig, "02_daily_revenue.png")

# 3 customer segmentation (spend quartiles) --------------------------------------------------
df = read("mongo_user_segments.csv")
if df is not None:
    fig, ax1 = plt.subplots(figsize=(7, 4.5))
    ax1.bar(df["segment"], df["users"], color="#8fb5d9")
    ax1.set_ylabel("Users")
    ax2 = ax1.twinx()
    ax2.plot(df["segment"], df["avg_spent"], color="#c0392b", marker="o", linewidth=2)
    ax2.set_ylabel("Average spend per user", color="#c0392b")
    ax1.set_title("User segments by spend quartile (Q1 = lowest)")
    save(fig, "03_user_segments.png")

# 4 top-selling vs most-viewed ----------------------------------------------------------------
sold, viewed = read("mongo_top_products.csv"), read("mongo_most_viewed.csv")
if sold is not None and viewed is not None:
    m = viewed.merge(sold[["product_id", "units_sold"]], on="product_id", how="left").fillna({"units_sold": 0})
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(m["views"], m["units_sold"], alpha=0.6, color="#2c7a4b")
    ax.set_xlabel("Product views (sessions)")
    ax.set_ylabel("Units sold")
    ax.set_title(f"Most-viewed vs top-selling products (corr = {m['views'].corr(m['units_sold']):.2f})")
    save(fig, "04_views_vs_sales.png")

# 5 conversion funnel ---------------------------------------------------------------------------
df = read("integrated_funnel.csv")
if df is not None:
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.barh(df["stage"][::-1], df["sessions"][::-1], color="#5b4b9a")
    for i, (n, p) in enumerate(zip(df["sessions"][::-1], df["pct_of_all"][::-1])):
        ax.text(n, i, f" {n:,} ({p}%)", va="center")
    ax.set_title("Conversion funnel (Spark)")
    ax.set_xlim(0, df["sessions"].max() * 1.25)
    save(fig, "05_funnel.png")

# 6 cohort heatmap ----------------------------------------------------------------------------------
df = read("spark_cohorts.csv")
if df is not None:
    pv = df.pivot(index="cohort_month", columns="months_since_registration", values="avg_spend_per_user")
    fig, ax = plt.subplots(figsize=(8, 5))
    im = ax.imshow(pv.values, aspect="auto", cmap="YlGnBu")
    ax.set_xticks(range(len(pv.columns)), pv.columns)
    ax.set_yticks(range(len(pv.index)), pv.index)
    ax.set_xlabel("Months since registration")
    ax.set_ylabel("Registration cohort")
    ax.set_title("Cohort analysis: average spend per cohort user")
    fig.colorbar(im, label="Avg spend per user")
    save(fig, "06_cohorts.png")

# 7 CLV segments ---------------------------------------------------------------------------------------
df = read("integrated_clv_segments.csv")
if df is not None:
    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.bar(df["segment"], df["avg_clv"], color=["#b8860b", "#c0a000", "#999999", "#a0522d"][: len(df)])
    for i, (u, c) in enumerate(zip(df["users"], df["avg_clv"])):
        ax.text(i, c, f"{u:,} users", ha="center", va="bottom")
    ax.set_title("Estimated 12-month CLV by K-Means segment")
    ax.set_ylabel("Average CLV")
    save(fig, "07_clv_segments.png")
