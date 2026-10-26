from __future__ import annotations

import math
import shutil
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import seaborn as sns
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

AMOUNT = T.DecimalType(18, 2)
RATE = T.DecimalType(18, 8)

DATA_DIR = Path("dados")
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
CHARTS_DIR = Path("graficos")

INSTRUCTION_SHEETS = {"Comece_Aqui", "Perguntas", "Regras", "Dicionario"}
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

FX_OUTLIER_RATIO = 3.0
UNMAPPED = "UNMAPPED"
UNATTRIBUTED = "UNATTRIBUTED"

PNL_DIMENSIONS = [
    "period",
    "country_code",
    "merchant_id",
    "product",
    "payment_method",
    "source_account",
    "pl_group",
    "pl_subgroup",
    "pl_line",
    "batch_id",
    "is_official",
    "is_late_posting",
    "is_mapped",
]
TPV_DIMENSIONS = ["period", "country_code", "merchant_id", "product", "payment_method", "batch_id"]


def get_spark(app_name: str = "orionpay") -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_excel_sheets(spark: SparkSession, path: Path) -> dict[str, DataFrame]:
    sheets = pd.read_excel(path, sheet_name=None, dtype=str, keep_default_na=False)
    tables = {}
    for name, pdf in sheets.items():
        if name in INSTRUCTION_SHEETS:
            continue
        schema = T.StructType([T.StructField(col, T.StringType(), True) for col in pdf.columns])
        rows = [[value if value != "" else None for value in row] for row in pdf.itertuples(index=False, name=None)]
        tables[name] = spark.createDataFrame(rows, schema)
    return tables


def write_csv(df: DataFrame, path: Path) -> Path:
    path = Path(path)
    tmp_dir = path.with_suffix(".tmp")
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("timestampFormat", TIMESTAMP_FORMAT)
        .csv(str(tmp_dir))
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(next(tmp_dir.glob("part-*.csv")), path)
    shutil.rmtree(tmp_dir)
    return path


def read_csv(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", False).csv(str(path))


def union_batches(*frames: DataFrame) -> DataFrame:
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def month_start(col: str) -> F.Column:
    return F.trunc(F.col(col), "month")


def parse_timestamp(col: str) -> F.Column:
    return F.to_timestamp(F.col(col), TIMESTAMP_FORMAT)


def cast_payment_events(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("event_at", parse_timestamp("event_at"))
        .withColumn("ingested_at", parse_timestamp("ingested_at"))
        .withColumn("amount_local", F.col("amount_local").cast(AMOUNT))
        .withColumn("period", month_start("event_at"))
    )


def cast_financial_entries(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("accounting_date", F.to_date("accounting_date"))
        .withColumn("posted_at", parse_timestamp("posted_at"))
        .withColumn("amount", F.col("amount").cast(AMOUNT))
        .withColumn("period", month_start("accounting_date"))
    )


def cast_fx_rates(df: DataFrame) -> DataFrame:
    return df.withColumn("period", F.to_date("period")).withColumn("usd_per_unit", F.col("usd_per_unit").cast(RATE))


def cast_account_mapping(df: DataFrame) -> DataFrame:
    return df.withColumn("valid_from", F.to_date("valid_from")).withColumn("valid_to", F.to_date("valid_to"))


def cast_close_calendar(df: DataFrame) -> DataFrame:
    return df.withColumn("period", F.to_date("period")).withColumn("official_cutoff_at", parse_timestamp("official_cutoff_at"))


def drop_exact_duplicates(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    counted = df.groupBy(df.columns).agg(F.count("*").alias("copies"))
    removed = counted.filter("copies > 1").withColumn("removed_rows", F.col("copies") - 1).drop("copies")
    return df.dropDuplicates(df.columns), removed


def correct_fx_outliers(fx: DataFrame) -> DataFrame:
    median_by_currency = fx.groupBy("currency_code").agg(F.percentile_approx("usd_per_unit", 0.5).alias("median_rate"))
    ratio = F.col("usd_per_unit") / F.col("median_rate")
    is_outlier = (ratio > FX_OUTLIER_RATIO) | (ratio < 1 / FX_OUTLIER_RATIO)
    power_of_ten = F.pow(F.lit(10.0), F.round(F.log10(ratio)))
    return (
        fx.join(median_by_currency, "currency_code")
        .withColumn("fx_is_corrected", is_outlier)
        .withColumn("usd_per_unit_original", F.col("usd_per_unit"))
        .withColumn(
            "usd_per_unit",
            F.when(is_outlier, (F.col("usd_per_unit") / power_of_ten).cast(RATE)).otherwise(F.col("usd_per_unit")),
        )
        .drop("median_rate")
    )


def impute_currency_from_country(entries: DataFrame, countries: DataFrame) -> DataFrame:
    country_currency = countries.select("country_code", F.col("currency_code").alias("country_currency"))
    return (
        entries.join(country_currency, "country_code", "left")
        .withColumn("currency_is_imputed", F.col("currency_code").isNull())
        .withColumn("currency_code", F.coalesce("currency_code", "country_currency"))
        .drop("country_currency")
    )


def convert_to_usd(df: DataFrame, fx: DataFrame, amount_col: str) -> DataFrame:
    rates = fx.select("period", "currency_code", "usd_per_unit", "usd_per_unit_original", "fx_is_corrected")
    usd = (F.col(amount_col) * F.col("usd_per_unit")).cast(T.DecimalType(30, 10))
    usd_original = (F.col(amount_col) * F.col("usd_per_unit_original")).cast(T.DecimalType(30, 10))
    return (
        df.join(rates, ["period", "currency_code"], "left")
        .withColumn("amount_usd", F.round(usd, 2).cast(AMOUNT))
        .withColumn("amount_usd_original_fx", F.round(usd_original, 2).cast(AMOUNT))
    )


def classify_entries(entries: DataFrame, mapping: DataFrame) -> DataFrame:
    rules = mapping.select(
        F.col("source_account").alias("map_account"), "pl_group", "pl_subgroup", "pl_line", "valid_from", "valid_to"
    )
    condition = (
        (entries.source_account == rules.map_account)
        & (entries.accounting_date >= rules.valid_from)
        & (entries.accounting_date < rules.valid_to)
    )
    return (
        entries.join(rules, condition, "left")
        .withColumn("is_mapped", F.col("map_account").isNotNull())
        .withColumn("pl_group", F.coalesce("pl_group", F.lit(UNMAPPED)))
        .withColumn("pl_subgroup", F.coalesce("pl_subgroup", F.lit(UNMAPPED)))
        .withColumn("pl_line", F.coalesce("pl_line", F.col("description")))
        .drop("map_account", "valid_from", "valid_to")
    )


def flag_official_close(entries: DataFrame, calendar: DataFrame) -> DataFrame:
    closed = F.col("period_status") == "CLOSED"
    within_cutoff = F.col("posted_at") <= F.col("official_cutoff_at")
    return (
        entries.join(calendar, "period", "left")
        .withColumn("is_official", closed & within_cutoff)
        .withColumn("is_late_posting", closed & ~within_cutoff)
    )


def flag_tpv(events: DataFrame) -> DataFrame:
    return events.withColumn("is_tpv", (F.col("event_type") == "CAPTURE") & (F.col("status") == "CONFIRMED"))


def build_fact_tpv(events: DataFrame) -> DataFrame:
    return (
        events.filter("is_tpv")
        .groupBy(TPV_DIMENSIONS)
        .agg(
            F.count("*").alias("payments"),
            F.sum("amount_usd").alias("tpv_usd"),
            F.sum("amount_usd_original_fx").alias("tpv_usd_original_fx"),
        )
    )


def build_fact_pnl(entries: DataFrame) -> DataFrame:
    return (
        entries.withColumn("merchant_id", F.coalesce("merchant_id", F.lit(UNATTRIBUTED)))
        .withColumn("product", F.coalesce("product", F.lit(UNATTRIBUTED)))
        .withColumn("payment_method", F.coalesce("payment_method", F.lit(UNATTRIBUTED)))
        .groupBy(PNL_DIMENSIONS)
        .agg(
            F.count("*").alias("entries"),
            F.sum("amount_usd").alias("amount_usd"),
            F.sum("amount_usd_original_fx").alias("amount_usd_original_fx"),
        )
    )


def monthly_pnl_spark(fact_pnl: DataFrame, fact_tpv: DataFrame, view: str) -> DataFrame:
    scope = fact_pnl.filter("is_official") if view == "official" else fact_pnl
    pnl = scope.groupBy("period").agg(
        F.sum(F.when(F.col("pl_group") == "NET_REVENUE", F.col("amount_usd"))).alias("net_revenue"),
        F.sum(F.when(F.col("pl_group") == "COST_OF_SALES", F.col("amount_usd"))).alias("cost_of_sales"),
        F.sum(F.when(F.col("pl_subgroup") == "DIRECT_COSTS", F.col("amount_usd"))).alias("direct_costs"),
        F.sum(F.when(F.col("pl_subgroup") == "INDIRECT_COSTS", F.col("amount_usd"))).alias("indirect_costs"),
        F.sum(F.when(F.col("pl_group") == UNMAPPED, F.col("amount_usd"))).alias("unmapped"),
    )
    tpv = fact_tpv.groupBy("period").agg(F.sum("tpv_usd").alias("tpv"))
    return (
        tpv.join(pnl, "period", "inner")
        .fillna(0, subset=["unmapped", "indirect_costs"])
        .withColumn("gross_profit", F.col("net_revenue") + F.col("cost_of_sales"))
        .withColumn("take_rate", F.col("net_revenue") / F.col("tpv"))
        .withColumn("gp_margin", F.col("gross_profit") / F.col("net_revenue"))
        .withColumn("gp_over_tpv", F.col("gross_profit") / F.col("tpv"))
        .withColumn("view", F.lit(view))
        .orderBy("period")
    )


def to_pandas(df: DataFrame) -> pd.DataFrame:
    columns = [
        F.col(field.name).cast("double") if isinstance(field.dataType, T.DecimalType) else F.col(field.name)
        for field in df.schema.fields
    ]
    return df.select(columns).toPandas()


def sum_column(df: DataFrame, col: str) -> float:
    value = df.agg(F.sum(col)).first()[0]
    return float(value) if value is not None else 0.0


def load_gold(name: str) -> pd.DataFrame:
    df = pd.read_csv(GOLD_DIR / f"{name}.csv", keep_default_na=False, na_values=[""])
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"])
    return df


def select_view(fact_pnl: pd.DataFrame, view: str, batches: tuple[str, ...] = ("B01", "B02")) -> pd.DataFrame:
    scoped = fact_pnl[fact_pnl["batch_id"].isin(batches)]
    if view == "official":
        return scoped[scoped["is_official"]]
    return scoped


def pnl_summary(
    fact_pnl: pd.DataFrame,
    fact_tpv: pd.DataFrame,
    by: list[str],
    view: str = "official",
    batches: tuple[str, ...] = ("B01", "B02"),
    amount_col: str = "amount_usd",
    tpv_col: str = "tpv_usd",
) -> pd.DataFrame:
    entries = select_view(fact_pnl, view, batches)
    pnl = (
        entries.assign(
            net_revenue=entries[amount_col].where(entries["pl_group"] == "NET_REVENUE", 0),
            cost_of_sales=entries[amount_col].where(entries["pl_group"] == "COST_OF_SALES", 0),
            direct_costs=entries[amount_col].where(entries["pl_subgroup"] == "DIRECT_COSTS", 0),
            indirect_costs=entries[amount_col].where(entries["pl_subgroup"] == "INDIRECT_COSTS", 0),
            unmapped=entries[amount_col].where(entries["pl_group"] == UNMAPPED, 0),
        )
        .groupby(by, dropna=False)[["net_revenue", "cost_of_sales", "direct_costs", "indirect_costs", "unmapped"]]
        .sum()
    )
    tpv_scope = fact_tpv[fact_tpv["batch_id"].isin(batches)]
    tpv = tpv_scope.groupby(by, dropna=False)[[tpv_col]].sum().rename(columns={tpv_col: "tpv"})
    summary = tpv.join(pnl, how="outer").fillna(0)
    summary["gross_profit"] = summary["net_revenue"] + summary["cost_of_sales"]
    summary["take_rate"] = safe_ratio(summary["net_revenue"], summary["tpv"])
    summary["gp_margin"] = safe_ratio(summary["gross_profit"], summary["net_revenue"])
    summary["gp_over_tpv"] = safe_ratio(summary["gross_profit"], summary["tpv"])
    return summary.round(6)


def safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.div(denominator.where(denominator != 0))


def closed_periods(calendar: pd.DataFrame) -> list[pd.Timestamp]:
    closed = calendar[calendar["period_status"] == "CLOSED"]
    return sorted(pd.to_datetime(closed["period"]).tolist())


INK = "#1b1b1b"
MUTED = "#8c8c8c"
GRID = "#e9e9e9"
NEUTRAL = "#c8c8c8"
ACCENT = "#c8423b"
ACCENT_2 = "#1f6f8b"
ACCENT_3 = "#e0a33a"
POSITIVE = "#2d8a5b"
SOURCE_NOTE = "Fonte: OrionPay (dados fictícios), lotes B01+B02. Valores em USD."


def set_chart_style() -> None:
    sns.set_theme(style="white")
    plt.rcParams.update(
        {
            "figure.dpi": 110,
            "savefig.dpi": 160,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": NEUTRAL,
            "axes.labelcolor": MUTED,
            "axes.titlesize": 12,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.spines.left": False,
            "axes.grid": True,
            "axes.grid.axis": "y",
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "ytick.left": False,
            "font.family": "DejaVu Sans",
            "legend.frameon": False,
        }
    )


def chart_titles(fig: plt.Figure, title: str, subtitle: str | None = None, source: str = SOURCE_NOTE) -> None:
    height = fig.get_figheight()
    fig.text(0.01, 1 - 0.22 / height, title, ha="left", va="top", fontsize=15, fontweight="bold", color=INK)
    if subtitle:
        fig.text(0.01, 1 - 0.55 / height, subtitle, ha="left", va="top", fontsize=10.5, color=MUTED)
    fig.text(0.01, 0.12 / height, source, ha="left", va="bottom", fontsize=8, color=MUTED)
    top = 1 - (1.05 if subtitle else 0.7) / height
    fig.tight_layout(rect=(0, 0.35 / height, 1, top))


def usd_formatter(decimals: int = 0) -> mticker.FuncFormatter:
    return mticker.FuncFormatter(lambda value, _: format_usd(value, decimals))


def pct_formatter(decimals: int = 1) -> mticker.FuncFormatter:
    return mticker.FuncFormatter(lambda value, _: f"{value * 100:.{decimals}f}%")


def format_usd(value: float, decimals: int = 1) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "–"
    sign = "-" if value < 0 else ""
    magnitude = abs(value)
    for threshold, suffix in ((1e6, "M"), (1e3, "k")):
        if magnitude >= threshold:
            return f"{sign}${magnitude / threshold:,.{decimals}f}{suffix}"
    return f"{sign}${magnitude:,.0f}"


def format_month(period: pd.Timestamp) -> str:
    months = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
    return f"{months[period.month - 1]}/{str(period.year)[2:]}"


def save_chart(fig: plt.Figure, name: str) -> Path:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    path = CHARTS_DIR / f"{name}.png"
    fig.savefig(path, bbox_inches="tight")
    return path


def waterfall(
    ax: plt.Axes,
    start: tuple[str, float],
    steps: pd.Series,
    end_label: str,
    highlight: str | None = None,
    min_step: float = 20,
) -> None:
    start_label, start_value = start
    small = steps.abs() < min_step
    if small.sum() > 1:
        steps = pd.concat([steps[~small], pd.Series({"Outros": steps[small].sum()})])
    labels = [start_label, *steps.index, end_label]
    running = start_value
    bottoms, heights, colors = [0.0], [start_value], [NEUTRAL]
    for label, delta in steps.items():
        bottoms.append(running if delta >= 0 else running + delta)
        heights.append(abs(delta))
        colors.append(ACCENT_3 if label == highlight else (POSITIVE if delta >= 0 else ACCENT))
        running += delta
    bottoms.append(0.0)
    heights.append(running)
    colors.append(INK)
    positions = range(len(labels))
    ax.bar(positions, heights, bottom=bottoms, color=colors, width=0.62)
    for pos, label, bottom, height in zip(positions, labels, bottoms, heights):
        is_step = 0 < pos < len(labels) - 1
        value = steps.iloc[pos - 1] if is_step else height
        text = f"{'+' if is_step and value >= 0 else ''}{format_usd(value)}"
        ax.text(pos, bottom + height, text, ha="center", va="bottom", fontsize=8.5, color=INK)
    ax.set_xticks(list(positions))
    ax.set_xticklabels(labels, rotation=35, ha="right")
    ax.yaxis.set_major_formatter(usd_formatter(1))
    top = max(b + h for b, h in zip(bottoms, heights))
    ax.set_ylim(min(0, min(bottoms)), max(ax.get_ylim()[1], top * 1.12))


def label_line_end(ax: plt.Axes, x, y, text: str, color: str, offset: float = 0) -> None:
    ax.annotate(
        text,
        xy=(x, y),
        xytext=(6, offset),
        textcoords="offset points",
        va="center",
        fontsize=9,
        color=color,
        fontweight="bold",
    )


def format_table(df: pd.DataFrame, usd: list[str] = (), pct: list[str] = (), integers: list[str] = ()):
    formats = {**{c: "${:,.0f}" for c in usd}, **{c: "{:.2%}" for c in pct}, **{c: "{:,.0f}" for c in integers}}
    return df.style.format(formats, na_rep="–")
