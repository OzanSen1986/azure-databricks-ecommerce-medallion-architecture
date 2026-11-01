# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Incremental loading in Databricks, with real e-commerce data
# MAGIC
# MAGIC **Idea:** each run processes only what is *new or changed* since the last successful run, never the whole history.
# MAGIC
# MAGIC ```
# MAGIC  new CSV files ──► BRONZE (Auto Loader)        only NEW FILES are read
# MAGIC                       │  Delta stream checkpoint
# MAGIC                       ▼
# MAGIC                    SILVER (MERGE)              only NEW BRONZE ROWS are cleaned; upsert by order_id
# MAGIC                       │  touched dates/months
# MAGIC                       ▼
# MAGIC                    GOLD (MERGE)                only the DAYS / MONTHS that changed are recomputed
# MAGIC ```
# MAGIC
# MAGIC The notebook **simulates daily deliveries** from your 1M-row file so you can watch each run:
# MAGIC
# MAGIC | Step | What arrives | What you should see |
# MAGIC |---|---|---|
# MAGIC | Delivery 1, 2 | new orders (consecutive date windows) | only the new file is processed |
# MAGIC | Run again | nothing | nothing is processed (idempotent) |
# MAGIC | Updates | changed status for existing orders | rows are **updated**, not duplicated |
# MAGIC | Stale replay | older versions of existing orders | ignored (older data never overwrites newer) |
# MAGIC | Re-delivered file | same orders, different file name | no duplicates in silver |
# MAGIC | Catch-up | 3 files at once | one run handles all of them |
# MAGIC
# MAGIC **Needs:** `ecommerce_orders_raw.csv` uploaded to the `source` folder of the volume (path printed by the setup cell). Serverless is fine.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration and setup

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", spark.sql("SELECT current_catalog()").first()[0])
dbutils.widgets.text("schema", "ecommerce_inc")
dbutils.widgets.text("volume", "raw")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")

BASE        = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SOURCE_FILE = f"{BASE}/source/ecommerce_orders_raw.csv"     # upload here
STAGING     = f"{BASE}/staging"                              # pre-split "deliveries" (not read by the pipeline)
LANDING     = f"{BASE}/landing/orders"                       # the pipeline watches THIS folder

def T(name): return f"{CATALOG}.{SCHEMA}.{name}"

BRONZE, SILVER, QUARANTINE = T("bronze_orders"), T("silver_orders"), T("silver_orders_quarantine")
GOLD_DAILY, GOLD_MCC, RUN_LOG = T("agg_daily_sales"), T("agg_monthly_category_country"), T("etl_run_log")
CHK_BRONZE, CHK_SILVER = f"{BASE}/_checkpoints/bronze", f"{BASE}/_checkpoints/silver"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
for d in ["source", "staging", "landing/orders"]:
    dbutils.fs.mkdirs(f"{BASE}/{d}")
print("Upload the CSV to:", SOURCE_FILE)
display(dbutils.fs.ls(f"{BASE}/source"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Simulate daily deliveries
# MAGIC Split the big file into 5 files by **order-date window** (about 7 months each), the way a real feed delivers time-ordered data. Lines are copied **byte for byte**, so the malformed rows survive and stay the pipeline's problem. Run this cell once.

# COMMAND ----------

def write_single_file(df_or_text, target_file, as_text=False, header=True):
    """Spark writes a folder of part files; this writes one and renames it to a clean single file."""
    tmp = f"{STAGING}/_tmp_write"
    dbutils.fs.rm(tmp, True)
    w = df_or_text.coalesce(1).write.mode("overwrite")
    (w.text(tmp) if as_text else w.option("header", header).csv(tmp))
    part = [f.path for f in dbutils.fs.ls(tmp) if f.name.startswith("part-")][0]
    dbutils.fs.cp(part, target_file)
    dbutils.fs.rm(tmp, True)

lines   = spark.read.text(SOURCE_FILE).filter(F.trim("value") != "")
HEADER  = lines.filter(F.col("value").startswith("order_id,")).first()[0]
data    = lines.filter(~F.col("value").startswith("order_id,"))

SPLIT_FORMATS = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "dd/MM/yyyy", "MMM dd, yyyy"]
od_raw = F.regexp_replace(F.regexp_extract("value", r'^[^,]*,("[^"]*"|[^,]*)', 1), '"', "")
od     = F.to_date(F.coalesce(*[F.expr(f"try_to_timestamp(od_raw, '{f}')") for f in SPLIT_FORMATS]))
months = F.months_between(od, F.to_date(F.lit("2023-01-01")))                # data spans Jan 2023 - Dec 2025
by_date = F.greatest(F.least(F.floor(months / 7.2) + 1, F.lit(5)), F.lit(1))  # 5 consecutive time windows
data = (data.withColumn("od_raw", od_raw)
            .withColumn("batch_no", F.when(od.isNotNull(), by_date).otherwise(F.pmod(F.hash("value"), F.lit(5)) + 1)))
hdr_row = spark.createDataFrame([(0, HEADER)], "k int, value string")

for b in range(1, 6):
    part = (data.filter(F.col("batch_no") == b).select("value").withColumn("k", F.lit(1))
                .unionByName(hdr_row).orderBy("k").select("value"))           # header first
    write_single_file(part, f"{STAGING}/orders_batch_0{b}.csv", as_text=True)

display(dbutils.fs.ls(STAGING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The incremental pipeline
# MAGIC The cleaning rules are the same as in the medallion notebook (dates, numbers, country/status mappings, quantity and price validation). **What is new is how data moves:**
# MAGIC
# MAGIC 1. **Bronze:** Auto Loader remembers processed files in its checkpoint.
# MAGIC 2. **Silver:** a Delta stream over Bronze (`availableNow`) remembers which Bronze rows it handled; `foreachBatch` cleans only those rows and **MERGEs** into Silver.
# MAGIC 3. **Reference data:** missing names/prices are filled from *this batch plus the Silver rows for the same customers/products* (instead of from the full history).
# MAGIC 4. **Gold:** recompute and MERGE only the days and months the batch touched.
# MAGIC 5. **Run log:** one audit row per batch.

# COMMAND ----------

# --- helpers shared with the full-refresh notebook (unchanged rules) -------------------------------
RAW_COLS = ["order_id", "order_date", "customer_id", "customer_name", "email", "country", "city",
            "product_id", "product_name", "category", "quantity", "unit_price", "discount",
            "shipping_cost", "total_amount", "payment_method", "order_status", "ship_date", "updated_at"]

NULL_TOKENS = ["", "n/a", "na", "null", "none", "unknown", "?", "-", "nan", "tbd"]

EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"

TS_FORMATS = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "dd/MM/yyyy", "MMM dd, yyyy"]

def map_col(mapping):
    """Literal map column. Missing keys return NULL (try_element_at is safe under ANSI mode)."""
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])

COUNTRY_MAP = {
    "united states": "United States", "usa": "United States", "us": "United States", "united states of america": "United States",
    "turkey": "Turkey", "türkiye": "Turkey", "turkiye": "Turkey", "tr": "Turkey",
    "germany": "Germany", "de": "Germany", "deutschland": "Germany",
    "united kingdom": "United Kingdom", "uk": "United Kingdom", "gb": "United Kingdom", "great britain": "United Kingdom",
    "france": "France", "fr": "France", "canada": "Canada", "ca": "Canada", "spain": "Spain", "es": "Spain",
    "italy": "Italy", "it": "Italy", "netherlands": "Netherlands", "nl": "Netherlands", "holland": "Netherlands",
    "australia": "Australia", "au": "Australia",
}
COUNTRY_CODE = {"United States": "US", "Turkey": "TR", "Germany": "DE", "United Kingdom": "GB", "France": "FR",
                "Canada": "CA", "Spain": "ES", "Italy": "IT", "Netherlands": "NL", "Australia": "AU"}
CATEGORY_MAP = {
    "electronics": "Electronics", "electroncs": "Electronics", "clothing": "Clothing", "cloths": "Clothing",
    "home & kitchen": "Home & Kitchen", "home and kitchen": "Home & Kitchen", "books": "Books", "book": "Books",
    "sports": "Sports", "beauty": "Beauty", "toys": "Toys", "toy": "Toys",
}
PAYMENT_MAP = {
    "credit card": "Credit Card", "cc": "Credit Card", "paypal": "PayPal", "debit card": "Debit Card", "debit": "Debit Card",
    "bank transfer": "Bank Transfer", "wire": "Bank Transfer", "cash on delivery": "Cash on Delivery", "cod": "Cash on Delivery",
}
STATUS_MAP = {
    "delivered": "Delivered", "shipped": "Shipped", "processing": "Processing",
    "cancelled": "Cancelled", "canceled": "Cancelled", "returned": "Returned",
}
WORD_NUMS = {"one": 1.0, "two": 2.0, "three": 3.0, "four": 4.0, "five": 5.0, "six": 6.0, "ten": 10.0}


def parse_ts(c):
    """First format that parses wins; no match → NULL (never raises, even in ANSI mode)."""
    return F.coalesce(*[F.expr(f"try_to_timestamp(`{c}`, '{f}')") for f in TS_FORMATS])

def to_num(c):
    """'$1,234.50' / '12.5 USD' / '€9.99' → double; garbage → NULL."""
    return F.expr(f"try_cast(regexp_replace(`{c}`, '[^0-9.-]', '') as double)")

def flag(cond, name):
    return F.when(cond, F.lit(name))

def non_null(arr):
    return F.filter(arr, lambda x: x.isNotNull())

def save_table(df, name):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name))

def stage(df, name):
    """Materialize an intermediate result so later steps don't recompute the whole lineage."""
    save_table(df, name)
    return df.sparkSession.table(name)

def mode_of(df, key, val, out):
    """Most frequent non-null value of `val` per `key` (ties broken alphabetically)."""
    cnt = df.filter(F.col(key).isNotNull() & F.col(val).isNotNull()).groupBy(key, val).count()
    w = Window.partitionBy(key).orderBy(F.col("count").desc(), F.col(val))
    return (cnt.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
               .select(key, F.col(val).alias(out)))

# COMMAND ----------

def standardize(b):
    """Row-level cleaning: strings, dates, numbers, categorical mapping. Adds typed helper columns."""
    t = (b.select(*RAW_COLS, "_rescued_data", "_source_file", "_ingested_at")
           .withColumn("raw_record", F.to_json(F.struct(*RAW_COLS))))         # original row, kept for quarantine

    for c in RAW_COLS:                                                       # trim + null tokens
        v = F.trim(F.col(c))
        t = t.withColumn(c, F.when(F.lower(v).isin(NULL_TOKENS), F.lit(None)).otherwise(v))

    qty_num   = F.coalesce(F.try_element_at(map_col(WORD_NUMS), F.lower("quantity")), to_num("quantity"))
    disc_raw  = to_num("discount")
    disc_frac = F.when(F.col("discount").contains("%") | (disc_raw > 1), disc_raw / 100).otherwise(disc_raw)
    ctry_key  = F.regexp_replace(F.lower("country"), r"\.", "")

    t = (t
      .withColumn("order_id_ok", F.coalesce(F.col("order_id").rlike("^ORD[0-9]{8}$"), F.lit(False)))
      .withColumn("order_ts", parse_ts("order_date"))
      .withColumn("order_dt", F.to_date("order_ts"))
      .withColumn("upd_ts", F.coalesce(parse_ts("updated_at"), F.col("order_ts")))
      .withColumn("ship_dt", F.to_date(F.expr("try_to_timestamp(ship_date, 'yyyy-MM-dd')")))
      .withColumn("cust_id", F.when(F.upper("customer_id").rlike("^C[0-9]{6}$"), F.upper("customer_id")))
      .withColumn("cust_name", F.initcap(F.lower("customer_name")))
      .withColumn("cust_email", F.when(F.lower("email").rlike(EMAIL_RE), F.lower("email")))
      .withColumn("ctry", F.try_element_at(map_col(COUNTRY_MAP), ctry_key))
      .withColumn("cty", F.initcap(F.lower("city")))
      .withColumn("prod_id", F.when(F.upper("product_id").rlike("^P[0-9]{5}$"), F.upper("product_id")))
      .withColumn("prod_name", F.col("product_name"))
      .withColumn("cat", F.try_element_at(map_col(CATEGORY_MAP), F.lower("category")))
      .withColumn("qty_num", qty_num)
      .withColumn("qty", F.when((F.col("qty_num") > 0) & (F.col("qty_num") <= 100000)
                                & (F.col("qty_num") == F.floor("qty_num")), F.col("qty_num").cast("int")))
      .withColumn("price_in", to_num("unit_price"))
      .withColumn("disc_in", F.when(disc_frac.between(0, 0.9), disc_frac))
      .withColumn("ship_in", to_num("shipping_cost"))
      .withColumn("total_in", to_num("total_amount"))
      .withColumn("pay", F.try_element_at(map_col(PAYMENT_MAP), F.lower("payment_method")))
      .withColumn("status", F.try_element_at(map_col(STATUS_MAP), F.lower("order_status")))
    )
    return t

# COMMAND ----------

# --- final derivations, flags and reject reasons (same rules as the full-refresh notebook) -------------
def reject_reasons(with_price=True):
    price_rule = [flag(F.col("price_final").isNull(), "price_unresolvable")] if with_price else []
    return non_null(F.array(
        flag(F.col("_rescued_data").isNotNull(), "malformed_row"),
        flag(~F.col("order_id_ok"), "invalid_order_id"),
        flag(F.col("order_ts").isNull(), "unparseable_order_date"),
        flag(F.col("order_dt") < F.to_date(F.lit("2020-01-01")), "order_date_too_old"),
        flag(F.col("order_dt") > F.current_date(), "order_date_in_future"),
        flag(F.col("qty").isNull(), "invalid_quantity"),
        flag(F.col("qty") > 100, "quantity_outlier"),
        flag(F.col("prod_id").isNull(), "invalid_product_id"),
        *price_rule,
    ))

def finalize(e):
    """Derive final business columns, DQ flags (non-fatal) and reject reasons (fatal)."""
    price_ok     = F.col("price_in") > 0
    price_outlier = F.coalesce(price_ok & (F.col("price_in") > 10 * F.col("ref_median_price")), F.lit(False))

    e = (e
      .withColumn("price_final", F.when(price_ok & ~price_outlier, F.col("price_in")).otherwise(F.col("ref_median_price")))
      .withColumn("disc_final", F.coalesce("disc_in", F.lit(0.0)))
      .withColumn("ship_final", F.coalesce(F.when(F.col("ship_in") >= 0, F.col("ship_in")), F.lit(0.0)))
      .withColumn("gross",  F.round(F.col("qty") * F.col("price_final"), 2))
      .withColumn("disc_amt", F.round(F.col("gross") * F.col("disc_final"), 2))
      .withColumn("net",    F.round(F.col("gross") - F.col("disc_amt"), 2))
      .withColumn("total_calc", F.round(F.col("net") + F.col("ship_final"), 2))
      .withColumn("ship_dt_ok", F.when(F.col("ship_dt") >= F.col("order_dt"), F.col("ship_dt")))
    )

    # --- Step 5: flags (non-fatal) and reject reasons (fatal) --------------------------------------
    dq_flags = non_null(F.array(
        flag(F.col("cust_recovered"), "customer_id_recovered_from_email"),
        flag(F.col("cust_id").isNull(), "customer_unknown"),
        flag(F.col("cust_name").isNull() & F.col("ref_name").isNotNull(), "customer_name_filled"),
        flag(F.col("cust_email").isNull() & F.col("ref_email").isNotNull(), "email_filled_or_fixed"),
        flag(F.col("ctry").isNull() & F.col("ref_country").isNotNull(), "country_filled"),
        flag(F.col("cty").isNull() & F.col("ref_city").isNotNull(), "city_filled"),
        flag(F.col("cat").isNull(), "category_filled_from_product"),
        flag(F.col("price_in").isNull() | ~price_ok, "price_imputed_from_product_median"),
        flag(price_outlier, "price_outlier_replaced"),
        flag(F.col("discount").isNull(), "discount_missing_assumed_0"),
        flag(F.col("discount").isNotNull() & F.col("disc_in").isNull(), "discount_invalid_assumed_0"),
        flag(F.col("ship_in").isNull() | (F.col("ship_in") < 0), "shipping_missing_assumed_0"),
        flag(F.col("total_in").isNull(), "source_total_missing"),
        flag(F.col("total_in").isNotNull() & (F.abs(F.col("total_in") - F.col("total_calc")) > 0.05), "source_total_mismatch"),
        flag(F.col("ship_dt").isNotNull() & F.col("ship_dt_ok").isNull(), "ship_date_before_order_date"),
        flag(F.col("status").isNull(), "status_unknown"),
    ))

    e = e.withColumn("reject_reasons", reject_reasons()).withColumn("dq_flags", dq_flags)
    return e


# COMMAND ----------

def upsert(sp, df, table, keys, newer_col=None):
    """Create the table on first use, otherwise MERGE. With `newer_col`, a matched row is only
    replaced when the incoming row is strictly newer (so stale or repeated data changes nothing)."""
    if not sp.catalog.tableExists(table):
        try:
            df.write.format("delta").saveAsTable(table)
            return
        except Exception as e:
            if "TABLE_OR_VIEW_ALREADY_EXISTS" not in str(e):
                raise
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    m = DeltaTable.forName(sp, table).alias("t").merge(df.alias("s"), cond)
    m = m.whenMatchedUpdateAll(condition=f"s.{newer_col} > t.{newer_col}") if newer_col else m.whenMatchedUpdateAll()
    m.whenNotMatchedInsertAll().execute()

POOL_COLS = ["cust_email", "cust_id", "cust_name", "ctry", "cty", "prod_id", "prod_name", "cat", "price_in"]

def build_pool(d, silver):
    """Rows used to look up reference values: this batch + Silver rows sharing a customer, e-mail or product."""
    pool = d.select(*POOL_COLS)
    if silver is None:
        return pool
    s = silver.filter(F.col("customer_id") != "UNKNOWN").select(
        "order_id",
        F.col("customer_email").alias("cust_email"),
        F.col("customer_id").alias("cust_id"),
        F.when(F.col("customer_name") != "Unknown",F.col("customer_name")).alias("cust_name"),
        F.when(F.col("country") != "Unknown",F.col("country")).alias("ctry"),
        F.when(F.col("city") != "Unknown", F.col("city")).alias("cty"),
        F.col("product_id").alias("prod_id"),
        F.when(F.col("product_name") != "Unknown",F.col("product_name")).alias("prod_name"),
        F.when(F.col("category") != "Unknown",F.col("category")).alias("cat"),
        F.col("unit_price").cast("double").alias("price_in"))
    keys = lambda c: d.select(c).filter(F.col(c).isNotNull()).distinct()
    sub = (s.join(keys("cust_id"), "cust_id", "left_semi")
            .unionByName(s.join(keys("cust_email"), "cust_email", "left_semi"))
            .unionByName(s.join(keys("prod_id"), "prod_id", "left_semi"))
            .dropDuplicates(["order_id"]).drop("order_id"))
    return pool.unionByName(sub)

def enrich(d, pool):
    ref_cid = mode_of(pool, "cust_email", "cust_id", "ref_cust_id")             # recover customer_id via e-mail
    d = (d.join(ref_cid, "cust_email", "left")
          .withColumn("cust_recovered", F.col("cust_id").isNull() & F.col("ref_cust_id").isNotNull())
          .withColumn("cust_id", F.coalesce("cust_id", "ref_cust_id")).drop("ref_cust_id"))
    cust_refs = [mode_of(pool, "cust_id", "cust_name", "ref_name"), mode_of(pool, "cust_id", "cust_email", "ref_email"),
                 mode_of(pool, "cust_id", "ctry", "ref_country"),   mode_of(pool, "cust_id", "cty", "ref_city")]
    prod_refs = [mode_of(pool, "prod_id", "prod_name", "ref_prod_name"), mode_of(pool, "prod_id", "cat", "ref_cat"),
                 (pool.filter(F.col("price_in") > 0).groupBy("prod_id")
                      .agg(F.percentile_approx("price_in", 0.5).alias("ref_median_price")))]
    for r in cust_refs: d = d.join(r, "cust_id", "left")
    for r in prod_refs: d = d.join(r, "prod_id", "left")
    return finalize(d)

def to_silver(e):
    return e.select(
        "order_id", "order_ts", F.col("order_dt").alias("order_date"),
        F.coalesce("cust_id", F.lit("UNKNOWN")).alias("customer_id"),
        F.coalesce("cust_name", "ref_name", F.lit("Unknown")).alias("customer_name"),
        F.coalesce("cust_email", "ref_email").alias("customer_email"),
        F.coalesce("ctry", "ref_country", F.lit("Unknown")).alias("country"),
        F.try_element_at(map_col(COUNTRY_CODE), F.coalesce("ctry", "ref_country")).alias("country_code"),
        F.coalesce("cty", "ref_city", F.lit("Unknown")).alias("city"),
        F.col("prod_id").alias("product_id"),
        F.coalesce("ref_prod_name", "prod_name", F.lit("Unknown")).alias("product_name"),
        F.coalesce("ref_cat", "cat", F.lit("Unknown")).alias("category"),
        F.col("qty").alias("quantity"),
        F.col("price_final").cast("decimal(12,2)").alias("unit_price"),
        F.col("disc_final").cast("decimal(5,4)").alias("discount_rate"),
        F.col("ship_final").cast("decimal(12,2)").alias("shipping_cost"),
        F.col("gross").cast("decimal(14,2)").alias("gross_amount"),
        F.col("disc_amt").cast("decimal(14,2)").alias("discount_amount"),
        F.col("net").cast("decimal(14,2)").alias("net_amount"),
        F.col("total_calc").cast("decimal(14,2)").alias("total_amount"),
        F.coalesce("pay", F.lit("Unknown")).alias("payment_method"),
        F.coalesce("status", F.lit("Unknown")).alias("order_status"),
        F.col("ship_dt_ok").alias("ship_date"),
        "dq_flags",
        F.col("upd_ts").alias("source_updated_at"),            # <- the "version" used to decide which row wins
        "_source_file", "_ingested_at",
        F.current_timestamp().alias("_silver_processed_at"))

# COMMAND ----------

REVENUE_STATUSES = ["Delivered", "Shipped"]

def gold_daily(s):
    is_rev = F.col("order_status").isin(REVENUE_STATUSES)
    rev = lambda c: F.sum(F.when(is_rev, F.col(c)).otherwise(0))
    return s.groupBy("order_date").agg(
        F.countDistinct("order_id").alias("orders"), F.sum("quantity").alias("units"),
        rev("gross_amount").alias("gross_sales"), rev("discount_amount").alias("discounts"), rev("net_amount").alias("net_revenue"),
        F.sum(F.when(F.col("order_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_orders"),
        F.sum(F.when(F.col("order_status") == "Returned", 1).otherwise(0)).alias("returned_orders"))

def gold_mcc(s):
    is_rev = F.col("order_status").isin(REVENUE_STATUSES)
    return (s.withColumn("month", F.trunc("order_date", "month")).groupBy("month", "category", "country").agg(
        F.countDistinct("order_id").alias("orders"), F.sum("quantity").alias("units"),
        F.sum(F.when(is_rev, F.col("net_amount")).otherwise(0)).alias("net_revenue")))

STG, STG_CHG = T("_stg_inc_batch"), T("_stg_inc_changed")

def process_batch(batch_df, batch_id):
    """Runs once per micro-batch of NEW bronze rows."""
    sp = batch_df.sparkSession
    if batch_df.isEmpty():
        return
    n_in = batch_df.count()

    std      = standardize(batch_df)
    bad_keys = std.filter(~F.col("order_id_ok"))
    w_dup    = Window.partitionBy("order_id").orderBy(F.col("upd_ts").desc_nulls_last(), F.col("_ingested_at").desc())
    dedup    = std.filter("order_id_ok").withColumn("rn", F.row_number().over(w_dup)).filter("rn = 1").drop("rn")

    silver_exists = sp.catalog.tableExists(SILVER)
    silver = sp.table(SILVER) if silver_exists else None
    e = stage(enrich(dedup, build_pool(dedup, silver)), STG)           # compute once, reuse below

    # --- quarantine (append) ---
    cols = ["order_id", "reject_reasons", "raw_record", "_source_file", "_ingested_at"]
    q = (e.filter(F.size("reject_reasons") > 0).select(*cols)
          .unionByName(bad_keys.withColumn("reject_reasons", reject_reasons(with_price=False)).select(*cols))
          .withColumn("_bronze_batch_id", F.lit(batch_id)).withColumn("_quarantined_at", F.current_timestamp()))
    n_quar = q.count()
    if n_quar:
        q.write.format("delta").mode("append").saveAsTable(QUARANTINE)

    # --- silver: classify against what Silver already holds, then MERGE only what changed ---
    cand = to_silver(e.filter(F.size("reject_reasons") == 0))
    if silver_exists:
        cmp = cand.select("order_id", "source_updated_at").join(
            silver.select("order_id", F.col("source_updated_at").alias("t_upd")), "order_id", "left")
        n_new  = cmp.filter("t_upd IS NULL").count()
        n_upd  = cmp.filter("t_upd IS NOT NULL AND source_updated_at > t_upd").count()
        n_skip = cmp.filter("t_upd IS NOT NULL AND source_updated_at <= t_upd").count()
        changed = cand.join(cmp.filter("t_upd IS NULL OR source_updated_at > t_upd").select("order_id"), "order_id", "left_semi")
    else:
        n_new, n_upd, n_skip = cand.count(), 0, 0
        changed = cand
    n_days = 0
    if n_new + n_upd > 0:
        changed = stage(changed, STG_CHG)          # freeze the rows to merge (independent of Silver's state)
        upsert(sp, changed, SILVER, ["order_id"], newer_col="source_updated_at")

        # --- gold: recompute ONLY the days / months touched by changed rows, then MERGE ---
        silver_now = sp.table(SILVER)
        days   = changed.select("order_date").distinct()
        months = changed.select(F.trunc("order_date", "month").alias("month")).distinct()
        n_days = days.count()
        upsert(sp, gold_daily(silver_now.join(days, "order_date", "left_semi")), GOLD_DAILY, ["order_date"])
        touched_m = silver_now.withColumn("month", F.trunc("order_date", "month")).join(months, "month", "left_semi")
        upsert(sp, gold_mcc(touched_m), GOLD_MCC, ["month", "category", "country"])

    # --- run log ---
    (sp.createDataFrame([(int(batch_id), n_in, n_new, n_upd, n_skip, n_quar, n_days)],
        "bronze_batch_id int, rows_in long, new_orders long, updated_orders long, ignored_not_newer long, quarantined long, days_recomputed long")
       .withColumn("run_at", F.current_timestamp()).write.format("delta").mode("append").saveAsTable(RUN_LOG))
    for t in (STG, STG_CHG):
        sp.sql(f"DROP TABLE IF EXISTS {t}")

# COMMAND ----------

def run_incremental():
    # 1) BRONZE: Auto Loader reads only files it has not seen
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv").option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", f"{BASE}/_schemas/orders")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(LANDING)
        .select("*", F.col("_metadata.file_path").alias("_source_file"),
                F.col("_metadata.file_modification_time").alias("_file_modified_at"),
                F.current_timestamp().alias("_ingested_at"))
        .writeStream.option("checkpointLocation", CHK_BRONZE).option("mergeSchema", "true")
        .trigger(availableNow=True).toTable(BRONZE).awaitTermination())

    # 2) SILVER + GOLD: stream over new bronze rows only, MERGE per micro-batch
    (spark.readStream.table(BRONZE)
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHK_SILVER)
        .trigger(availableNow=True).start().awaitTermination())

def deliver(batch_no=None, name=None, from_staging=None):
    """Simulate a file arriving in the landing folder."""
    src = from_staging or f"{STAGING}/orders_batch_0{batch_no}.csv"
    dst = f"{LANDING}/{name or src.split('/')[-1]}"
    dbutils.fs.cp(src, dst)
    print("delivered ->", dst)

def status(title):
    print(f"\n=== {title} ===")
    for label, t in [("bronze rows", BRONZE), ("silver rows", SILVER), ("quarantined", QUARANTINE)]:
        print(f"{label:12s}", spark.table(t).count() if spark.catalog.tableExists(t) else 0)
    if spark.catalog.tableExists(RUN_LOG):
        display(spark.table(RUN_LOG).orderBy(F.desc("run_at")).limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. First delivery (initial load)

# COMMAND ----------

deliver(1)
run_incremental()
status("after delivery 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Second delivery: only the new file is processed
# MAGIC Look at the run log: `rows_in` is about the size of **one file**, not the total, and `days_recomputed` covers only the days in that file's date window.

# COMMAND ----------

deliver(2)
run_incremental()
status("after delivery 2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run again with no new file: nothing happens
# MAGIC Auto Loader sees no new file and the silver stream sees no new bronze rows, so no new row appears in the run log.

# COMMAND ----------

before = spark.table(RUN_LOG).count()
run_incremental()
print("new log rows:", spark.table(RUN_LOG).count() - before, "(expected 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Updates: existing orders change status
# MAGIC Real sources send changed rows again with a newer `updated_at`. We pick ~1% of the orders already in Silver and send them as "Returned".

# COMMAND ----------

def make_update_files():
    batch1 = (spark.read.option("header", "true").csv(f"{STAGING}/orders_batch_01.csv")      # all strings
              .join(spark.table(SILVER).select("order_id"), "order_id", "left_semi")
              .filter(F.expr("pmod(hash(order_id), 100) = 0")))
    upd   = batch1.withColumn("order_status", F.lit("Returned")).withColumn("updated_at", F.lit("2026-01-15 10:00:00"))
    stale = batch1.withColumn("order_status", F.lit("Processing")).withColumn("updated_at", F.lit("2000-01-01 00:00:00"))
    write_single_file(upd,   f"{STAGING}/orders_batch_06_updates.csv")
    write_single_file(stale, f"{STAGING}/orders_batch_07_stale_replay.csv")
    return upd.select("order_id").first()[0], upd.count()

sample_id, n_changed = make_update_files()
show = lambda: display(spark.table(SILVER).filter(F.col("order_id") == sample_id)
                       .select("order_id", "order_status", "source_updated_at"))
print(f"{n_changed} orders will be updated; watching {sample_id}")
show()                                           # BEFORE

# COMMAND ----------

deliver(name="orders_batch_06_updates.csv", from_staging=f"{STAGING}/orders_batch_06_updates.csv")
run_incremental()
status("after updates")
show()                                           # AFTER: status = Returned, silver row count unchanged

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Stale replay: older versions arrive late
# MAGIC The same orders arrive again with `updated_at` in the year 2000. The MERGE condition `s.source_updated_at > t.source_updated_at` makes the pipeline ignore them: look at `ignored_not_newer` in the log. Status stays **Returned**.

# COMMAND ----------

deliver(name="orders_batch_07_stale_replay.csv", from_staging=f"{STAGING}/orders_batch_07_stale_replay.csv")
run_incremental()
status("after stale replay")
show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Same data delivered twice under a new file name
# MAGIC Auto Loader tracks **files**, so a renamed copy is a "new" file and lands in Bronze again (Bronze is a raw log). Silver must not duplicate: `new_orders` and `updated_orders` should be 0 and `days_recomputed` 0 (nothing changed, so Gold is left alone).

# COMMAND ----------

deliver(2, name="orders_batch_02_redelivery.csv")
run_incremental()
status("after re-delivery")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Catch-up: several files in one run

# COMMAND ----------

for b in (3, 4, 5):
    deliver(b)
run_incremental()
status("after catch-up")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Prove the incremental result is correct
# MAGIC Recompute Gold **from scratch** out of Silver and compare with the incrementally maintained Gold. They must match.

# COMMAND ----------

s = spark.table(SILVER)
checks = {
    "silver order_id unique": s.count() == s.select("order_id").distinct().count(),
    "daily gold == full recompute": (gold_daily(s).subtract(spark.table(GOLD_DAILY).select(*gold_daily(s).columns)).count() == 0
                                     and spark.table(GOLD_DAILY).count() == s.select("order_date").distinct().count()),
    "monthly gold == full recompute": (gold_mcc(s).subtract(spark.table(GOLD_MCC).select(*gold_mcc(s).columns)).count() == 0),
}
for k, ok in checks.items():
    print("PASS" if ok else "FAIL", "-", k)
display(spark.table(RUN_LOG).orderBy("run_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. The same idea for other kinds of sources
# MAGIC | Source | "What is new?" is answered by | Where progress is remembered |
# MAGIC |---|---|---|
# MAGIC | Files in cloud storage (this notebook) | Auto Loader file tracking | Stream checkpoint |
# MAGIC | Another Delta table, append-only | Streaming read with `availableNow` | Stream checkpoint |
# MAGIC | Another Delta table with updates/deletes | Change Data Feed | Checkpoint or last version number |
# MAGIC | Database table with `modified_at` | High-water mark query | Control table |
# MAGIC | Database without `modified_at` | ID watermark, CDC tool, or hash compare | Control table / CDC log |
# MAGIC
# MAGIC High-water-mark version of the same Silver step, for a MySQL source:
# MAGIC ```python
# MAGIC last = spark.table("ctl.watermark").filter("pipeline='orders'").first()["last_value"]
# MAGIC new  = (spark.read.format("jdbc").option("url", url)
# MAGIC         .option("dbtable", f"(SELECT * FROM orders WHERE modified_at > '{last}' - INTERVAL 10 MINUTE) q").load())
# MAGIC process_batch(new, batch_id=0)          # same cleaning + MERGE; then advance the watermark AFTER success
# MAGIC ```
# MAGIC
# MAGIC ### Rules that make it safe
# MAGIC 1. **Idempotent writes:** MERGE on a business key, never blind append into Silver.
# MAGIC 2. **A version column** (`source_updated_at`) so late or repeated data cannot overwrite newer data.
# MAGIC 3. **Advance progress only after the write succeeds** (the stream checkpoint does this for you).
# MAGIC 4. **Recompute only what changed** downstream (touched days/months), and keep a way to rebuild everything.
# MAGIC 5. **Log every run** (rows in, new, updated, ignored, quarantined) so a silent failure is visible.
# MAGIC 6. **Deletes** are invisible to file and watermark loads: use soft-delete flags, CDC or Change Data Feed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Reset (optional): start the demo over

# COMMAND ----------

for t in [BRONZE, SILVER, QUARANTINE, GOLD_DAILY, GOLD_MCC, RUN_LOG, STG, STG_CHG]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
for p in [f"{BASE}/_checkpoints", f"{BASE}/_schemas", LANDING]:
    dbutils.fs.rm(p, True)
dbutils.fs.mkdirs(LANDING)