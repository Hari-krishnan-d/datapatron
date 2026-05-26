import json
from pyspark.sql.functions import col

# -------------------------
# LOAD CONFIG
# -------------------------
CONFIG="/Volumes/test_ui/bronze/new/migration_plan.json"

with open(CONFIG) as f:
    cfg=json.load(f)

pipelines=cfg["migration_plan"]["pipelines"]


# -------------------------
# READ S3
# -------------------------
def read_s3(source, table):

    conn   = source.get("connection", {})
    key    = conn.get("access_key")
    secret = conn.get("secret_key")
    bucket = source["location"]["bucket"]
    base   = source["location"].get("base_path", "").strip("/")
    filename = table["source_name"]          # No quote() — S3A encodes internally

    path = f"s3a://{bucket}/{base}/{filename}" if base else f"s3a://{bucket}/{filename}"
    print("READ S3:", path)

    reader = (spark.read
        .format(source.get("file_format", "csv"))
        .option("header", True)
        .option("inferSchema", True))

    if key and secret:
        # Pass credentials per-bucket (Unity Catalog compatible)
        # No endpoint set — S3A auto-detects the correct region from AWS
        reader = (reader
            .option(f"fs.s3a.bucket.{bucket}.access.key", key)
            .option(f"fs.s3a.bucket.{bucket}.secret.key", secret)
            .option(f"fs.s3a.bucket.{bucket}.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))
        print(f"AWS credentials loaded for bucket '{bucket}'")
    else:
        print("WARNING: No AWS credentials found — attempting with instance profile")

    return reader.load(path)


# -------------------------
# READ AZURE SQL
# -------------------------
def read_azure_sql(source, table):

    conn = source["connection"]
    jdbc = f"jdbc:sqlserver://{conn['host']}:1433;database={conn['database']}"

    print("READ SQL:", table["source_name"])

    return (spark.read.format("jdbc")
        .option("url",      jdbc)
        .option("dbtable",  table["source_name"])
        .option("user",     conn["user"])
        .option("password", conn["password"])
        .option("driver",   "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load())


# -------------------------
# WATERMARK INCREMENTAL
# -------------------------
def apply_incremental(df, table):

    wm  = table.get("watermark_column")
    if wm is None:
        return df

    tgt = table["target_table"]

    try:
        existing = spark.sql(f"select max({wm}) m from {tgt}").collect()[0]["m"]
        if existing:
            print("Applying watermark >", existing)
            df = df.filter(col(wm) > existing)
    except:
        print("First load / target empty")

    return df


# -------------------------
# ALIGN SCHEMA TO TARGET
# -------------------------
def align_schema(df, target):

    try:
        tgt = spark.table(target)
        for c in tgt.schema:
            src = [x for x in df.columns if x.lower() == c.name.lower()]
            if src:
                df = df.withColumnRenamed(src[0], c.name)
                df = df.withColumn(c.name, df[c.name].cast(c.dataType))
        return df
    except:
        return df


# -------------------------
# WRITE TABLE
# -------------------------
def write_table(df, table):

    print("WRITE:", table["target_table"])
    df = align_schema(df, table["target_table"])

    (df.write
        .mode(table.get("write_strategy", "append"))
        .saveAsTable(table["target_table"]))


# -------------------------
# MAIN EXECUTION LOOP
# -------------------------
for pipe in pipelines:

    source = pipe["source"]

    print("\n==============================")
    print("PIPELINE:", pipe["pipeline_id"])
    print("==============================")

    for table in pipe["tables"]:
        try:
            print("\nProcessing:", table["source_name"])

            if source["source_type"] == "s3":
                df = read_s3(source, table)
            elif source["source_type"] == "azure_sql":
                df = read_azure_sql(source, table)
            else:
                raise Exception("Unsupported source type")

            if table.get("load_type") == "incremental":
                df = apply_incremental(df, table)

            write_table(df, table)

        except Exception as e:
            print("FAILED:", table["source_name"])
            print("ERROR:", str(e))

print("\n===== INGESTION COMPLETED =====")