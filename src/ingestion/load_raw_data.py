"""
Bronze layer ingestion.
 
Loads the raw, UK-adapted policyholder source data as-is and persists it as
a Delta table with no cleaning, filtering, or transformation applied. This
preserves an untouched snapshot of the source data, which is required for
audit and traceability: every later layer can always be traced back to
exactly what the source system originally provided.
 
Data quality checks and any corrective logic belong to the Silver layer
(see src/dq_gate/rules.py), not here.
"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession

def get_spark_session(app_name: str = "policyholder-pipeline") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

def load_raw_policyholders(spark: SparkSession, source_path: str) -> DataFrame:
    """Read the source CSV with no cleaning or filtering applied."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(source_path)
    )

def write_bronze(df: DataFrame, bronze_path: str) -> None:
    """Persists the raw dataset as a Delta table at the given path."""
    df.write.format("delta").mode("overwrite").save(bronze_path)

def main():
    spark = get_spark_session()

    source_path = "data/raw/uk_policyholders_source.csv"
    bronze_path = "data/bronze/policyholders"

    raw_df = load_raw_policyholders(spark, source_path)
    write_bronze(raw_df, bronze_path)

    print(f"Bronze layer written to {bronze_path}: {raw_df.count()} rows.")
    raw_df.printSchema()

if __name__ == "__main__":
    main()