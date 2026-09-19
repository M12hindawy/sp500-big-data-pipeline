"""
Spark Batch Pipeline for S&P 500 Stocks Dataset
Architecture:
Bronze CSV (MinIO)
  -> Explicit Schema & Validation
  -> Quarantine Invalid Records
  -> Deduplication on (symbol, trading_date)
  -> Financial Feature Derivation
  -> Company Metadata Join
  -> Silver Parquet (partitioned by year, month)
  -> Gold Parquet (daily_market_metrics)
  -> PostgreSQL Serving Layer (idempotent upsert/staging)
  -> Data Quality & Pipeline Run Auditing
"""
import sys
import uuid
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from common import (
    create_spark_session, STOCKS_RAW_SCHEMA, COMPANIES_SCHEMA,
    apply_validation_rules, calculate_derived_features, get_postgres_properties
)

def run_batch_pipeline():
    run_id = str(uuid.uuid4())[:8]
    start_time = datetime.utcnow()
    print(f"[{start_time}] Starting S&P 500 Batch Pipeline (Run ID: {run_id})")

    spark = create_spark_session("SP500_Batch_Pipeline")
    spark.sparkContext.setLogLevel("WARN")

    bronze_stocks_path = "s3a://stock-data/bronze/stocks/sp500_stocks.csv"
    bronze_companies_path = "s3a://stock-data/bronze/company/sp500_companies.csv"
    silver_output_path = "s3a://stock-data/silver/stocks"
    gold_output_path = "s3a://stock-data/gold/daily_market_metrics"
    quarantine_output_path = "s3a://stock-data/quarantine/batch"

    input_rows = 0
    valid_rows = 0
    rejected_rows = 0

    try:
        # 1. READ RAW STOCKS
        print(f"Reading raw stocks from {bronze_stocks_path}...")
        raw_df = (
            spark.read
            .option("header", "true")
            .schema(STOCKS_RAW_SCHEMA)
            .csv(bronze_stocks_path)
            .withColumn("trading_date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        )

        input_rows = raw_df.count()
        print(f"Total raw observations read: {input_rows}")

        # 2. VALIDATION & QUARANTINE
        validated_df = apply_validation_rules(raw_df)
        
        valid_df = validated_df.filter(F.col("is_valid") == True).drop("is_valid", "rejection_reason")
        invalid_df = validated_df.filter(F.col("is_valid") == False).withColumn("rejected_at", F.current_timestamp())

        rejected_rows = invalid_df.count()
        valid_rows = valid_df.count()
        print(f"Validation summary: Valid={valid_rows} | Rejected={rejected_rows}")

        if rejected_rows > 0:
            print(f"Quarantining {rejected_rows} invalid records to {quarantine_output_path}...")
            invalid_df.write.mode("append").parquet(quarantine_output_path)

        # 3. DETERMINISTIC DEDUPLICATION
        # Business key: (symbol, trading_date)
        window_spec = Window.partitionBy("symbol", "trading_date").orderBy(F.col("volume").desc())
        deduped_df = (
            valid_df
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num", "date")
        )
        deduped_count = deduped_df.count()
        duplicates_count = valid_rows - deduped_count
        print(f"Deduplication: Kept={deduped_count} | Removed Duplicates={duplicates_count}")

        # 4. FEATURE DERIVATIONS
        enriched_df = calculate_derived_features(deduped_df)

        # 5. COMPANY METADATA JOIN
        print(f"Reading company metadata from {bronze_companies_path}...")
        try:
            companies_df = (
                spark.read
                .option("header", "true")
                .schema(COMPANIES_SCHEMA)
                .csv(bronze_companies_path)
                .select(
                    F.upper(F.trim(F.col("symbol"))).alias("comp_symbol"),
                    F.col("company"),
                    F.col("sector"),
                    F.col("sub_industry")
                )
            )
            final_df = (
                enriched_df.join(companies_df, enriched_df["symbol"] == companies_df["comp_symbol"], "left")
                .drop("comp_symbol")
                .na.fill({"company": "Unknown", "sector": "Unknown", "sub_industry": "Unknown"})
            )
        except Exception as e:
            print(f"Warning: Could not join company metadata ({e}). Proceeding without company enrichment.")
            final_df = (
                enriched_df
                .withColumn("company", F.lit("Unknown"))
                .withColumn("sector", F.lit("Unknown"))
                .withColumn("sub_industry", F.lit("Unknown"))
            )

        # 6. WRITE SILVER PARQUET (Partitioned by year, month)
        print(f"Writing Silver Parquet to {silver_output_path}...")
        (
            final_df.write
            .mode("overwrite")
            .partitionBy("year", "month")
            .parquet(silver_output_path)
        )
        print("Silver Parquet write completed.")

        # 7. WRITE GOLD PARQUET
        print(f"Writing Gold Parquet to {gold_output_path}...")
        (
            final_df.write
            .mode("overwrite")
            .parquet(gold_output_path)
        )
        print("Gold Parquet write completed.")

        # 8. WRITE TO POSTGRESQL (Serving Layer)
        pg_url, pg_properties = get_postgres_properties()
        print("Writing Gold table to PostgreSQL (daily_market_metrics)...")
        
        # Select target columns for Postgres
        pg_columns = [
            "trading_date", "symbol", "company", "sector", "sub_industry",
            "open", "high", "low", "close", "adj_close", "volume",
            "price_change", "price_change_pct", "intraday_range", "intraday_range_pct",
            "typical_price", "dollar_volume", "processed_at"
        ]
        pg_df = final_df.select([
            F.col(c) if c not in ["open", "high", "low", "close", "adj_close"]
            else F.col(c).alias(f"{c}_price")
            for c in pg_columns
        ])

        # Write to staging table then perform idempotent upsert
        pg_df.write.jdbc(
            url=pg_url,
            table="stg_daily_market_metrics",
            mode="overwrite",
            properties=pg_properties
        )

        # Execute upsert in Postgres via JDBC driver connection
        conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            pg_url, pg_properties["user"], pg_properties["password"]
        )
        stmt = conn.createStatement()
        upsert_sql = """
            INSERT INTO daily_market_metrics (
                trading_date, symbol, company, sector, sub_industry,
                open_price, high_price, low_price, close_price, adj_close_price, volume,
                price_change, price_change_pct, intraday_range, intraday_range_pct,
                typical_price, dollar_volume, processed_at
            )
            SELECT 
                trading_date, symbol, company, sector, sub_industry,
                open_price, high_price, low_price, close_price, adj_close_price, volume,
                price_change, price_change_pct, intraday_range, intraday_range_pct,
                typical_price, dollar_volume, processed_at
            FROM stg_daily_market_metrics
            ON CONFLICT (symbol, trading_date) DO UPDATE SET
                company = EXCLUDED.company,
                sector = EXCLUDED.sector,
                sub_industry = EXCLUDED.sub_industry,
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                adj_close_price = EXCLUDED.adj_close_price,
                volume = EXCLUDED.volume,
                price_change = EXCLUDED.price_change,
                price_change_pct = EXCLUDED.price_change_pct,
                intraday_range = EXCLUDED.intraday_range,
                intraday_range_pct = EXCLUDED.intraday_range_pct,
                typical_price = EXCLUDED.typical_price,
                dollar_volume = EXCLUDED.dollar_volume,
                processed_at = EXCLUDED.processed_at;
        """
        stmt.execute(upsert_sql)
        stmt.close()
        conn.close()
        print("PostgreSQL daily_market_metrics upsert completed successfully.")

        # 9. RECORD DATA QUALITY RESULTS & PIPELINE RUN AUDIT
        end_time = datetime.utcnow()
        conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            pg_url, pg_properties["user"], pg_properties["password"]
        )
        stmt = conn.createStatement()

        # Insert Data Quality Checks
        dq_sql = f"""
            INSERT INTO data_quality_results (run_id, dataset, check_name, expected, actual, status, details)
            VALUES 
            ('{run_id}', 'sp500_stocks', 'ROW_COUNT', '> 0', '{input_rows}', 'PASSED', 'Total rows read from Bronze'),
            ('{run_id}', 'sp500_stocks', 'REJECTED_ROWS', '0', '{rejected_rows}', '{'PASSED' if rejected_rows == 0 else 'WARNING'}', 'Quarantined invalid rows'),
            ('{run_id}', 'sp500_stocks', 'DUPLICATES', '0', '{duplicates_count}', '{'PASSED' if duplicates_count == 0 else 'INFO'}', 'Deduplicated on (symbol, trading_date)');
        """
        stmt.execute(dq_sql)

        # Insert Pipeline Run Audit
        run_sql = f"""
            INSERT INTO pipeline_runs (run_id, pipeline_name, start_time, end_time, status, input_rows, output_rows, rejected_rows)
            VALUES ('{run_id}', 'SP500_Batch_Pipeline', '{start_time.isoformat()}', '{end_time.isoformat()}', 'SUCCESS', {input_rows}, {deduped_count}, {rejected_rows});
        """
        stmt.execute(run_sql)
        stmt.close()
        conn.close()

        print("============================================================")
        print("SPARK BATCH PIPELINE FINISHED SUCCESSFULLY")
        print(f"Run ID:        {run_id}")
        print(f"Input Rows:    {input_rows}")
        print(f"Output Rows:   {deduped_count}")
        print(f"Rejected Rows: {rejected_rows}")
        print("============================================================")

    except Exception as e:
        print(f"BATCH PIPELINE FAILED: {e}", file=sys.stderr)
        end_time = datetime.utcnow()
        try:
            pg_url, pg_properties = get_postgres_properties()
            conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
                pg_url, pg_properties["user"], pg_properties["password"]
            )
            stmt = conn.createStatement()
            err_msg = str(e).replace("'", "''")
            fail_sql = f"""
                INSERT INTO pipeline_runs (run_id, pipeline_name, start_time, end_time, status, input_rows, output_rows, rejected_rows, error_message)
                VALUES ('{run_id}', 'SP500_Batch_Pipeline', '{start_time.isoformat()}', '{end_time.isoformat()}', 'FAILED', {input_rows}, 0, {rejected_rows}, '{err_msg[:500]}');
            """
            stmt.execute(fail_sql)
            stmt.close()
            conn.close()
        except Exception:
            pass
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_batch_pipeline()
