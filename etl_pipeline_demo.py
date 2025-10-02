#!/usr/bin/env python3
"""
Apache Iceberg ETL Pipeline Demo

This demo showcases Iceberg's key features:
1. Time Travel - Access historical data snapshots
2. Branching - Manage different data environments
3. Schema Evolution - Modify schemas safely
4. ACID Transactions - Ensure data consistency
"""

import os
import shutil
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg,
    count,
    current_timestamp,
)
import time


class IcebergETLDemo:
    def __init__(self):
        self.warehouse_path = "./iceberg_warehouse"
        self.catalog_name = "demo_catalog"
        self.spark = self._create_spark_session()
        self._setup_catalog()
        
    def _create_spark_session(self):
        """Create Spark session configured for Iceberg only."""
        jar_path = self._resolve_iceberg_jar()

        builder = (
            SparkSession.builder.appName("IcebergETLDemo")
            .config("spark.jars", jar_path)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions."
                "IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog"
            )
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config(
                f"spark.sql.catalog.{self.catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{self.catalog_name}.type", "hadoop")
            .config(
                f"spark.sql.catalog.{self.catalog_name}.warehouse",
                self.warehouse_path,
            )
        )
        # Derive versions for logging (best effort)
        jar_file = os.path.basename(jar_path)
        spark_minor = scala_bin = iceberg_ver = "?"
    # Expected pattern:
    # iceberg-spark-runtime-<sparkMinor>_<scala>-<iceberg>.jar
        try:
            core = jar_file.replace(
                "iceberg-spark-runtime-", ""
            ).rstrip(".jar")
            left, iceberg_ver = core.rsplit('-', 1)
            spark_minor, scala_bin = left.split('_', 1)
        except Exception:  # pragma: no cover - best effort only
            pass
        print(
            "✅ Using Iceberg JAR (Spark "
            f"{spark_minor} / Scala {scala_bin} / Iceberg {iceberg_ver}): "
            f"{jar_path}"
        )
        return builder.getOrCreate()

    def _resolve_iceberg_jar(self) -> str:
        """Resolve the Iceberg Spark runtime JAR to use.

        Resolution order:
        1. Environment variable ICEBERG_JAR (absolute or relative path)
        2. A jar in ./jars that matches current PySpark minor version
        3. Fallback to the lexicographically last iceberg-spark-runtime-* jar
        """
        env_path = os.getenv("ICEBERG_JAR")
        if env_path and os.path.exists(env_path):
            print(f"ℹ️  Using ICEBERG_JAR from environment: {env_path}")
            return env_path

        jars_dir = os.path.join(os.getcwd(), "jars")
        if not os.path.isdir(jars_dir):
            raise FileNotFoundError(
                "jars/ directory not found. Run "
                "scripts/download_iceberg_jars.py first."
            )

        try:
            import pyspark  # local import to avoid early import errors
            spark_minor = ".".join(pyspark.__version__.split(".")[:2])
        except Exception:
            spark_minor = None

        candidates = []
        for name in os.listdir(jars_dir):
            if (
                name.startswith("iceberg-spark-runtime-")
                and name.endswith(".jar")
            ):
                candidates.append(name)

        if not candidates:
            raise FileNotFoundError(
                "No iceberg-spark-runtime-*.jar found in jars/. "
                "Run scripts/download_iceberg_jars.py to fetch one."
            )

        # Prefer matching current PySpark minor version if available
        if spark_minor:
            for name in sorted(candidates):
                if f"iceberg-spark-runtime-{spark_minor}_" in name:
                    return os.path.join(jars_dir, name)

        # Fallback: pick latest (lexicographically last) jar
        selected = sorted(candidates)[-1]
        print(
            "⚠️  No jar matched current PySpark minor; falling back to: "
            f"{selected}"
        )
        return os.path.join(jars_dir, selected)
    
    def _setup_catalog(self):
        """Setup Iceberg catalog (required)."""
        os.makedirs(self.warehouse_path, exist_ok=True)
        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.etl_demo"
        )
        print(
            "✅ Created / confirmed Iceberg catalog + database: "
            f"{self.catalog_name}.etl_demo"
        )

    def reset_pipeline_tables(self):
        """Drop existing demo tables and branches so the script is idempotent.

        Safe to run even if objects don't exist. Ensures that repeated
        executions start from a clean slate without manual cleanup.
        """
        print(
            "\n♻️  Resetting any existing demo tables/branches "
            "(idempotent)..."
        )
        raw = f"{self.catalog_name}.etl_demo.raw_transactions"
        intermediate = (
            f"{self.catalog_name}.etl_demo.intermediate_transactions"
        )
        final = f"{self.catalog_name}.etl_demo.final_metrics"
        # Drop branches if they exist (Iceberg 1.10 supports DROP BRANCH)
        for branch in ["staging", "dev"]:
            try:
                self.spark.sql(
                    f"ALTER TABLE {raw} DROP BRANCH {branch}"
                )
                print(f"  - Dropped branch '{branch}'")
            except Exception:
                pass  # Ignore if branch doesn't exist
        for tbl in [final, intermediate, raw]:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {tbl}")
                print(f"  - Dropped table if existed: {tbl}")
            except Exception as e:
                print(f"  - Warning: could not drop {tbl}: {e}")
        print("✅ Reset complete")
        
    def cleanup(self):
        """Clean up resources"""
        if os.path.exists(self.warehouse_path):
            shutil.rmtree(self.warehouse_path)
        self.spark.stop()
    
    def generate_sample_data(self, num_records=1000):
        """Generate sample raw data"""
        import random
        
        # Sample data representing e-commerce transactions
        products = ["laptop", "mouse", "keyboard", "monitor", "headphones"]
        regions = ["north", "south", "east", "west"]
        
        data = []
        base_time = datetime.now() - timedelta(days=7)
        
        for i in range(num_records):
            record = {
                "transaction_id": f"txn_{i:06d}",
                "product": random.choice(products),
                "region": random.choice(regions),
                "quantity": random.randint(1, 10),
                "unit_price": round(random.uniform(10.0, 1000.0), 2),
                "customer_id": f"cust_{random.randint(1, 100):03d}",
                "timestamp": base_time
                + timedelta(hours=random.randint(0, 168))
            }
            record["total_amount"] = record["quantity"] * record["unit_price"]
            data.append(record)
        
        return self.spark.createDataFrame(data)
    
    def create_raw_table(self):
        """Create and populate Iceberg raw data table."""
        print("🔄 Creating raw data table...")
        df = self.generate_sample_data(500)
        table_name = f"{self.catalog_name}.etl_demo.raw_transactions"
        # createOrReplace keeps reruns idempotent
        df.writeTo(table_name).using("iceberg").createOrReplace()
        self.raw_table = table_name
        print(f"✅ Created raw_transactions table with {df.count()} records")
        return df
    
    def create_intermediate_table(self):
        """Create intermediate Iceberg table with cleaned data."""
        print("🔄 Creating intermediate data table...")
        raw_df = self.spark.table(self.raw_table)
        intermediate_df = (
            raw_df.filter(col("quantity") > 0)
                  .filter(col("unit_price") > 0)
                  .withColumn("revenue", col("quantity") * col("unit_price"))
                  .withColumn("processed_at", current_timestamp())
                  .select(
                      "transaction_id", "product", "region", "quantity",
                      "unit_price", "revenue", "customer_id",
                      "timestamp", "processed_at"
                  )
        )
        table_name = f"{self.catalog_name}.etl_demo.intermediate_transactions"
        intermediate_df.writeTo(table_name).using("iceberg").createOrReplace()
        self.intermediate_table = table_name
        print(
            "✅ Created intermediate_transactions table with "
            f"{intermediate_df.count()} records"
        )
        return intermediate_df
    
    def create_final_table(self):
        """Create final aggregated Iceberg table."""
        print("🔄 Creating final aggregated table...")
        intermediate_df = self.spark.table(self.intermediate_table)
        final_df = (
            intermediate_df.groupBy("product", "region")
            .agg(
                spark_sum("revenue").alias("total_revenue"),
                avg("unit_price").alias("avg_unit_price"),
                count("transaction_id").alias("transaction_count"),
                spark_sum("quantity").alias("total_quantity"),
            )
            .withColumn("aggregated_at", current_timestamp())
        )
        table_name = f"{self.catalog_name}.etl_demo.final_metrics"
        final_df.writeTo(table_name).using("iceberg").createOrReplace()
        self.final_table = table_name
        print(f"✅ Created final_metrics table with {final_df.count()} records")
        return final_df
    
    def simulate_data_updates(self):
        """Simulate additional data updates for time travel demo."""
        print("\n🔄 Simulating data updates for time travel demo...")
        new_data = self.generate_sample_data(200)
        new_data.writeTo(self.raw_table).using("iceberg").append()
        print("✅ Added 200 new transactions")
        time.sleep(2)

        raw_df = self.spark.table(self.raw_table)
        intermediate_df = (
            raw_df.filter(col("quantity") > 0)
                  .filter(col("unit_price") > 0)
                  .withColumn("revenue", col("quantity") * col("unit_price"))
                  .withColumn("processed_at", current_timestamp())
                  .select(
                      "transaction_id", "product", "region", "quantity",
                      "unit_price", "revenue", "customer_id",
                      "timestamp", "processed_at"
                  )
        )
        # Full table overwrite (new snapshot)
        intermediate_df.writeTo(self.intermediate_table) \
            .using("iceberg").overwritePartitions()
        print("✅ Updated intermediate_transactions table")
        time.sleep(2)

        final_df = (
            intermediate_df.groupBy("product", "region")
            .agg(
                spark_sum("revenue").alias("total_revenue"),
                avg("unit_price").alias("avg_unit_price"),
                count("transaction_id").alias("transaction_count"),
                spark_sum("quantity").alias("total_quantity"),
            )
            .withColumn("aggregated_at", current_timestamp())
        )
        final_df.writeTo(self.final_table).using("iceberg") \
            .overwritePartitions()
        print("✅ Updated final_metrics table")
    
    def demonstrate_time_travel(self):
        """Demonstrate Iceberg time travel comparing earliest vs
        latest snapshot."""
        print("\n🕰️  DEMONSTRATING TIME TRAVEL")
        print("=" * 50)
        try:
            print("\n📋 Table History (Snapshots):")
            history = self.spark.sql(
                f"SELECT * FROM {self.raw_table}.history"
            )
            history.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Could not access table history: {e}")
            return

        snapshots = history.collect()
        if len(snapshots) < 2:
            print(
                "⚠️ Not enough snapshots yet for a comparison. "
                "Add more data updates."
            )
            return

        first_snapshot = snapshots[0]["snapshot_id"]
        latest_snapshot = snapshots[-1]["snapshot_id"]

        print(
            f"\n📊 Data at first snapshot (ID: {first_snapshot}):"
        )
        first_snapshot_df = self.spark.read.option(
            "snapshot-id", str(first_snapshot)
        ).format("iceberg").load(self.raw_table)
        (
            first_snapshot_df.groupBy("product").count()
            .withColumnRenamed("count", "transaction_count")
            .orderBy("transaction_count", ascending=False)
            .show()
        )

        print(
            f"\n📊 Data at latest snapshot (ID: {latest_snapshot}):"
        )
        latest_snapshot_df = self.spark.read.option(
            "snapshot-id", str(latest_snapshot)
        ).format("iceberg").load(self.raw_table)
        (
            latest_snapshot_df.groupBy("product").count()
            .withColumnRenamed("count", "transaction_count")
            .orderBy("transaction_count", ascending=False)
            .show()
        )
    
    def demonstrate_time_travel_sql(self):
        """Show SQL time travel using VERSION AS OF and TIMESTAMP AS OF.

        This supplements the snapshot-id reader example with pure SQL
        syntax so users can see both approaches side-by-side.
        """
        print("\n🕰️  DEMONSTRATING TIME TRAVEL (SQL Syntax)")
        print("=" * 50)
        try:
            snapshots_df = self.spark.sql(
                f"""
                SELECT snapshot_id, committed_at
                FROM {self.raw_table}.snapshots
                ORDER BY committed_at
                """
            )
            rows = snapshots_df.collect()
        except Exception as e:
            print(f"⚠️ Could not read snapshots metadata: {e}")
            return

        if len(rows) < 1:
            print("⚠️ No snapshots available yet.")
            return

        # Use the earliest snapshot for a deterministic example.
        first_id = rows[0].snapshot_id
        first_ts = rows[0].committed_at  # Python datetime

        # Format timestamp to millisecond precision for Spark SQL literal.
        # Spark accepts TIMESTAMP 'yyyy-MM-dd HH:mm:ss.SSS'
        ts_literal = first_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        print(
            f"First snapshot id: {first_id}\n"
            f"Committed at: {first_ts} (-> {ts_literal})"
        )
        print("\n📊 Products (VERSION AS OF <snapshot_id>):")
        try:
            self.spark.sql(
                f"""
                SELECT product, COUNT(*) AS transaction_count
                FROM {self.raw_table} VERSION AS OF {first_id}
                GROUP BY product
                ORDER BY transaction_count DESC
                """
            ).show()
        except Exception as e:
            print(f"⚠️ VERSION AS OF query failed: {e}")

        print("\n📊 Products (TIMESTAMP AS OF <timestamp>):")
        try:
            self.spark.sql(
                f"""
                SELECT product, COUNT(*) AS transaction_count
                FROM {self.raw_table} TIMESTAMP AS OF TIMESTAMP '{ts_literal}'
                GROUP BY product
                ORDER BY transaction_count DESC
                """
            ).show()
        except Exception as e:
            print(f"⚠️ TIMESTAMP AS OF query failed: {e}")
        
        print(
            "\nℹ️  Both queries should produce identical counts "
            "(same snapshot)."
        )
    
    def demonstrate_branching(self):
        """Demonstrate Iceberg branching capabilities."""
        print("\n🌿 DEMONSTRATING BRANCHING")
        print("=" * 50)
        try:
            print("\n🔧 Creating 'dev' branch...")
            self.spark.sql(f"ALTER TABLE {self.raw_table} CREATE BRANCH dev")
            print("✅ Created 'dev' branch")

            print("\n📝 Adding experimental data to 'dev' branch...")
            experimental_data = self.spark.createDataFrame(
                [
                    ("exp_001", "tablet", "north", 5, 299.99,
                     "exp_customer", datetime.now(), 1499.95),
                    ("exp_002", "smartwatch", "south", 2, 199.99,
                     "exp_customer", datetime.now(), 399.98),
                ],
                [
                    "transaction_id", "product", "region", "quantity",
                    "unit_price", "customer_id", "timestamp", "total_amount",
                ],
            )
            experimental_data.writeTo(
                f"{self.catalog_name}.etl_demo.raw_transactions.branch_dev"
            ).using("iceberg").append()
            print("✅ Added experimental data to 'dev' branch")

            print("\n📊 Data in main branch:")
            main_count = self.spark.sql(
                "SELECT COUNT(*) as count FROM "
                f"{self.catalog_name}.etl_demo.raw_transactions"
            ).collect()[0]["count"]
            print(f"Records in main: {main_count}")

            print("\n📊 Data in dev branch:")
            dev_count = self.spark.sql(
                f"SELECT COUNT(*) as count FROM "
                f"{self.catalog_name}.etl_demo.raw_transactions.branch_dev"
            ).collect()[0]["count"]
            print(f"Records in dev: {dev_count}")

            print("\n🆕 New products in dev branch:")
            dev_products = self.spark.sql(
                f"""SELECT DISTINCT product
                FROM {self.catalog_name}.etl_demo.raw_transactions.branch_dev
                WHERE product NOT IN (
                    SELECT DISTINCT product FROM
                    {self.catalog_name}.etl_demo.raw_transactions
                )"""
            )
            dev_products.show()

            print("\n🔄 Creating 'staging' branch from 'dev'...")
            # Iceberg SQL parser (v1.10) lacks "AS OF BRANCH <name>".
            # Fetch the snapshot_id of 'dev' and create 'staging' at that
            # snapshot using AS OF VERSION.
            dev_snapshot_row = self.spark.sql(
                f"SELECT snapshot_id FROM "
                f"{self.catalog_name}.etl_demo.raw_transactions.refs "
                "WHERE name='dev'"
            ).collect()
            if not dev_snapshot_row:
                raise RuntimeError(
                    "Dev branch snapshot not found; cannot create staging."
                )
            dev_snapshot_id = dev_snapshot_row[0][0]
            self.spark.sql(
                "ALTER TABLE "
                f"{self.raw_table} CREATE BRANCH staging AS OF VERSION "
                f"{dev_snapshot_id}"
            )
            print(
                "✅ Created 'staging' branch from 'dev' (snapshot "
                f"{dev_snapshot_id})"
            )

            print("\n📝 All branches:")
            branches = self.spark.sql(
                "SELECT * FROM "
                f"{self.catalog_name}.etl_demo.raw_transactions.refs"
            )
            branches.show()
        except Exception as e:
            print(
                "⚠️ Branching demo encountered an error (maybe not "
                f"supported in this Iceberg version): {e}"
            )
    
    def demonstrate_schema_evolution(self):
        """Demonstrate Iceberg schema evolution."""
        print("\n🔄 DEMONSTRATING SCHEMA EVOLUTION")
        print("=" * 50)
        print("\n📋 Current schema:")
        current_schema = self.spark.table(self.raw_table).schema
        for field in current_schema.fields:
            print(f"  - {field.name}: {field.dataType}")

        try:
            print("\n➕ Adding 'discount_amount' column...")
            self.spark.sql(
                f"ALTER TABLE {self.raw_table} ADD COLUMN "
                "discount_amount DOUBLE"
            )
            print("✅ Added 'discount_amount' column")
        except Exception as e:
            print(f"⚠️ Could not add column (maybe already exists): {e}")

        print("\n📝 Inserting data with new column...")
        new_data_with_discount = self.spark.createDataFrame([
            (
                "new_001", "smartphone", "east", 1, 599.99,
                "new_customer", datetime.now(), 599.99, 50.0,
            ),
            (
                "new_002", "case", "west", 3, 29.99,
                "new_customer", datetime.now(), 89.97, 5.0,
            ),
        ], [
            "transaction_id",
            "product",
            "region",
            "quantity",
            "unit_price",
            "customer_id",
            "timestamp",
            "total_amount",
            "discount_amount",
        ])
        new_data_with_discount.writeTo(
            f"{self.catalog_name}.etl_demo.raw_transactions"
        ).using("iceberg").append()

        print("\n📋 Updated schema:")
        updated_schema = self.spark.table(
            f"{self.catalog_name}.etl_demo.raw_transactions"
        ).schema
        for field in updated_schema.fields:
            print(f"  - {field.name}: {field.dataType}")

        print("\n📊 Data with discount information:")
        discount_data = self.spark.sql(
            f"""
            SELECT transaction_id,
                   product,
                   total_amount,
                   COALESCE(discount_amount, 0.0) AS discount_amount,
                   (total_amount - COALESCE(discount_amount, 0.0))
                       AS final_amount
            FROM {self.catalog_name}.etl_demo.raw_transactions
            WHERE transaction_id LIKE 'new_%'
            """
        )
        discount_data.show()
    
    def show_metadata_and_lineage(self):
        """Show Iceberg table metadata and lineage information."""
        print("\n📊 METADATA AND LINEAGE")
        print("=" * 50)

        print("\n🏷️  Table Properties:")
        try:
            properties = self.spark.sql(f"SHOW TBLPROPERTIES {self.raw_table}")
            properties.show(truncate=False)
        except Exception as e:
            print(f"Table properties not available: {e}")

        print("\n📁 Table Files:")
        try:
            files = self.spark.sql(
                "SELECT * FROM "
                f"{self.catalog_name}.etl_demo.raw_transactions.files"
            )
            files.select(
                "file_path", "file_format", "record_count",
                "file_size_in_bytes"
            ).show(truncate=False)
        except Exception as e:
            print(f"File information not available: {e}")

        print("\n📸 Table Snapshots:")
        try:
            snapshots = self.spark.sql(
                "SELECT committed_at, snapshot_id, operation, summary "
                "FROM "
                f"{self.catalog_name}.etl_demo.raw_transactions.snapshots"
            )
            snapshots.show(truncate=False)
        except Exception as e:
            print(f"Snapshot information not available: {e}")


def main():
    """Main demo function"""
    print("🚀 Apache Iceberg ETL Pipeline Demo")
    print("=" * 50)
    
    demo = IcebergETLDemo()
    
    try:
        # Create the ETL pipeline
        print("\n📦 SETTING UP ETL PIPELINE")
        print("=" * 30)
        
        demo.create_raw_table()
        demo.create_intermediate_table()
        demo.create_final_table()
        
        # Simulate updates for time travel
        demo.simulate_data_updates()
        
        # Demonstrate key features
        demo.demonstrate_time_travel()
        demo.demonstrate_time_travel_sql()
        demo.demonstrate_branching()
        demo.demonstrate_schema_evolution()
        demo.show_metadata_and_lineage()
        
        print("\n🎉 Demo completed successfully!")
        print("\nKey takeaways:")
        print("✅ Time Travel: Query historical data snapshots")
        print("✅ Branching: Manage different data environments")
        print("✅ Schema Evolution: Safely modify table schemas")
        print("✅ ACID Transactions: Ensure data consistency")
        print("✅ Metadata Management: Track data lineage and changes")
        
    except Exception as e:
        print(f"❌ Demo encountered an error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        print("\n🧹 Cleaning up...")
        # demo.cleanup()
        # print("✅ Cleanup completed")
 

if __name__ == "__main__":
    main()
