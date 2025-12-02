# Databricks notebook source
# MAGIC %pip install --quiet databricks-vectorsearch
# MAGIC %restart_python
# MAGIC #!/usr/bin/env python3
# MAGIC import sys
# MAGIC import time
# MAGIC import logging
# MAGIC import requests
# MAGIC import pandas as pd
# MAGIC import io
# MAGIC
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.vector_search.client import VectorSearchClient
# MAGIC
# MAGIC # Configure logging
# MAGIC logging.basicConfig(
# MAGIC     level=logging.INFO,
# MAGIC     format="%(asctime)s %(levelname)s %(message)s",
# MAGIC     handlers=[logging.StreamHandler(sys.stdout)]
# MAGIC )
# MAGIC logger = logging.getLogger(__name__)
# MAGIC
# MAGIC def create_catalog_and_schema(spark, catalog: str, schema: str):
# MAGIC     try:
# MAGIC         spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
# MAGIC         spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
# MAGIC         logger.info(f"Ensured catalog `{catalog}` and schema `{schema}`.")
# MAGIC     except Exception as e:
# MAGIC         logger.error(f"Error creating catalog/schema: {e}")
# MAGIC         raise
# MAGIC
# MAGIC def load_csv_to_table(spark, table: str, url: str, catalog: str, schema: str):
# MAGIC     try:
# MAGIC         logger.info(f"Loading table `{table}` from {url}")
# MAGIC         resp = requests.get(url)
# MAGIC         resp.raise_for_status()
# MAGIC         df = pd.read_csv(io.StringIO(resp.text))
# MAGIC         spark_df = spark.createDataFrame(df)
# MAGIC         full_table = f"{catalog}.{schema}.{table}"
# MAGIC         spark_df.write.mode("overwrite").saveAsTable(full_table)
# MAGIC         logger.info(f"Created table {full_table}")
# MAGIC     except Exception as e:
# MAGIC         logger.error(f"Error loading CSV for table {table}: {e}")
# MAGIC         raise
# MAGIC
# MAGIC def enable_cdf(spark, catalog: str, schema: str, table: str):
# MAGIC     """Enable Delta change data feed (CDF) on the table."""
# MAGIC     full_name = f"{catalog}.{schema}.{table}"
# MAGIC     try:
# MAGIC         logger.info(f"Enabling CDF on {full_name}")
# MAGIC         spark.sql(f"""
# MAGIC             ALTER TABLE {full_name}
# MAGIC             SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC         """)
# MAGIC         logger.info(f"CDF enabled on {full_name}")
# MAGIC     except Exception as e:
# MAGIC         logger.warning(f"Could not enable CDF on {full_name}: {e}")
# MAGIC         # We don’t necessarily stop — maybe it's already enabled or unsupported.
# MAGIC
# MAGIC def ensure_endpoint(client: VectorSearchClient, endpoint_name: str,
# MAGIC                     timeout: int = 600, poll_interval: int = 10):
# MAGIC     try:
# MAGIC         eps = client.list_endpoints().get("endpoints", [])
# MAGIC         names = [ep["name"] for ep in eps]
# MAGIC         if endpoint_name not in names:
# MAGIC             logger.info(f"Creating vector search endpoint '{endpoint_name}'")
# MAGIC             client.create_endpoint(name=endpoint_name, endpoint_type="STANDARD")
# MAGIC         else:
# MAGIC             logger.info(f"Endpoint '{endpoint_name}' already exists")
# MAGIC
# MAGIC         start = time.time()
# MAGIC         while True:
# MAGIC             ep_info = client.get_endpoint(endpoint_name)
# MAGIC             state = ep_info.get("endpoint_status", {}).get("state", "UNKNOWN")
# MAGIC             logger.info(f"Endpoint state: {state}")
# MAGIC             if state in ("ONLINE", "PROVISIONED", "READY"):
# MAGIC                 logger.info(f"Endpoint '{endpoint_name}' ready")
# MAGIC                 break
# MAGIC             if state in ("FAILED", "OFFLINE"):
# MAGIC                 raise RuntimeError(f"Endpoint '{endpoint_name}' failed (state={state})")
# MAGIC             if time.time() - start > timeout:
# MAGIC                 raise TimeoutError(f"Timed out waiting for endpoint '{endpoint_name}' to be ready")
# MAGIC             time.sleep(poll_interval)
# MAGIC     except Exception as e:
# MAGIC         logger.error(f"Error ensuring endpoint: {e}")
# MAGIC         raise
# MAGIC
# MAGIC def index_exists(client: VectorSearchClient, endpoint_name: str, index_name: str) -> bool:
# MAGIC     try:
# MAGIC         resp = client.list_indexes(name=endpoint_name)
# MAGIC         for idx in resp.get("indexes", []):
# MAGIC             if idx.get("name") == index_name:
# MAGIC                 return True
# MAGIC         return False
# MAGIC     except Exception as e:
# MAGIC         logger.warning(f"Could not list indexes (assuming none exist): {e}")
# MAGIC         return False
# MAGIC
# MAGIC def create_delta_index(client: VectorSearchClient, endpoint_name: str,
# MAGIC                        source_table: str, index_name: str,
# MAGIC                        pipeline_type: str, primary_key: str, embedding_column: str):
# MAGIC     if index_exists(client, endpoint_name, index_name):
# MAGIC         logger.info(f"Index '{index_name}' already exists — skipping")
# MAGIC         return
# MAGIC
# MAGIC     try:
# MAGIC         logger.info(f"Creating index '{index_name}' on {source_table}")
# MAGIC         client.create_delta_sync_index(
# MAGIC             endpoint_name=endpoint_name,
# MAGIC             index_name=index_name,
# MAGIC             source_table_name=source_table,
# MAGIC             pipeline_type=pipeline_type,
# MAGIC             primary_key=primary_key,
# MAGIC             embedding_source_column=embedding_column,
# MAGIC             embedding_model_endpoint_name="databricks-gte-large-en"
# MAGIC         )
# MAGIC         logger.info(f"Index '{index_name}' created successfully")
# MAGIC     except Exception as ex:
# MAGIC         msg = str(ex)
# MAGIC         # Detect “no change data feed” error
# MAGIC         if "does not have change data feed enabled" in msg:
# MAGIC             logger.info(f"Detected missing CDF error for {source_table}. Trying to enable CDF and retry.")
# MAGIC             # Parse catalog.schema.table
# MAGIC             parts = source_table.split(".")
# MAGIC             if len(parts) == 3:
# MAGIC                 cat, sch, tbl = parts
# MAGIC                 try:
# MAGIC                     enable_cdf(spark, cat, sch, tbl)
# MAGIC                     logger.info(f"Retrying index creation '{index_name}' after enabling CDF")
# MAGIC                     client.create_delta_sync_index(
# MAGIC                         endpoint_name=endpoint_name,
# MAGIC                         index_name=index_name,
# MAGIC                         source_table_name=source_table,
# MAGIC                         pipeline_type=pipeline_type,
# MAGIC                         primary_key=primary_key,
# MAGIC                         embedding_source_column=embedding_column,
# MAGIC                         embedding_model_endpoint_name="databricks-gte-large-en"
# MAGIC                     )
# MAGIC                     logger.info(f"Index '{index_name}' created after retry")
# MAGIC                     return
# MAGIC                 except Exception as e2:
# MAGIC                     logger.error(f"Retry failed for index '{index_name}': {e2}")
# MAGIC             else:
# MAGIC                 logger.error(f"Could not parse table name '{source_table}' to enable CDF retry")
# MAGIC         logger.error(f"Failed to create index '{index_name}': {msg}")
# MAGIC         raise
# MAGIC
# MAGIC def main(spark):
# MAGIC     catalog = "bricks_lab"
# MAGIC     schema = "default"
# MAGIC     endpoint_name = "bricks_endpoint"
# MAGIC     base_url = "https://raw.githubusercontent.com/databricks/tmm/main/bricks-workshop/data"
# MAGIC     csv_files = {
# MAGIC         "billing": f"{base_url}/billing.csv",
# MAGIC         "customers": f"{base_url}/customers.csv",
# MAGIC         "knowledge_base": f"{base_url}/knowledge_base.csv",
# MAGIC         "support_tickets": f"{base_url}/support_tickets.csv"
# MAGIC     }
# MAGIC
# MAGIC     # Your specified keys
# MAGIC     kb_pk = "kb_id"
# MAGIC     kb_text = "formatted_content"
# MAGIC     st_pk = "ticket_id"
# MAGIC     st_text = "formatted_content"
# MAGIC
# MAGIC     # Create catalog and schema
# MAGIC     create_catalog_and_schema(spark, catalog, schema)
# MAGIC
# MAGIC     # Load tables
# MAGIC     for tbl, url in csv_files.items():
# MAGIC         load_csv_to_table(spark, tbl, url, catalog, schema)
# MAGIC
# MAGIC     # Ensure vector search endpoint
# MAGIC     vs_client = VectorSearchClient()
# MAGIC     ensure_endpoint(vs_client, endpoint_name)
# MAGIC
# MAGIC     # Enable CDF on source tables
# MAGIC     try:
# MAGIC         enable_cdf(spark, catalog, schema, "knowledge_base")
# MAGIC         enable_cdf(spark, catalog, schema, "support_tickets")
# MAGIC     except Exception as e:
# MAGIC         logger.warning(f"Failed enabling CDF for one or more tables: {e}")
# MAGIC
# MAGIC     # Create indexes
# MAGIC     kb_full = f"{catalog}.{schema}.knowledge_base"
# MAGIC     st_full = f"{catalog}.{schema}.support_tickets"
# MAGIC     idx_kb = f"{catalog}.{schema}.knowledge_base_index"
# MAGIC     idx_st = f"{catalog}.{schema}.support_tickets_index"
# MAGIC
# MAGIC     create_delta_index(vs_client, endpoint_name, kb_full, idx_kb, "TRIGGERED", kb_pk, kb_text)
# MAGIC     create_delta_index(vs_client, endpoint_name, st_full, idx_st, "TRIGGERED", st_pk, st_text)
# MAGIC
# MAGIC     logger.info("Setup complete.")
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC     try:
# MAGIC         main(spark)
# MAGIC     except NameError:
# MAGIC         logger.error("This script must run in Databricks environment where `spark` is defined.")
# MAGIC         sys.exit(1)
# MAGIC     except Exception as e:
# MAGIC         logger.error(f"Setup script failed: {e}")
# MAGIC         sys.exit(1)
