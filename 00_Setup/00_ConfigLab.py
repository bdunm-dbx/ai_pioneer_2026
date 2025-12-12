# Databricks notebook source
# MAGIC %md
# MAGIC Add some notes here -- also, this probably isn't surfaced to users and just people setting up.
# MAGIC
# MAGIC Runs on Serverless Env 4

# COMMAND ----------

# MAGIC %pip install --quiet databricks-vectorsearch
# MAGIC %restart_python

# COMMAND ----------

import sys
import time
import logging
import os
import shutil
import pandas as pd

from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def create_catalog_and_schema(spark, catalog: str, schema: str):
    """Create Unity Catalog and schema if they don't exist."""
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        logger.info(f"Ensured catalog `{catalog}` and schema `{schema}` exist.")
    except Exception as e:
        logger.error(f"Error creating catalog/schema: {e}")
        raise

def load_csv_to_table(spark, table: str, file_path: str, catalog: str, schema: str):
    """Load CSV from workspace into Delta table using pandas."""
    try:
        logger.info(f"Loading table `{table}` from {file_path}")
        
        workspace_path = file_path.replace("file:", "")
        df = pd.read_csv(workspace_path)
        spark_df = spark.createDataFrame(df)
        
        full_table = f"{catalog}.{schema}.{table}"
        spark_df.write.mode("overwrite").saveAsTable(full_table)
        logger.info(f"Created table {full_table}")
    except Exception as e:
        logger.error(f"Error loading CSV for table {table}: {e}")
        raise

def create_volume(spark, catalog: str, schema: str, volume: str):
    """Create a Unity Catalog volume if it doesn't exist."""
    try:
        full_volume = f"{catalog}.{schema}.{volume}"
        logger.info(f"Creating volume {full_volume}")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_volume}")
        logger.info(f"Volume {full_volume} ensured.")
    except Exception as e:
        logger.error(f"Error creating volume {full_volume}: {e}")
        raise

def copy_folder_to_volume(workspace_folder: str, catalog: str, schema: str, volume: str, target_subfolder: str = ""):
    """Copy a workspace folder to a UC volume, preserving folder structure."""
    try:
        if target_subfolder:
            volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{target_subfolder}"
        else:
            volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
        
        logger.info(f"Copying {workspace_folder} to {volume_path}")
        
        # Use Python shutil to copy directory tree from workspace to volume
        shutil.copytree(workspace_folder, volume_path, dirs_exist_ok=True)
        
        logger.info(f"Successfully copied to {volume_path}")
        
    except Exception as e:
        logger.error(f"Error copying {workspace_folder} to volume: {e}")
        raise

def enable_cdf(spark, catalog: str, schema: str, table: str):
    """Enable Delta change data feed (CDF) on a table."""
    full_name = f"{catalog}.{schema}.{table}"
    try:
        logger.info(f"Enabling CDF on {full_name}")
        spark.sql(f"""
            ALTER TABLE {full_name}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
        logger.info(f"CDF enabled on {full_name}")
    except Exception as e:
        logger.warning(f"Could not enable CDF on {full_name}: {e}")

def ensure_endpoint(client: VectorSearchClient, endpoint_name: str,
                    timeout: int = 600, poll_interval: int = 10):
    """Create vector search endpoint if needed and wait for it to be ready."""
    try:
        eps = client.list_endpoints().get("endpoints", [])
        names = [ep["name"] for ep in eps]
        if endpoint_name not in names:
            logger.info(f"Creating vector search endpoint '{endpoint_name}'")
            client.create_endpoint(name=endpoint_name, endpoint_type="STANDARD")
        else:
            logger.info(f"Endpoint '{endpoint_name}' already exists")

        start = time.time()
        while True:
            ep_info = client.get_endpoint(endpoint_name)
            state = ep_info.get("endpoint_status", {}).get("state", "UNKNOWN")
            logger.info(f"Endpoint state: {state}")
            if state in ("ONLINE", "PROVISIONED", "READY"):
                logger.info(f"Endpoint '{endpoint_name}' ready")
                break
            if state in ("FAILED", "OFFLINE"):
                raise RuntimeError(f"Endpoint '{endpoint_name}' failed (state={state})")
            if time.time() - start > timeout:
                raise TimeoutError(f"Timed out waiting for endpoint '{endpoint_name}' to be ready")
            time.sleep(poll_interval)
    except Exception as e:
        logger.error(f"Error ensuring endpoint: {e}")
        raise

def index_exists(client: VectorSearchClient, endpoint_name: str, index_name: str) -> bool:
    """Check if a vector search index already exists on the endpoint."""
    try:
        resp = client.list_indexes(name=endpoint_name)
        for idx in resp.get("indexes", []):
            if idx.get("name") == index_name:
                return True
        return False
    except Exception as e:
        logger.warning(f"Could not list indexes (assuming none exist): {e}")
        return False

def create_delta_index(client: VectorSearchClient, endpoint_name: str,
                       source_table: str, index_name: str,
                       pipeline_type: str, primary_key: str, embedding_column: str):
    """Create a delta sync vector search index with automatic CDF retry if needed."""
    if index_exists(client, endpoint_name, index_name):
        logger.info(f"Index '{index_name}' already exists, skipping")
        return

    try:
        logger.info(f"Creating index '{index_name}' on {source_table}")
        client.create_delta_sync_index(
            endpoint_name=endpoint_name,
            index_name=index_name,
            source_table_name=source_table,
            pipeline_type=pipeline_type,
            primary_key=primary_key,
            embedding_source_column=embedding_column,
            embedding_model_endpoint_name="databricks-gte-large-en"
        )
        logger.info(f"Index '{index_name}' created successfully")
    except Exception as ex:
        msg = str(ex)
        if "does not have change data feed enabled" in msg:
            logger.info(f"CDF not enabled on {source_table}, attempting to enable and retry")
            parts = source_table.split(".")
            if len(parts) == 3:
                cat, sch, tbl = parts
                try:
                    enable_cdf(spark, cat, sch, tbl)
                    logger.info(f"Retrying index creation for '{index_name}'")
                    client.create_delta_sync_index(
                        endpoint_name=endpoint_name,
                        index_name=index_name,
                        source_table_name=source_table,
                        pipeline_type=pipeline_type,
                        primary_key=primary_key,
                        embedding_source_column=embedding_column,
                        embedding_model_endpoint_name="databricks-gte-large-en"
                    )
                    logger.info(f"Index '{index_name}' created after retry")
                    return
                except Exception as e2:
                    logger.error(f"Retry failed for index '{index_name}': {e2}")
            else:
                logger.error(f"Could not parse table name '{source_table}' for CDF retry")
        logger.error(f"Failed to create index '{index_name}': {msg}")
        raise

def main(spark):
    """Main setup function for AI Pioneer lab."""
    catalog = "ai_pioneer"
    schema = "lab_data"
    endpoint_name = "ai_pioneer_vs_endpoint"
    
    # Determine data folder path from notebook location
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    logger.info(f"Notebook path: {notebook_path}")
    
    notebook_dir = os.path.dirname(notebook_path)
    data_folder = f"file:/Workspace{notebook_dir}/Data"
    logger.info(f"Using data folder: {data_folder}")
    
    csv_files = {
        "billing": f"{data_folder}/billing.csv",
        "customers": f"{data_folder}/customers.csv",
        "knowledge_base": f"{data_folder}/knowledge_base.csv",
        "support_tickets": f"{data_folder}/support_tickets.csv",
        "cust_service_data": f"{data_folder}/cust_service_data.csv",
        "policies": f"{data_folder}/policies.csv",
        "product_docs": f"{data_folder}/product_docs.csv"
    }

    # Step 1: Create catalog and schema
    create_catalog_and_schema(spark, catalog, schema)

    # Step 2: Load CSV files into Delta tables
    for tbl, file_path in csv_files.items():
        load_csv_to_table(spark, tbl, file_path, catalog, schema)

    # Step 3: Create UC volumes and copy unstructured data folders
    workspace_data_folder = data_folder.replace("file:", "")
    
    # Create volume for call transcripts
    volume_calls = "customer_call_transcripts"
    create_volume(spark, catalog, schema, volume_calls)
    logger.info("Copying call transcripts to volume")
    copy_folder_to_volume(
        workspace_folder=f"{workspace_data_folder}/call_transcripts",
        catalog=catalog,
        schema=schema,
        volume=volume_calls,
        target_subfolder=""
    )
    
    # Create volume for customer emails
    volume_emails = "customer_emails"
    create_volume(spark, catalog, schema, volume_emails)
    logger.info("Copying email correspondence to volume")
    copy_folder_to_volume(
        workspace_folder=f"{workspace_data_folder}/email_correspondence",
        catalog=catalog,
        schema=schema,
        volume=volume_emails,
        target_subfolder=""
    )
    
    # Create volume for marketing materials
    volume_marketing = "marketing"
    create_volume(spark, catalog, schema, volume_marketing)
    logger.info("Copying marketing materials to volume")
    copy_folder_to_volume(
        workspace_folder=f"{workspace_data_folder}/marketing_materials",
        catalog=catalog,
        schema=schema,
        volume=volume_marketing,
        target_subfolder=""
    )
    
    logger.info(f"Unstructured data copied to volumes: {volume_calls}, {volume_emails}, {volume_marketing}")

    # Step 4: Create vector search endpoint
    vs_client = VectorSearchClient()
    ensure_endpoint(vs_client, endpoint_name)

    # Step 5: Enable CDF on source tables
    try:
        enable_cdf(spark, catalog, schema, "knowledge_base")
        enable_cdf(spark, catalog, schema, "support_tickets")
    except Exception as e:
        logger.warning(f"Failed enabling CDF for one or more tables: {e}")

    # Step 6: Create vector search indexes
    kb_table = f"{catalog}.{schema}.knowledge_base"
    st_table = f"{catalog}.{schema}.support_tickets"
    kb_index = f"{catalog}.{schema}.knowledge_base_index"
    st_index = f"{catalog}.{schema}.support_tickets_index"
    
    kb_pk = "kb_id"
    kb_text = "formatted_content"
    st_pk = "ticket_id"
    st_text = "formatted_content"

    create_delta_index(vs_client, endpoint_name, kb_table, kb_index, "TRIGGERED", kb_pk, kb_text)
    create_delta_index(vs_client, endpoint_name, st_table, st_index, "TRIGGERED", st_pk, st_text)

    logger.info("Setup complete.")

if __name__ == "__main__":
    try:
        main(spark)
    except NameError:
        logger.error("This script must run in Databricks environment where `spark` is defined.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Setup script failed: {e}")
        sys.exit(1)

# COMMAND ----------


