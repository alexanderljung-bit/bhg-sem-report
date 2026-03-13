"""
GA4 BigQuery Connector
-----------------------
Handles discovery, connection testing, and management of GA4 BigQuery datasets.
Project: Bygghemma - BigData
Dataset pattern: analytics_[9-digit GA4 property ID]
Table pattern: events_YYYYMMDD (standard GA4 raw export)
"""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    import streamlit as st
    _HAS_STREAMLIT = True
except ImportError:
    _HAS_STREAMLIT = False

CONFIG_PATH = Path(__file__).parent / "data_sources.json"
DEFAULT_PROJECT = "bygghemma-bigdata"
BQ_CONFIG_DATASET = "app_config"
BQ_CONFIG_TABLE = "data_sources"

BUSINESS_AREAS = ["Home Improvement", "Value Home", "Premium Living"]

COMPANIES = [
    "Bygghemma Nordic",
    "Home Furnishing Nordic",
    "NordicNest Group",
    "Hemfint Group",
    "Hafa Bathroom Group",
    "HYMA Group",
]


def _get_bq_client():
    """Get a BigQuery client for config operations.
    Reuses the cached client from bq_data if available (proven on Cloud).
    """
    # 1. Try reusing the already-working cached client from bq_data
    try:
        from bq_data import _BQ_CLIENT
        if _BQ_CLIENT is not None:
            return _BQ_CLIENT
    except Exception:
        pass

    # 2. Build own client from Streamlit secrets
    try:
        from google.cloud import bigquery
        if _HAS_STREAMLIT and "gcp_service_account" in st.secrets:
            from google.oauth2.service_account import Credentials
            creds = Credentials.from_service_account_info(
                dict(st.secrets["gcp_service_account"]),
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            return bigquery.Client(credentials=creds, project=DEFAULT_PROJECT)
        # Local dev
        return bigquery.Client(project=DEFAULT_PROJECT)
    except Exception:
        return None


def _load_config_from_bq() -> dict | None:
    """Try to load config from BigQuery table."""
    client = _get_bq_client()
    if not client:
        return None
    try:
        sql = f"SELECT config_json FROM `{DEFAULT_PROJECT}.{BQ_CONFIG_DATASET}.{BQ_CONFIG_TABLE}` ORDER BY updated_at DESC LIMIT 1"
        rows = list(client.query(sql, location="EU").result())
        if rows:
            return json.loads(rows[0]["config_json"])
    except Exception:
        pass
    return None


def _ensure_bq_table(client):
    """Create config table if it doesn't exist (dataset must exist already)."""
    from google.cloud import bigquery
    table_id = f"{DEFAULT_PROJECT}.{BQ_CONFIG_DATASET}.{BQ_CONFIG_TABLE}"
    schema = [
        bigquery.SchemaField("config_json", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    try:
        client.get_table(table)
    except Exception:
        try:
            client.create_table(table, exists_ok=True)
        except Exception:
            pass  # Table likely already exists


def _save_config_to_bq(config: dict) -> bool:
    """Save config to BigQuery using parameterized DML."""
    client = _get_bq_client()
    if not client:
        return False
    try:
        from google.cloud import bigquery
        _ensure_bq_table(client)
        table_id = f"{DEFAULT_PROJECT}.{BQ_CONFIG_DATASET}.{BQ_CONFIG_TABLE}"
        config_json = json.dumps(config, ensure_ascii=False)

        # Step 1: Delete old rows
        client.query(f"DELETE FROM `{table_id}` WHERE TRUE", location="EU").result()

        # Step 2: Insert new row with parameterized query (safe for any JSON)
        sql = f"INSERT INTO `{table_id}` (config_json, updated_at) VALUES (@config_json, CURRENT_TIMESTAMP())"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("config_json", "STRING", config_json),
            ]
        )
        client.query(sql, job_config=job_config, location="EU").result()
        return True
    except Exception as e:
        if _HAS_STREAMLIT:
            st.warning(f"⚠️ Kunde inte spara till BigQuery: {e}")
        return False


def _load_config() -> dict:
    """Load config with session caching: session_state → BigQuery → secrets → file."""
    # 0. Return cached config if available (avoids BQ query on every rerun)
    if _HAS_STREAMLIT and "_config_cache" in st.session_state:
        return st.session_state["_config_cache"]

    # 1. Try BigQuery (persistent cloud storage)
    bq_config = _load_config_from_bq()
    if bq_config and bq_config.get("sources"):
        if _HAS_STREAMLIT:
            st.session_state["_config_cache"] = bq_config
        return bq_config

    # 2. Try Streamlit secrets (initial seed)
    if _HAS_STREAMLIT:
        try:
            if "data_sources" in st.secrets:
                config = json.loads(st.secrets["data_sources"])
                _save_config_to_bq(config)
                st.session_state["_config_cache"] = config
                return config
        except Exception:
            pass

    # 3. Fall back to local file
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
            if _HAS_STREAMLIT:
                st.session_state["_config_cache"] = config
            return config
    return {"project": DEFAULT_PROJECT, "sources": []}


def _save_config(config: dict):
    """Save config to BQ + local file and update session cache."""
    _save_config_to_bq(config)
    # Update session cache immediately
    if _HAS_STREAMLIT:
        st.session_state["_config_cache"] = config
    # Also save locally if possible (for local dev)
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def get_config() -> dict:
    """Return the current config."""
    return _load_config()


def get_project_id() -> str:
    """Return the configured GCP project ID."""
    return _load_config().get("project", DEFAULT_PROJECT)


def set_project_id(project_id: str):
    """Update the GCP project ID."""
    config = _load_config()
    config["project"] = project_id
    _save_config(config)


def get_connected_sources() -> list[dict]:
    """Return list of connected data sources."""
    return _load_config().get("sources", [])


def add_source(dataset_id: str, label: str, business_area: str = "", company: str = "",
               vat_status: str = "ex_vat", currency: str = "SEK",
               gads_customer_id: str = "") -> dict:
    """Add a new data source to the config."""
    config = _load_config()

    # Extract property ID from dataset name
    property_id = dataset_id.replace("analytics_", "")

    # Check for duplicates
    for s in config["sources"]:
        if s["dataset_id"] == dataset_id:
            return {"error": f"Dataset {dataset_id} is already connected."}

    source = {
        "dataset_id": dataset_id,
        "label": label,
        "ga4_property_id": property_id,
        "business_area": business_area,
        "company": company,
        "gads_customer_id": gads_customer_id,
        "vat_status": vat_status,
        "currency": currency,
        "added_at": datetime.now().isoformat(),
        "status": "connected",
    }
    config["sources"].append(source)
    _save_config(config)
    return source


def remove_source(dataset_id: str) -> bool:
    """Remove a data source from the config."""
    config = _load_config()
    original_len = len(config["sources"])
    config["sources"] = [s for s in config["sources"] if s["dataset_id"] != dataset_id]
    _save_config(config)
    return len(config["sources"]) < original_len


def update_source(dataset_id: str, **fields) -> bool:
    """Update fields on an existing source (e.g. vat_status, currency)."""
    config = _load_config()
    for s in config["sources"]:
        if s["dataset_id"] == dataset_id:
            s.update(fields)
            _save_config(config)
            return True
    return False


def has_credentials() -> bool:
    """Check if GCP credentials are configured."""
    # 1. Streamlit Cloud secrets (gcp_service_account as dict)
    if _HAS_STREAMLIT:
        try:
            if "gcp_service_account" in st.secrets:
                cred_dict = dict(st.secrets["gcp_service_account"])
                # Write to a temp file so google-cloud-bigquery can use it
                tmp = tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False, encoding="utf-8"
                )
                json.dump(cred_dict, tmp)
                tmp.close()
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name
                return True
        except Exception:
            pass

    # 2. Environment variable pointing to a file
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if cred_path and os.path.isfile(cred_path):
        return True

    # 3. Path stored in local config
    config = _load_config()
    local_cred = config.get("credentials_path", "")
    if local_cred and os.path.isfile(local_cred):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = local_cred
        return True

    return False


def set_credentials_path(path: str) -> dict:
    """Set the path to the GCP service account JSON file."""
    if not os.path.isfile(path):
        return {"error": f"File not found: {path}"}

    # Validate it's valid JSON
    try:
        with open(path, "r", encoding="utf-8") as f:
            cred_data = json.load(f)
        if "type" not in cred_data:
            return {"error": "Invalid service account JSON (missing 'type' field)."}
    except json.JSONDecodeError:
        return {"error": "File is not valid JSON."}

    # Save to config and set env var
    config = _load_config()
    config["credentials_path"] = path
    _save_config(config)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path

    return {
        "success": True,
        "project_id": cred_data.get("project_id", "unknown"),
        "client_email": cred_data.get("client_email", "unknown"),
    }


def get_credentials_info() -> Optional[dict]:
    """Return info about the configured credentials."""
    config = _load_config()
    cred_path = config.get("credentials_path") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not cred_path or not os.path.isfile(cred_path):
        return None
    try:
        with open(cred_path, "r", encoding="utf-8") as f:
            cred_data = json.load(f)
        return {
            "path": cred_path,
            "project_id": cred_data.get("project_id", "unknown"),
            "client_email": cred_data.get("client_email", "unknown"),
            "type": cred_data.get("type", "unknown"),
        }
    except Exception:
        return {"path": cred_path, "error": "Could not read credentials file."}


def discover_datasets() -> list[dict]:
    """Discover all analytics_* datasets in the configured BQ project."""
    if not has_credentials():
        return []

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=get_project_id())
        datasets = []
        for ds in client.list_datasets():
            if ds.dataset_id.startswith("analytics_"):
                property_id = ds.dataset_id.replace("analytics_", "")
                datasets.append({
                    "dataset_id": ds.dataset_id,
                    "ga4_property_id": property_id,
                    "full_id": f"{get_project_id()}.{ds.dataset_id}",
                })
        return datasets
    except Exception as e:
        return [{"error": str(e)}]


def test_connection(dataset_id: str) -> dict:
    """Test a dataset connection by checking for events tables."""
    if not has_credentials():
        return {"error": "No GCP credentials configured."}

    try:
        from google.cloud import bigquery
        project = get_project_id()
        client = bigquery.Client(project=project)

        # List tables matching events_*
        tables = list(client.list_tables(f"{project}.{dataset_id}"))
        event_tables = [t.table_id for t in tables if t.table_id.startswith("events_")]

        if not event_tables:
            return {"error": f"No events_* tables found in {dataset_id}"}

        event_tables.sort()
        first_date = event_tables[0].replace("events_", "")
        last_date = event_tables[-1].replace("events_", "")

        return {
            "success": True,
            "table_count": len(event_tables),
            "first_date": first_date,
            "last_date": last_date,
            "dataset_id": dataset_id,
        }
    except Exception as e:
        return {"error": str(e)}


# Ensure config file exists on import (skip on read-only filesystem)
try:
    if not CONFIG_PATH.exists():
        _save_config({"project": DEFAULT_PROJECT, "sources": []})
except Exception:
    pass
