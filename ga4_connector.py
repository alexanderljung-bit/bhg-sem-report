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
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import streamlit as st
    _HAS_STREAMLIT = True
except ImportError:
    _HAS_STREAMLIT = False

CONFIG_PATH = Path(__file__).parent / "data_sources.json"
DEFAULT_PROJECT = "bygghemma-bigdata"

BUSINESS_AREAS = ["Home Improvement", "Value Home", "Premium Living"]

COMPANIES = [
    "Bygghemma Nordic",
    "Home Furnishing Nordic",
    "NordicNest Group",
    "Hemfint Group",
    "Hafa Bathroom Group",
    "HYMA Group",
]


def _load_config() -> dict:
    """Load the data sources config from disk or Streamlit secrets."""
    # Try Streamlit secrets first (Streamlit Cloud deployment)
    if _HAS_STREAMLIT:
        try:
            if "data_sources" in st.secrets:
                return json.loads(st.secrets["data_sources"])
        except Exception:
            pass
    # Fall back to local file
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"project": DEFAULT_PROJECT, "sources": []}


def _save_config(config: dict):
    """Save the data sources config to disk."""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


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


# Ensure config file exists on import
if not CONFIG_PATH.exists():
    _save_config({"project": DEFAULT_PROJECT, "sources": []})
