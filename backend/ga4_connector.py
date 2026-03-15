"""
GA4 BigQuery Connector (FastAPI)
---------------------------------
Handles discovery, connection testing, and management of GA4 BigQuery datasets.
Config stored in BigQuery table `app_config.data_sources`.
"""

import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

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

# In-memory config cache
_config_cache: dict | None = None


def _get_bq_client() -> bigquery.Client | None:
    """Get a BigQuery client for config operations."""
    try:
        from . import bq_data
        if bq_data._BQ_CLIENT is not None:
            return bq_data._BQ_CLIENT
    except Exception:
        pass

    try:
        creds_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
        if creds_json:
            from google.oauth2.service_account import Credentials
            info = json.loads(creds_json)
            creds = Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            return bigquery.Client(credentials=creds, project=DEFAULT_PROJECT)
        return bigquery.Client(project=DEFAULT_PROJECT)
    except Exception:
        return None


def _load_config_from_bq() -> dict | None:
    """Load config from BigQuery table."""
    client = _get_bq_client()
    if not client:
        return None
    try:
        sql = f"SELECT config_json FROM `{DEFAULT_PROJECT}.{BQ_CONFIG_DATASET}.{BQ_CONFIG_TABLE}` ORDER BY updated_at DESC LIMIT 1"
        rows = list(client.query(sql, location="EU").result())
        if rows:
            return json.loads(rows[0]["config_json"])
    except Exception as e:
        logger.warning(f"Could not load config from BQ: {e}")
    return None


def _save_config_to_bq(config: dict) -> bool:
    """Save config to BigQuery using parameterized DML."""
    client = _get_bq_client()
    if not client:
        return False
    try:
        table_id = f"{DEFAULT_PROJECT}.{BQ_CONFIG_DATASET}.{BQ_CONFIG_TABLE}"
        config_json = json.dumps(config, ensure_ascii=False)

        client.query(f"DELETE FROM `{table_id}` WHERE TRUE", location="EU").result()

        sql = f"INSERT INTO `{table_id}` (config_json, updated_at) VALUES (@config_json, CURRENT_TIMESTAMP())"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("config_json", "STRING", config_json),
            ]
        )
        client.query(sql, job_config=job_config, location="EU").result()
        return True
    except Exception as e:
        logger.error(f"Could not save config to BQ: {e}")
        return False


def _load_config() -> dict:
    """Load config: memory cache → BigQuery → local file."""
    global _config_cache
    if _config_cache is not None:
        return _config_cache

    bq_config = _load_config_from_bq()
    if bq_config and bq_config.get("sources"):
        _config_cache = bq_config
        return bq_config

    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
            _config_cache = config
            return config

    return {"project": DEFAULT_PROJECT, "sources": []}


def _save_config(config: dict):
    """Save config to BQ and update memory cache."""
    global _config_cache
    _save_config_to_bq(config)
    _config_cache = config
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def invalidate_cache():
    """Force reload config from BQ on next access."""
    global _config_cache
    _config_cache = None


def get_config() -> dict:
    return _load_config()


def get_project_id() -> str:
    return _load_config().get("project", DEFAULT_PROJECT)


def set_project_id(project_id: str):
    config = _load_config()
    config["project"] = project_id
    _save_config(config)


def get_connected_sources() -> list[dict]:
    return _load_config().get("sources", [])


def add_source(dataset_id: str, label: str, business_area: str = "", company: str = "",
               vat_status: str = "ex_vat", currency: str = "SEK",
               gads_customer_id: str = "") -> dict:
    config = _load_config()
    property_id = dataset_id.replace("analytics_", "")

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
    config = _load_config()
    original_len = len(config["sources"])
    config["sources"] = [s for s in config["sources"] if s["dataset_id"] != dataset_id]
    _save_config(config)
    return len(config["sources"]) < original_len


def update_source(dataset_id: str, **fields) -> bool:
    config = _load_config()
    for s in config["sources"]:
        if s["dataset_id"] == dataset_id:
            s.update(fields)
            _save_config(config)
            return True
    return False


def has_credentials() -> bool:
    creds_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if creds_json:
        return True
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if cred_path and os.path.isfile(cred_path):
        return True
    return False


def get_credentials_info() -> Optional[dict]:
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not cred_path or not os.path.isfile(cred_path):
        creds_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
        if creds_json:
            info = json.loads(creds_json)
            return {
                "source": "GCP_SERVICE_ACCOUNT_JSON env var",
                "project_id": info.get("project_id", "unknown"),
                "client_email": info.get("client_email", "unknown"),
            }
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
    client = _get_bq_client()
    if not client:
        return []
    try:
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
    client = _get_bq_client()
    if not client:
        return {"error": "No BigQuery client available."}
    try:
        project = get_project_id()
        tables = list(client.list_tables(f"{project}.{dataset_id}"))
        event_tables = [t.table_id for t in tables if t.table_id.startswith("events_")]
        if not event_tables:
            return {"error": f"No events_* tables found in {dataset_id}"}
        event_tables.sort()
        return {
            "success": True,
            "table_count": len(event_tables),
            "first_date": event_tables[0].replace("events_", ""),
            "last_date": event_tables[-1].replace("events_", ""),
            "dataset_id": dataset_id,
        }
    except Exception as e:
        return {"error": str(e)}
