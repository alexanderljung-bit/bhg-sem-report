"""Data Sources CRUD endpoints."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from .. import ga4_connector

router = APIRouter(prefix="/sources", tags=["sources"])


class AddSourceRequest(BaseModel):
    dataset_id: str
    label: str
    business_area: str = ""
    company: str = ""
    vat_status: str = "ex_vat"
    currency: str = "SEK"
    gads_customer_id: str = ""


class UpdateSourceRequest(BaseModel):
    label: Optional[str] = None
    business_area: Optional[str] = None
    company: Optional[str] = None
    vat_status: Optional[str] = None
    currency: Optional[str] = None
    gads_customer_id: Optional[str] = None


class TestConnectionRequest(BaseModel):
    dataset_id: str


@router.get("")
def list_sources():
    """List all connected data sources."""
    return {
        "sources": ga4_connector.get_connected_sources(),
        "business_areas": ga4_connector.BUSINESS_AREAS,
        "companies": ga4_connector.COMPANIES,
    }


@router.post("")
def add_source(req: AddSourceRequest):
    """Add a new data source."""
    result = ga4_connector.add_source(
        dataset_id=req.dataset_id,
        label=req.label,
        business_area=req.business_area,
        company=req.company,
        vat_status=req.vat_status,
        currency=req.currency,
        gads_customer_id=req.gads_customer_id,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.put("/{dataset_id}")
def update_source(dataset_id: str, req: UpdateSourceRequest):
    """Update a data source."""
    fields = {k: v for k, v in req.model_dump().items() if v is not None}
    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")
    ok = ga4_connector.update_source(dataset_id, **fields)
    if not ok:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"success": True}


@router.delete("/{dataset_id}")
def delete_source(dataset_id: str):
    """Remove a data source."""
    ok = ga4_connector.remove_source(dataset_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"success": True}


@router.post("/test")
def test_connection(req: TestConnectionRequest):
    """Test a dataset connection."""
    result = ga4_connector.test_connection(req.dataset_id)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("/discover")
def discover_datasets():
    """Auto-discover analytics_* datasets."""
    return {"datasets": ga4_connector.discover_datasets()}
