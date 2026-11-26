"""Cost measurement records with explicit coverage and uncertainty."""

import os
from datetime import datetime, timezone

COST_PER_DPU_HOUR_USD = float(os.getenv("GLUE_COST_PER_DPU_HOUR_USD", "0.44"))
COST_PRICE_SOURCE = os.getenv(
    "GLUE_COST_PRICE_SOURCE",
    "Project estimate assumption; official price source and effective date unverified",
)


def _iso_or_none(value):
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def build_finops_measurements(
    job_name,
    run_id,
    execution_time_seconds,
    dpu_seconds,
    period_start,
    period_end,
):
    """Build measured/estimated Glue and explicitly unavailable GCP records."""
    collected_at = datetime.now(timezone.utc).isoformat()
    if dpu_seconds is not None:
        dpu_hours = float(dpu_seconds) / 3600.0
        quantity_status = "measured"
        quantity_source = "AWS Glue GetJobRun.DPUSeconds"
    elif execution_time_seconds is not None:
        dpu_hours = float(execution_time_seconds) * 2.0 / 3600.0
        quantity_status = "estimated"
        quantity_source = "AWS Glue ExecutionTime x assumed 2 G.1X DPUs"
    else:
        dpu_hours = None
        quantity_status = "unavailable"
        quantity_source = "AWS Glue run metrics unavailable"

    glue_cost = (
        dpu_hours * COST_PER_DPU_HOUR_USD if dpu_hours is not None else None
    )
    common = {
        "run_id": run_id,
        "period_start": _iso_or_none(period_start),
        "period_end": _iso_or_none(period_end),
        "collected_at": collected_at,
        "currency": "USD",
    }
    return [
        {
            **common,
            "provider": "AWS",
            "service": "AWS Glue",
            "metric": "dpu_hours",
            "quantity": dpu_hours,
            "unit": "DPU-h",
            "cost_amount": glue_cost,
            "measurement_status": (
                "estimated" if quantity_status != "unavailable" else "unavailable"
            ),
            "quantity_status": quantity_status,
            "source_reference": quantity_source,
            "price_source": COST_PRICE_SOURCE,
            "price_unit_amount": COST_PER_DPU_HOUR_USD,
            "bytes_processed": None,
            "bytes_billed": None,
            "bytes_read": None,
            "egress_bytes": None,
        },
        {
            **common,
            "provider": "GCP",
            "service": "BigQuery",
            "metric": "bytes_processed_billed_and_read",
            "quantity": None,
            "unit": "bytes",
            "cost_amount": None,
            "measurement_status": "unavailable",
            "quantity_status": "unavailable",
            "source_reference": (
                "Connector/Storage API telemetry and Billing Export not validated "
                "(F0.7/F0.8)"
            ),
            "price_source": None,
            "price_unit_amount": None,
            "bytes_processed": None,
            "bytes_billed": None,
            "bytes_read": None,
            "egress_bytes": None,
        },
        {
            **common,
            "provider": "GCP",
            "service": "Network egress to AWS",
            "metric": "egress_bytes",
            "quantity": None,
            "unit": "bytes",
            "cost_amount": None,
            "measurement_status": "unavailable",
            "quantity_status": "unavailable",
            "source_reference": (
                "GCP Cloud Billing Export and egress attribution not validated "
                "(F0.8)"
            ),
            "price_source": None,
            "price_unit_amount": None,
            "bytes_processed": None,
            "bytes_billed": None,
            "bytes_read": None,
            "egress_bytes": None,
        },
    ]
