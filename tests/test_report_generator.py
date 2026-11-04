import datetime
import io
import json

from monitor import report_generator
from monitor.report_generator import _build_layer_summary
from monitor.report_generator import _as_utc
from monitor.report_generator import fetch_s3_manifest
from monitor.report_generator import _format_utc_timestamp


def test_report_timestamp_is_converted_to_utc():
    timestamp = datetime.datetime(
        2026,
        11,
        3,
        19,
        29,
        2,
        tzinfo=datetime.timezone(datetime.timedelta(hours=-3)),
    )

    assert _format_utc_timestamp(timestamp) == "2026-11-03 22:29:02 UTC"


def test_manifest_lookup_uses_events_layer_and_utc_date(monkeypatch):
    class FakePaginator:
        def paginate(self, **kwargs):
            assert kwargs["Prefix"] == "auditoria/events/"
            return iter([])

    class FakeS3:
        def get_object(self, Bucket, Key):
            assert Key == (
                "auditoria/events/ingestion_date=2026-11-04/"
                "run_id=jr-event/manifest.json"
            )
            body = json.dumps({"status": "SUCCEEDED"}).encode("utf-8")
            return {"Body": io.BytesIO(body)}

        def get_paginator(self, operation):
            return FakePaginator()

    monkeypatch.setattr(
        report_generator.boto3, "client", lambda *args, **kwargs: FakeS3()
    )
    started_on = datetime.datetime(
        2026,
        11,
        3,
        22,
        16,
        tzinfo=datetime.timezone(datetime.timedelta(hours=-3)),
    )

    assert _as_utc(started_on).strftime("%Y-%m-%d") == "2026-11-04"
    manifest, key = fetch_s3_manifest("04_events_processor", "jr-event", "2026-11-04")

    assert manifest == {"status": "SUCCEEDED"}
    assert key == (
        "auditoria/events/ingestion_date=2026-11-04/" "run_id=jr-event/manifest.json"
    )


def test_manifest_lookup_uses_gold_path_from_job_logs(monkeypatch):
    class FakePaginator:
        def paginate(self, **kwargs):
            raise AssertionError(
                "Paginator should not be needed when logs identify the manifest"
            )

    expected_key = (
        "auditoria/gold/ingestion_date=2026-11-04/" "run_id=gld-custom-id/manifest.json"
    )

    class FakeS3:
        def get_object(self, Bucket, Key):
            assert Key == expected_key
            body = json.dumps({"status": "SUCCEEDED"}).encode("utf-8")
            return {"Body": io.BytesIO(body)}

        def get_paginator(self, operation):
            return FakePaginator()

    monkeypatch.setattr(
        report_generator.boto3, "client", lambda *args, **kwargs: FakeS3()
    )
    manifest, key = fetch_s3_manifest(
        "03_gold_aggregation",
        "jr-glue-id",
        "2026-11-03",
        f"Manifesto gravado: s3://{report_generator.FULL_BUCKET_NAME}/{expected_key}",
    )

    assert manifest == {"status": "SUCCEEDED"}
    assert key == expected_key


def test_event_summary_reports_manifest_metrics():
    title, summary = _build_layer_summary(
        "events",
        {
            "status": "SUCCEEDED",
            "total_events_processed": 48,
            "alunos_events_count": 35,
            "metas_events_count": 13,
            "metas_events_by_domain": {
                "municipio": 10,
                "uf": 2,
                "brasil": 1,
            },
            "latency_metrics_seconds": {
                "avg_sec": 156.629,
                "min_sec": 156.629,
                "max_sec": 156.629,
            },
            "streaming_engine": "Spark Structured Streaming (AvailableNow)",
        },
    )

    assert "Processamento de Eventos" in title
    assert "**48**" in summary
    assert "**35**" in summary
    assert "**13**" in summary
    assert "**10 / 2 / 1**" in summary
    assert "**156.629 / 156.629 / 156.629 s**" in summary


def test_silver_short_circuit_does_not_report_zero_metrics_or_failed_gate():
    title, summary = _build_layer_summary(
        "silver",
        {
            "status": "SKIPPED (Already Up-to-Date)",
            "gate_status": "APPROVED",
            "s3_path": "s3://example/silver/alunos_curado/",
        },
    )

    assert "Camada Silver" in title
    assert "0.00%" not in summary
    assert "Abaixo do limite" not in summary
    assert "Não reexecutado (Silver existente preservada)" in summary
    assert summary.count("N/A (sem processamento nesta execução)") == 3


def test_silver_success_manifest_metrics_are_reported():
    _, summary = _build_layer_summary(
        "silver",
        {
            "status": "SUCCEEDED",
            "input_rows": 3_867_999,
            "output_rows": 3_868_434,
            "match_ratio": 99.5,
            "gate_status": "APPROVED",
            "s3_path": "s3://example/silver/alunos_curado/",
        },
    )

    assert "**3,867,999**" in summary
    assert "**3,868,434**" in summary
    assert "**99.50%**" in summary
    assert "Dentro do limite" in summary


def test_silver_failed_gate_is_reported_as_failed():
    _, summary = _build_layer_summary(
        "silver",
        {
            "status": "FAILED",
            "quality_gate_passed": False,
            "join_match_ratio_percent": 80.0,
        },
    )

    assert "**80.00%**" in summary
    assert "Abaixo do limite" in summary


def test_silver_summary_describes_delta_format_without_date_partition():
    _, summary = _build_layer_summary(
        "silver",
        {
            "status": "SUCCEEDED",
            "s3_path": "s3://example/silver/alunos_curado/",
        },
    )

    assert "Formato Delta Lake" in summary
    assert "sem partição por data" in summary
    assert "particionado por data" not in summary


def test_gold_short_circuit_does_not_report_unmeasured_metrics_as_zero():
    _, summary = _build_layer_summary(
        "gold",
        {
            "status": "SKIPPED (Already Up-to-Date)",
            "gold_s3_prefixes": {
                "indicadores": "s3://example/gold/indicadores/",
            },
        },
    )

    assert summary.count("N/A (sem processamento nesta execução)") == 4
    assert "Não reexecutada (conjuntos existentes mantidos)" in summary
    assert "0.000%" not in summary
    assert "**0**" not in summary
    assert "Destinos S3 Gold" in summary
    assert "Local Sync" not in summary


def test_gold_success_manifest_metrics_are_reported():
    _, summary = _build_layer_summary(
        "gold",
        {
            "status": "SUCCEEDED",
            "input_rows_silver": 1000,
            "output_rows_indicadores": 10,
            "output_rows_metas": 9,
            "output_rows_features": 8,
            "cross_validation_passed": True,
            "cross_validation_diff_percent": 0.0363,
            "gold_s3_prefixes": {
                "indicadores": "s3://example/gold/indicadores/",
            },
        },
    )

    assert "**1,000**" in summary
    assert "**10**" in summary
    assert "**9**" in summary
    assert "**8**" in summary
    assert "Dentro da tolerância (0.036%)" in summary


def test_gold_summary_reports_indicator_and_meta_counts_by_level():
    _, summary = _build_layer_summary(
        "gold",
        {
            "status": "SUCCEEDED",
            "output_rows_indicadores": 30,
            "output_rows_indicadores_by_level": {
                "municipio": 10,
                "uf": 15,
                "brasil": 5,
            },
            "output_rows_metas": 6,
            "output_rows_metas_by_level": {
                "municipio": 3,
                "uf": 2,
                "brasil": 1,
            },
            "output_rows_features_by_level": {
                "municipio": 3,
                "uf": 2,
                "brasil": 1,
            },
        },
    )

    assert "municipio: 10" in summary
    assert "uf: 15" in summary
    assert "brasil: 5" in summary
    assert "municipio: 3" in summary
    assert "uf: 2" in summary
    assert "brasil: 1" in summary
    assert summary.count("municipio: 3") == 2
    assert "redes compatíveis conforme dicionário oficial" in summary


def test_bronze_summary_does_not_report_metadata_as_s3_row_count():
    _, summary = _build_layer_summary(
        "bronze",
        {
            "tables_metrics": [
                {
                    "entity": "alunos",
                    "bq_rows": 100,
                    "s3_rows": None,
                    "status": "SUCCEEDED",
                    "partitions": 2,
                    "size_mb": 1.0,
                }
            ]
        },
    )

    assert "| `alunos` | 100 | N/A | `SUCCEEDED` |" in summary
