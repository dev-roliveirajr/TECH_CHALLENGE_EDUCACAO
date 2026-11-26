import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from common.manifest_lib import (
    get_latest_bronze_manifest,
    get_latest_manifest,
    get_latest_approved_gold_manifest,
    get_latest_approved_silver_manifest,
    validate_bronze_manifest,
)

ENTITIES = ["alunos", "municipio", "uf"]


def _manifest(status="SUCCEEDED", entity_status="SUCCEEDED"):
    return {
        "status": status,
        "tables_metrics": [
            {
                "entity": entity,
                "status": entity_status,
                "s3_prefix": f"s3://data-bucket/bronze/{entity}/ref_ano=2026/",
            }
            for entity in ENTITIES
        ],
    }


def test_get_latest_bronze_manifest_paginates_and_reads_latest():
    first_page = {
        "Contents": [
            {
                "Key": "auditoria/bronze/ingestion_date=2026-01-01/run_id=old/manifest.json",
                "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc),
            }
        ]
    }
    second_page = {
        "Contents": [
            {
                "Key": "auditoria/bronze/ingestion_date=2026-01-02/run_id=new/manifest.json",
                "LastModified": datetime(2026, 1, 2, tzinfo=timezone.utc),
            }
        ]
    }
    paginator = MagicMock()
    paginator.paginate.return_value = [first_page, second_page]
    client = MagicMock(get_paginator=MagicMock(return_value=paginator))
    client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=b'{"status":"SUCCEEDED"}'))
    }

    manifest = get_latest_bronze_manifest(client, "data-bucket")

    assert manifest == {"status": "SUCCEEDED"}
    client.get_object.assert_called_once_with(
        Bucket="data-bucket",
        Key="auditoria/bronze/ingestion_date=2026-01-02/run_id=new/manifest.json",
    )


def test_get_latest_manifest_paginates_for_requested_layer():
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "auditoria/silver/ingestion_date=2026-01-03/run_id=run/manifest.json",
                    "LastModified": datetime(2026, 1, 3, tzinfo=timezone.utc),
                }
            ]
        }
    ]
    client = MagicMock(get_paginator=MagicMock(return_value=paginator))
    client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=b'{"status":"SUCCEEDED"}'))
    }

    manifest = get_latest_manifest(
        client, "data-bucket", "auditoria/silver/", "Camada Silver"
    )

    assert manifest == {"status": "SUCCEEDED"}
    paginator.paginate.assert_called_once_with(
        Bucket="data-bucket",
        Prefix="auditoria/silver/",
    )


def test_get_latest_bronze_manifest_rejects_missing_or_unsuccessful_manifest():
    paginator = MagicMock()
    paginator.paginate.return_value = []
    client = MagicMock(get_paginator=MagicMock(return_value=paginator))
    with pytest.raises(FileNotFoundError, match="Não há manifesto"):
        get_latest_bronze_manifest(client, "data-bucket")

    paginator.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "auditoria/bronze/ingestion_date=2026-01-02/run_id=failed/manifest.json",
                    "LastModified": datetime(2026, 1, 2, tzinfo=timezone.utc),
                }
            ]
        }
    ]
    client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=b'{"status":"FAILED"}'))
    }
    with pytest.raises(ValueError, match="não representa uma execução concluída"):
        get_latest_bronze_manifest(client, "data-bucket")


def test_validate_bronze_manifest_requires_all_entities_and_valid_prefixes():
    prefixes, all_skipped = validate_bronze_manifest(
        _manifest(entity_status="SKIPPED (Already Up-to-Date)"),
        "data-bucket",
        ENTITIES,
    )

    assert set(prefixes) == set(ENTITIES)
    assert all_skipped is True

    incomplete = _manifest()
    incomplete["tables_metrics"].pop()
    with pytest.raises(ValueError, match="não contém todas as entidades"):
        validate_bronze_manifest(incomplete, "data-bucket", ENTITIES)

    invalid_prefix = _manifest()
    invalid_prefix["tables_metrics"][0]["s3_prefix"] = "s3://other/bronze/alunos/"
    with pytest.raises(ValueError, match="prefixo inválido"):
        validate_bronze_manifest(invalid_prefix, "data-bucket", ENTITIES)


def test_validate_bronze_manifest_does_not_skip_if_any_entity_was_ingested():
    manifest = _manifest(entity_status="SKIPPED (Already Up-to-Date)")
    manifest["tables_metrics"][0]["status"] = "SUCCEEDED"

    _, all_skipped = validate_bronze_manifest(manifest, "data-bucket", ENTITIES)

    assert all_skipped is False


def test_manifest_listing_permission_failure_is_reported():
    paginator = MagicMock()
    paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}},
        "ListObjectsV2",
    )
    client = MagicMock(get_paginator=MagicMock(return_value=paginator))

    with pytest.raises(RuntimeError, match="listar os manifestos"):
        get_latest_bronze_manifest(client, "data-bucket")


def _client_for_committed_manifest(manifest, layer_prefix, output_prefixes):
    paginator = MagicMock()

    def paginate(*, Bucket, Prefix):
        if Prefix == layer_prefix:
            return [
                {
                    "Contents": [
                        {
                            "Key": f"{layer_prefix}ingestion_date=2026-01-03/"
                            "run_id=run/manifest.json",
                            "LastModified": datetime(2026, 1, 3, tzinfo=timezone.utc),
                        }
                    ]
                }
            ]
        if Prefix in output_prefixes:
            suffix = "_delta_log/00000000000000000000.json"
            if layer_prefix.endswith("gold/"):
                suffix = "part-00000.parquet"
            return [{"Contents": [{"Key": f"{Prefix}{suffix}"}]}]
        return []

    paginator.paginate.side_effect = paginate
    client = MagicMock(get_paginator=MagicMock(return_value=paginator))
    client.get_object.return_value = {
        "Body": MagicMock(
            read=MagicMock(return_value=json.dumps(manifest).encode("utf-8"))
        )
    }
    return client


def test_latest_committed_silver_manifest_accepts_skipped_snapshot():
    outputs = {
        "municipio": "s3://data-bucket/silver/runs/run/metas/municipio/",
        "uf": "s3://data-bucket/silver/runs/run/metas/uf/",
        "brasil": "s3://data-bucket/silver/runs/run/metas/brasil/",
    }
    manifest = {
        "status": "SKIPPED (Already Up-to-Date)",
        "gate_status": "APPROVED",
        "publication_status": "COMMITTED",
        "official_student_s3_prefix": "s3://data-bucket/silver/runs/run/alunos/",
        "official_target_s3_prefixes": outputs,
    }
    client = _client_for_committed_manifest(
        manifest,
        "auditoria/silver/",
        {
            "silver/runs/run/alunos/",
            *(uri.removeprefix("s3://data-bucket/") for uri in outputs.values()),
        },
    )

    resolved = get_latest_approved_silver_manifest(client, "data-bucket")

    assert resolved == manifest


def test_latest_committed_gold_manifest_accepts_skipped_outputs():
    outputs = {
        "indicator": "s3://data-bucket/gold/runs/run/indicator/",
    }
    manifest = {
        "status": "SKIPPED (Already Up-to-Date)",
        "gate_status": "APPROVED",
        "publication_status": "COMMITTED",
        "gold_s3_prefixes": outputs,
    }
    client = _client_for_committed_manifest(
        manifest,
        "auditoria/gold/",
        {"gold/runs/run/indicator/"},
    )

    resolved = get_latest_approved_gold_manifest(
        client,
        "data-bucket",
        required_datasets=["indicator"],
    )

    assert resolved == manifest
