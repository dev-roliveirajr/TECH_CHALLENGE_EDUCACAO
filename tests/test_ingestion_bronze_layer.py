import importlib.util
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def bronze_module(monkeypatch):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "src" / "scripts" / "ingestion_bronze_layer.py"
    monkeypatch.setenv("S3_BUCKET_NAME", "test-bucket")
    monkeypatch.setenv("GCP_BIGQUERY_KEY_SECRET_NAME", "test-secret")
    monkeypatch.delenv("GCP_CREDENTIALS_LOCAL_FALLBACK", raising=False)

    spark = SimpleNamespace(
        conf=MagicMock(),
        read=MagicMock(),
    )
    glue_context_module = types.ModuleType("awsglue.context")
    glue_context_module.GlueContext = MagicMock(
        return_value=SimpleNamespace(spark_session=spark)
    )
    awsglue_module = types.ModuleType("awsglue")
    awsglue_module.__path__ = []
    awsglue_module.context = glue_context_module

    glue_utils_module = types.ModuleType("awsglue.utils")
    glue_utils_module.getResolvedOptions = MagicMock(
        side_effect=ValueError("not a Glue invocation")
    )
    awsglue_module.utils = glue_utils_module

    spark_context_module = types.ModuleType("pyspark.context")
    spark_context_module.SparkContext = MagicMock()
    pyspark_module = types.ModuleType("pyspark")
    pyspark_module.__path__ = []
    pyspark_module.context = spark_context_module

    spark_functions_module = types.ModuleType("pyspark.sql.functions")
    spark_functions_module.lit = lambda value: value
    spark_sql_module = types.ModuleType("pyspark.sql")
    spark_sql_module.__path__ = []
    spark_sql_module.functions = spark_functions_module
    pyspark_module.sql = spark_sql_module

    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.Client = MagicMock()
    google_cloud_module = types.ModuleType("google.cloud")
    google_cloud_module.__path__ = []
    google_cloud_module.bigquery = bigquery_module

    service_account_module = types.ModuleType("google.oauth2.service_account")
    service_account_module.Credentials = SimpleNamespace(
        from_service_account_info=MagicMock(
            return_value=SimpleNamespace(project_id="test-project")
        )
    )
    google_oauth_module = types.ModuleType("google.oauth2")
    google_oauth_module.__path__ = []
    google_oauth_module.service_account = service_account_module

    google_module = types.ModuleType("google")
    google_module.__path__ = []
    google_module.cloud = google_cloud_module
    google_module.oauth2 = google_oauth_module

    s3_client = MagicMock()
    secrets_client = MagicMock(
        get_secret_value=MagicMock(
            return_value={
                "SecretString": json.dumps(
                    {
                        "type": "service_account",
                        "client_email": "pipeline@example.test",
                        "private_key": "test-key",
                    }
                )
            }
        )
    )

    import boto3

    monkeypatch.setattr(
        boto3,
        "client",
        lambda service, **kwargs: (
            secrets_client if service == "secretsmanager" else s3_client
        ),
    )
    for name, module in {
        "awsglue": awsglue_module,
        "awsglue.context": glue_context_module,
        "awsglue.utils": glue_utils_module,
        "pyspark": pyspark_module,
        "pyspark.context": spark_context_module,
        "pyspark.sql": spark_sql_module,
        "pyspark.sql.functions": spark_functions_module,
        "google": google_module,
        "google.cloud": google_cloud_module,
        "google.cloud.bigquery": bigquery_module,
        "google.oauth2": google_oauth_module,
        "google.oauth2.service_account": service_account_module,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    spec = importlib.util.spec_from_file_location(
        "ingestion_bronze_layer_under_test", script_path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_current_source_version_skips_bigquery_table_read(bronze_module, monkeypatch):
    source_modified = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)
    bronze_module.bq_client.get_table.return_value = SimpleNamespace(
        modified=source_modified,
        num_rows=100,
        num_bytes=1024,
    )
    monkeypatch.setattr(
        bronze_module,
        "_read_entity_state",
        lambda entity: {
            "source_modified_at": source_modified,
            "source_modified": "2026-11-03T12:00:00Z",
            "bq_rows": 100,
            "source_num_bytes": 1024,
            "s3_prefix": (
                "s3://test-bucket/bronze/alunos/" "ref_ano=2026/ref_mes=11/ref_dia=03/"
            ),
            "size_mb": 0.01,
        },
    )
    monkeypatch.setattr(bronze_module, "_entity_state_has_data", lambda *_: True)
    read_table = MagicMock(side_effect=AssertionError("unexpected full table read"))
    monkeypatch.setattr(bronze_module, "load_bigquery_table", read_table)

    result = bronze_module.process_entity("alunos")

    assert result["status"] == "SKIPPED (Already Up-to-Date)"
    assert result["source_modified"] == "2026-11-03T12:00:00Z"
    read_table.assert_not_called()


def test_partition_overwrite_mode_uses_connector_enum_value(bronze_module):
    bronze_module.spark.conf.set.assert_any_call(
        "spark.sql.sources.partitionOverwriteMode", "DYNAMIC"
    )


def test_missing_marker_reads_table_once_and_records_current_version(
    bronze_module, monkeypatch
):
    source_modified = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)
    bronze_module.bq_client.get_table.return_value = SimpleNamespace(
        modified=source_modified,
        num_rows=100,
        num_bytes=1024,
    )
    monkeypatch.setattr(bronze_module, "_read_entity_state", lambda entity: None)
    dataframe = MagicMock()
    dataframe.columns = []
    dataframe.rdd.getNumPartitions.return_value = 1
    dataframe.withColumn.return_value = dataframe
    dataframe.coalesce.return_value = dataframe
    read_table = MagicMock(return_value=dataframe)
    monkeypatch.setattr(bronze_module, "load_bigquery_table", read_table)
    write_state = MagicMock()
    monkeypatch.setattr(bronze_module, "_write_entity_state", write_state)

    result = bronze_module.process_entity("alunos")

    assert result["status"] == "SUCCEEDED"
    read_table.assert_called_once_with(f"{bronze_module.DATASET_BQ}.alunos")
    assert write_state.call_args.args[1]["source_modified"] == ("2026-11-03T12:00:00Z")


def test_new_source_version_replaces_partition_and_records_state(
    bronze_module, monkeypatch
):
    previous_modified = datetime(2026, 11, 2, 12, tzinfo=timezone.utc)
    current_modified = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)
    bronze_module.bq_client.get_table.return_value = SimpleNamespace(
        modified=current_modified,
        num_rows=100,
        num_bytes=1024,
    )
    monkeypatch.setattr(
        bronze_module,
        "_read_entity_state",
        lambda entity: {
            "source_modified_at": previous_modified,
            "bq_rows": 100,
            "source_num_bytes": 1024,
            "s3_prefix": (
                "s3://test-bucket/bronze/alunos/" "ref_ano=2026/ref_mes=11/ref_dia=02/"
            ),
        },
    )
    monkeypatch.setattr(bronze_module, "_entity_state_has_data", lambda *_: True)

    dataframe = MagicMock()
    dataframe.columns = []
    dataframe.rdd.getNumPartitions.return_value = 1
    dataframe.withColumn.return_value = dataframe
    dataframe.coalesce.return_value = dataframe
    monkeypatch.setattr(
        bronze_module, "load_bigquery_table", MagicMock(return_value=dataframe)
    )
    write_state = MagicMock()
    monkeypatch.setattr(bronze_module, "_write_entity_state", write_state)

    result = bronze_module.process_entity("alunos")

    assert result["status"] == "SUCCEEDED"
    assert result["source_modified"] == "2026-11-03T12:00:00Z"
    dataframe.write.mode.assert_called_once_with("overwrite")
    dataframe.write.mode.return_value.partitionBy.assert_called_once_with(
        "ref_ano", "ref_mes", "ref_dia"
    )
    recorded_state = write_state.call_args.args[1]
    assert recorded_state["source_modified"] == "2026-11-03T12:00:00Z"
    assert recorded_state["status"] == "SUCCEEDED"


def test_changed_metadata_reloads_table_even_when_modified_time_matches(
    bronze_module, monkeypatch
):
    source_modified = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)
    bronze_module.bq_client.get_table.return_value = SimpleNamespace(
        modified=source_modified,
        num_rows=101,
        num_bytes=2048,
    )
    monkeypatch.setattr(
        bronze_module,
        "_read_entity_state",
        lambda entity: {
            "source_modified_at": source_modified,
            "source_modified": "2026-11-03T12:00:00Z",
            "bq_rows": 100,
            "source_num_bytes": 1024,
            "s3_prefix": (
                "s3://test-bucket/bronze/alunos/" "ref_ano=2026/ref_mes=11/ref_dia=03/"
            ),
            "size_mb": 0.01,
        },
    )
    monkeypatch.setattr(bronze_module, "_entity_state_has_data", lambda *_: True)
    dataframe = MagicMock()
    dataframe.columns = []
    dataframe.rdd.getNumPartitions.return_value = 1
    dataframe.withColumn.return_value = dataframe
    dataframe.coalesce.return_value = dataframe
    read_table = MagicMock(return_value=dataframe)
    monkeypatch.setattr(bronze_module, "load_bigquery_table", read_table)
    monkeypatch.setattr(bronze_module, "_write_entity_state", MagicMock())

    result = bronze_module.process_entity("alunos")

    assert result["status"] == "SUCCEEDED"
    read_table.assert_called_once()


def test_missing_source_state_is_treated_as_not_ingested(bronze_module):
    bronze_module.s3_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
        "GetObject",
    )

    assert bronze_module._read_entity_state("alunos") is None


def test_source_state_access_failure_is_reported(bronze_module):
    bronze_module.s3_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}},
        "GetObject",
    )

    with pytest.raises(RuntimeError, match="consultar o estado Bronze"):
        bronze_module._read_entity_state("alunos")
