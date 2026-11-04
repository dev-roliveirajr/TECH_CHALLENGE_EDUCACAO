import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError


@pytest.fixture(scope="module")
def deploy_module():
    project_root = Path(__file__).resolve().parents[1]
    infra_path = project_root / "src" / "infra"
    module_path = infra_path / "deploy_aws_setup.py"
    sys.path.insert(0, str(infra_path))
    try:
        with (
            patch("dotenv.load_dotenv", return_value=False),
            patch("boto3.client", return_value=MagicMock()),
        ):
            spec = importlib.util.spec_from_file_location(
                "deploy_aws_setup_under_test", module_path
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(infra_path))


def test_local_validation_rejects_missing_bucket_name(deploy_module, monkeypatch):
    monkeypatch.setattr(deploy_module, "raw_bucket_name", "")

    with pytest.raises(ValueError, match="S3_BUCKET_NAME"):
        deploy_module._validate_local_configuration()


def test_gold_aggregation_helper_is_uploaded_with_glue_scripts(deploy_module):
    helper = next(
        item
        for item in deploy_module.SCRIPTS_TO_DEPLOY
        if item["local"] == "src/common/gold_aggregation.py"
    )

    assert helper["s3_key"] == "scripts/common/gold_aggregation.py"
    assert helper["job_name"] is None


def test_local_validation_requires_service_account_credentials(
    deploy_module, monkeypatch, tmp_path
):
    credential_path = tmp_path / "service-account.json"
    credential_path.write_text('{"type": "authorized_user"}', encoding="utf-8")
    monkeypatch.setattr(deploy_module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(deploy_module, "GCP_KEY_LOCAL", credential_path.name)
    monkeypatch.setattr(deploy_module, "SCRIPTS_TO_DEPLOY", [])
    monkeypatch.setattr(deploy_module, "raw_bucket_name", "literacy-data")

    with pytest.raises(ValueError, match="service account"):
        deploy_module._validate_local_configuration()


def test_aws_identity_derives_role_arn_and_preserves_explicit_bucket(
    deploy_module, monkeypatch
):
    monkeypatch.setattr(deploy_module, "AWS_ACCOUNT_ID", "")
    monkeypatch.setattr(deploy_module, "raw_bucket_name", "literacy-data")
    monkeypatch.setattr(deploy_module, "raw_glue_role", "LabRole")
    monkeypatch.setattr(
        deploy_module,
        "sts_client",
        MagicMock(
            get_caller_identity=MagicMock(
                return_value={
                    "Account": "123456789012",
                    "Arn": "arn:aws:sts::123456789012:assumed-role/LabRole/session",
                }
            )
        ),
    )

    deploy_module._resolve_aws_identity()

    assert deploy_module.AWS_ACCOUNT_ID == "123456789012"
    assert deploy_module.S3_BUCKET == "literacy-data"
    assert deploy_module.GLUE_ROLE_ARN == "arn:aws:iam::123456789012:role/LabRole"


def test_bucket_permission_errors_do_not_assume_bucket_exists(
    deploy_module, monkeypatch
):
    client_error = ClientError(
        {"Error": {"Code": "403", "Message": "Access denied"}},
        "HeadBucket",
    )
    monkeypatch.setattr(
        deploy_module,
        "s3_client",
        MagicMock(head_bucket=MagicMock(side_effect=client_error)),
    )
    monkeypatch.setattr(deploy_module, "S3_BUCKET", "literacy-data")

    with pytest.raises(RuntimeError, match="Não foi possível verificar"):
        deploy_module._bucket_is_available()


def test_lifecycle_setup_preserves_unrelated_rules(deploy_module, monkeypatch):
    client = MagicMock(
        get_bucket_lifecycle_configuration=MagicMock(
            return_value={
                "Rules": [
                    {
                        "ID": "KeepExistingRule",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "archive/"},
                        "Expiration": {"Days": 90},
                    },
                    {
                        "ID": "ExpireRawLandingZoneEventsAfter7Days",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "old/"},
                        "Expiration": {"Days": 30},
                    },
                ]
            }
        )
    )
    monkeypatch.setattr(deploy_module, "s3_client", client)
    monkeypatch.setattr(deploy_module, "S3_BUCKET", "literacy-data")
    monkeypatch.setattr(deploy_module, "LIFECYCLE_RULES_EXPIRATION_DAYS", 7)

    deploy_module.setup_s3_lifecycle_rules()

    rules = client.put_bucket_lifecycle_configuration.call_args.kwargs[
        "LifecycleConfiguration"
    ]["Rules"]
    assert [rule["ID"] for rule in rules] == [
        "KeepExistingRule",
        "ExpireRawLandingZoneEventsAfter7Days",
    ]
    assert rules[-1]["Filter"]["Prefix"] == "bronze/events_raw_landing/"
