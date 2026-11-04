"""Helpers for reading and validating pipeline manifests."""

import json

from botocore.exceptions import ClientError

BRONZE_SUCCESS_STATUSES = {
    "SUCCEEDED",
    "SKIPPED (Already Up-to-Date)",
}


def get_latest_manifest(s3_client, bucket_name, prefix, layer_name):
    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        pages = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
        )
        manifests = [
            item
            for page in pages
            for item in page.get("Contents", [])
            if item.get("Key", "").endswith("/manifest.json")
        ]
    except ClientError as error:
        raise RuntimeError(
            f"Não foi possível listar os manifestos da {layer_name}."
        ) from error

    if not manifests:
        return None

    latest = max(manifests, key=lambda item: item["LastModified"])
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=latest["Key"])
        manifest = json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as error:
        raise RuntimeError(
            f"Não foi possível ler o manifesto da {layer_name} " f"'{latest['Key']}'."
        ) from error
    except (
        KeyError,
        AttributeError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise ValueError(
            f"O manifesto da {layer_name} '{latest['Key']}' não contém JSON válido."
        ) from error

    if not isinstance(manifest, dict):
        raise ValueError(
            f"O manifesto da {layer_name} '{latest['Key']}' deve conter "
            "um objeto JSON."
        )
    return manifest


def get_latest_bronze_manifest(s3_client, bucket_name):
    manifest = get_latest_manifest(
        s3_client,
        bucket_name,
        "auditoria/bronze/ingestion_date=",
        "Camada Bronze",
    )
    if manifest is None:
        raise FileNotFoundError(
            "Não há manifesto da Camada Bronze disponível para processamento."
        )
    if manifest.get("status") != "SUCCEEDED":
        raise ValueError(
            "O manifesto Bronze mais recente não representa uma execução concluída."
        )
    return manifest


def validate_bronze_manifest(manifest, bucket_name, expected_entities):
    metrics = manifest.get("tables_metrics")
    if not isinstance(metrics, list):
        raise ValueError("O manifesto Bronze não contém tables_metrics válido.")

    expected_entities = set(expected_entities)
    prefixes = {}
    statuses = {}
    for metric in metrics:
        if not isinstance(metric, dict):
            raise ValueError("O manifesto Bronze contém uma métrica inválida.")
        entity = metric.get("entity")
        status = metric.get("status")
        prefix = metric.get("s3_prefix")
        if entity not in expected_entities:
            continue
        if entity in statuses:
            raise ValueError(
                f"O manifesto Bronze contém mais de um registro para '{entity}'."
            )
        if status not in BRONZE_SUCCESS_STATUSES:
            raise ValueError(
                f"A entidade Bronze '{entity}' não foi concluída "
                f"(status: {status!r})."
            )
        expected_prefix = f"s3://{bucket_name}/bronze/{entity}/"
        if not isinstance(prefix, str) or not prefix.startswith(expected_prefix):
            raise ValueError(
                f"O manifesto Bronze contém um prefixo inválido para '{entity}'."
            )
        statuses[entity] = status
        prefixes[entity] = prefix

    missing_entities = expected_entities.difference(statuses)
    if missing_entities:
        raise ValueError(
            "O manifesto Bronze não contém todas as entidades necessárias: "
            + ", ".join(sorted(missing_entities))
        )

    all_skipped = all(
        status == "SKIPPED (Already Up-to-Date)" for status in statuses.values()
    )
    return prefixes, all_skipped
