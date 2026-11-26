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


def get_latest_approved_gold_manifest(
    s3_client,
    bucket_name,
    required_datasets,
):
    """Resolve the latest committed Gold run and verify every required output."""
    try:
        pages = s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket_name,
            Prefix="auditoria/gold/",
        )
        manifests = [
            item
            for page in pages
            for item in page.get("Contents", [])
            if item.get("Key", "").endswith("/manifest.json")
        ]
    except ClientError as error:
        raise RuntimeError("Não foi possível listar os manifestos Gold.") from error

    for item in sorted(
        manifests,
        key=lambda manifest: manifest["LastModified"],
        reverse=True,
    ):
        key = item["Key"]
        manifest = _read_manifest(s3_client, bucket_name, key, "Camada Gold")
        if (
            manifest.get("status") not in {"SUCCEEDED", "SKIPPED (Already Up-to-Date)"}
            or manifest.get("gate_status") != "APPROVED"
            or manifest.get("publication_status") != "COMMITTED"
        ):
            continue

        outputs = manifest.get("gold_s3_prefixes")
        if not isinstance(outputs, dict):
            raise ValueError(
                f"O manifesto Gold comprometido '{key}' não lista os outputs."
            )
        missing = set(required_datasets).difference(outputs)
        if missing:
            raise ValueError(
                f"O manifesto Gold comprometido '{key}' não lista todos os "
                f"datasets obrigatórios: {sorted(missing)}."
            )

        for dataset in required_datasets:
            uri = outputs[dataset]
            expected_prefix = f"s3://{bucket_name}/gold/runs/"
            if not isinstance(uri, str) or not uri.startswith(expected_prefix):
                raise ValueError(
                    f"O manifesto Gold comprometido '{key}' contém um prefixo "
                    f"inválido para '{dataset}'."
                )
            prefix = uri.removeprefix(f"s3://{bucket_name}/")
            try:
                pages = s3_client.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket_name,
                    Prefix=prefix,
                )
            except ClientError as error:
                raise RuntimeError(
                    f"Não foi possível validar o output Gold '{dataset}'."
                ) from error
            has_parquet = any(
                object_item.get("Key", "").endswith(".parquet")
                for page in pages
                for object_item in page.get("Contents", [])
            )
            if not has_parquet:
                raise ValueError(
                    f"O manifesto Gold comprometido '{key}' aponta para um "
                    f"output ausente ou sem Parquet: '{dataset}'."
                )
        return manifest
    return None


def get_latest_approved_silver_manifest(s3_client, bucket_name):
    """Resolve the latest Silver snapshot published with a complete commit marker."""
    try:
        pages = s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket_name,
            Prefix="auditoria/silver/",
        )
        manifests = [
            item
            for page in pages
            for item in page.get("Contents", [])
            if item.get("Key", "").endswith("/manifest.json")
        ]
    except ClientError as error:
        raise RuntimeError("Não foi possível listar os manifestos Silver.") from error

    for item in sorted(
        manifests,
        key=lambda manifest: manifest["LastModified"],
        reverse=True,
    ):
        manifest = _read_manifest(s3_client, bucket_name, item["Key"], "Camada Silver")
        if (
            manifest.get("status") not in {"SUCCEEDED", "SKIPPED (Already Up-to-Date)"}
            or manifest.get("gate_status") != "APPROVED"
            or manifest.get("publication_status") != "COMMITTED"
        ):
            continue
        student_prefix = manifest.get("official_student_s3_prefix")
        targets = manifest.get("official_target_s3_prefixes")
        expected_prefix = f"s3://{bucket_name}/silver/runs/"
        if (
            not isinstance(student_prefix, str)
            or not student_prefix.startswith(expected_prefix)
            or not isinstance(targets, dict)
            or set(targets) != {"municipio", "uf", "brasil"}
            or any(
                not isinstance(prefix, str) or not prefix.startswith(expected_prefix)
                for prefix in targets.values()
            )
        ):
            raise ValueError(
                f"O manifesto Silver comprometido '{item['Key']}' não contém "
                "prefixos oficiais versionados válidos."
            )
        prefixes = [student_prefix, *targets.values()]
        for uri in prefixes:
            prefix = uri.removeprefix(f"s3://{bucket_name}/")
            try:
                pages = s3_client.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket_name,
                    Prefix=prefix,
                )
                has_delta_log = any(
                    object_item.get("Key", "").startswith(f"{prefix}_delta_log/")
                    for page in pages
                    for object_item in page.get("Contents", [])
                )
            except ClientError as error:
                raise RuntimeError(
                    f"Não foi possível validar o snapshot Silver em '{uri}'."
                ) from error
            if not has_delta_log:
                raise ValueError(
                    f"O manifesto Silver comprometido '{item['Key']}' aponta "
                    f"para um snapshot Delta ausente ou incompleto: '{uri}'."
                )
        return manifest
    return None


def _read_manifest(s3_client, bucket_name, key, layer_name):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        manifest = json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as error:
        raise RuntimeError(
            f"Não foi possível ler o manifesto da {layer_name} '{key}'."
        ) from error
    except (
        KeyError,
        AttributeError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise ValueError(
            f"O manifesto da {layer_name} '{key}' não contém JSON válido."
        ) from error
    if not isinstance(manifest, dict):
        raise ValueError(
            f"O manifesto da {layer_name} '{key}' deve conter um objeto JSON."
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
