"""Synchronize only datasets from the latest committed Gold manifest."""

import os
import tempfile

from common.manifest_lib import get_latest_approved_gold_manifest

GOLD_LOCAL_DATASETS = {
    "gold_features_alfabetizacao": "features_municipais",
    "gold_features_alfabetizacao_uf": "features_uf",
    "gold_features_alfabetizacao_brasil": "features_brasil",
    "gold_indicadores_municipais": "indicadores_municipais",
    "gold_indicadores_uf": "indicadores_uf",
    "gold_indicadores_brasil": "indicadores_brasil",
    "gold_desempenho_vs_metas": "metas_municipais",
    "gold_desempenho_vs_metas_uf": "metas_uf",
    "gold_desempenho_vs_metas_brasil": "metas_brasil",
}


def sync_committed_gold(s3_client, bucket_name, output_dir):
    """Download and consolidate every dataset from one approved Gold run."""
    import pandas as pd

    manifest = get_latest_approved_gold_manifest(
        s3_client,
        bucket_name,
        required_datasets=GOLD_LOCAL_DATASETS.values(),
    )
    if manifest is None:
        raise FileNotFoundError(
            "Não há manifesto Gold comprometido para sincronização local."
        )

    os.makedirs(output_dir, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output_dir) as temporary_dir:
        staged_files = []
        for local_name, manifest_name in GOLD_LOCAL_DATASETS.items():
            uri = manifest["gold_s3_prefixes"][manifest_name]
            prefix = uri.removeprefix(f"s3://{bucket_name}/")
            paginator = s3_client.get_paginator("list_objects_v2")
            parquet_keys = [
                item["Key"]
                for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix)
                for item in page.get("Contents", [])
                if item.get("Key", "").endswith(".parquet")
            ]
            if not parquet_keys:
                raise ValueError(
                    f"O dataset Gold comprometido '{manifest_name}' não contém "
                    "arquivos Parquet."
                )

            parts = []
            for index, key in enumerate(parquet_keys):
                local_part = os.path.join(
                    temporary_dir,
                    f"{local_name}_{index}.parquet",
                )
                s3_client.download_file(bucket_name, key, local_part)
                parts.append(pd.read_parquet(local_part))

            staged_path = os.path.join(temporary_dir, f"{local_name}.parquet")
            pd.concat(parts, ignore_index=True).to_parquet(staged_path, index=False)
            staged_files.append(
                (
                    staged_path,
                    os.path.join(output_dir, f"{local_name}.parquet"),
                )
            )

        for staged_path, target_path in staged_files:
            os.replace(staged_path, target_path)

    return manifest["run_id"]
