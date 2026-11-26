from unittest.mock import MagicMock

import pandas as pd
import pytest

from common import gold_sync


def test_sync_committed_gold_uses_only_manifest_outputs(tmp_path, monkeypatch):
    outputs = {
        manifest_name: f"s3://data-bucket/gold/runs/run-1/{manifest_name}/"
        for manifest_name in gold_sync.GOLD_LOCAL_DATASETS.values()
    }
    manifest = {"run_id": "run-1", "gold_s3_prefixes": outputs}
    monkeypatch.setattr(
        gold_sync,
        "get_latest_approved_gold_manifest",
        lambda *args, **kwargs: manifest,
    )

    def paginate(*, Bucket, Prefix):
        return [
            {"Contents": [{"Key": f"{Prefix}part-00000.parquet"}]},
        ]

    client = MagicMock()
    client.get_paginator.return_value.paginate.side_effect = paginate

    def download_file(bucket, key, path):
        pd.DataFrame({"source_key": [key]}).to_parquet(path, index=False)

    client.download_file.side_effect = download_file

    resolved_run = gold_sync.sync_committed_gold(
        client,
        "data-bucket",
        str(tmp_path),
    )

    assert resolved_run == "run-1"
    assert client.download_file.call_count == len(gold_sync.GOLD_LOCAL_DATASETS)
    for local_name in gold_sync.GOLD_LOCAL_DATASETS:
        target = tmp_path / f"{local_name}.parquet"
        assert target.is_file()
        assert pd.read_parquet(target).iloc[0]["source_key"].startswith(
            "gold/runs/run-1/"
        )

def test_sync_committed_gold_rejects_missing_commit_before_writing(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        gold_sync,
        "get_latest_approved_gold_manifest",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(FileNotFoundError, match="manifesto Gold comprometido"):
        gold_sync.sync_committed_gold(MagicMock(), "data-bucket", str(tmp_path))

    assert list(tmp_path.iterdir()) == []
