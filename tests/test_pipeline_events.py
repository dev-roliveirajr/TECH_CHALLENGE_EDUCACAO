import pytest

import pipeline_events


def test_local_event_check_finds_partitioned_meta_event(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    landing = tmp_path / "data/events_landing/meta_uf/ref_ano=2024/ref_mes=01"
    landing.mkdir(parents=True)
    (landing / "events.json").write_text("{}\n", encoding="utf-8")

    assert pipeline_events.check_local_landing_events()


def test_s3_event_check_paginates_until_json_event(monkeypatch):
    class FakePaginator:
        def paginate(self, **kwargs):
            yield {"Contents": [{"Key": "bronze/events_raw_landing/README.txt"}]}
            yield {"Contents": [{"Key": "bronze/events_raw_landing/alunos/a.json"}]}

    class FakeS3:
        def get_paginator(self, operation):
            assert operation == "list_objects_v2"
            return FakePaginator()

    monkeypatch.setattr(pipeline_events, "get_s3_client", lambda: FakeS3())

    assert pipeline_events.check_s3_landing_events()


def test_s3_event_check_propagates_listing_errors(monkeypatch):
    class FakePaginator:
        def paginate(self, **kwargs):
            raise PermissionError("S3 list denied")

    class FakeS3:
        def get_paginator(self, operation):
            return FakePaginator()

    monkeypatch.setattr(pipeline_events, "get_s3_client", lambda: FakeS3())

    with pytest.raises(PermissionError, match="S3 list denied"):
        pipeline_events.check_s3_landing_events()


def test_glue_start_failure_reports_error_and_runs_local_fallback(monkeypatch, capsys):
    class FakeGlue:
        def start_job_run(self, **kwargs):
            raise RuntimeError("Glue unavailable")

    monkeypatch.setattr(pipeline_events, "get_glue_client", lambda: FakeGlue())
    monkeypatch.setattr(pipeline_events, "get_logs_client", lambda: None)
    monkeypatch.setattr(
        pipeline_events,
        "run_local_script_fallback",
        lambda job_name: f"fallback:{job_name}",
    )

    result = pipeline_events.run_glue_job("04_events_processor")

    assert result == "fallback:04_events_processor"
    assert "Glue unavailable" in capsys.readouterr().out
