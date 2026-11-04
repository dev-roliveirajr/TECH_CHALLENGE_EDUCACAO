"""
===============================================================================
GERADOR DE RELATÓRIOS DE EXECUÇÃO (src/monitor/report_generator.py)
===============================================================================
Descrição: Módulo chamado pelo orquestrador (pipeline.py) ao final de cada Job.
  - Leitura de manifestos S3 das camadas Bronze, Silver e Gold.
  - Inclusão de métricas e logs do CloudWatch nos relatórios.
===============================================================================
"""

import os
import sys
import datetime
import json
import re
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv(override=True)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "tc-alfabetizacao")

if AWS_ACCOUNT_ID and AWS_ACCOUNT_ID not in S3_BUCKET_NAME:
    FULL_BUCKET_NAME = f"{S3_BUCKET_NAME}-{AWS_ACCOUNT_ID}-{AWS_REGION}"
else:
    FULL_BUCKET_NAME = S3_BUCKET_NAME

COST_PER_DPU_HOUR = 0.44  # Custo médio AWS Glue G.1X DPU-Hora em USD


def _as_utc(value):
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc)


def _format_utc_timestamp(value):
    if value is None:
        return "N/A"
    return _as_utc(value).strftime("%Y-%m-%d %H:%M:%S UTC")


def fetch_stdout_cloudwatch_logs(log_stream_name: str) -> str:
    """Busca o log completo de saída de aplicação (stdout) do Log Group /aws-glue/jobs/output."""
    if not log_stream_name:
        return " LogStream de execução não disponibilizado."

    logs_client = boto3.client("logs", region_name=AWS_REGION)
    log_group = "/aws-glue/jobs/output"

    try:
        filtered_lines = []
        noise_indicators = ["FutureWarning", "google.api_core", "WARN NativeCodeLoader"]
        next_token = None

        while True:
            kwargs = {
                "logGroupName": log_group,
                "logStreamName": log_stream_name,
                "startFromHead": True,
                "limit": 10000,
            }
            if next_token:
                kwargs["nextToken"] = next_token

            response = logs_client.get_log_events(**kwargs)
            events = response.get("events", [])

            for ev in events:
                msg = ev["message"].strip()
                if any(noise in msg for noise in noise_indicators):
                    continue
                timestamp_str = datetime.datetime.fromtimestamp(
                    ev["timestamp"] / 1000.0, tz=datetime.timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S")
                filtered_lines.append(f"[{timestamp_str} UTC] {msg}")

            new_token = response.get("nextForwardToken")
            if not events or new_token == next_token:
                break
            next_token = new_token

        return (
            "\n".join(filtered_lines)
            if filtered_lines
            else " Execução concluída sem mensagens relevantes adicionais."
        )
    except Exception as e:
        return f" Log de execução disponível no console CloudWatch (Grupo: {log_group}, Stream: {log_stream_name}). Msg: {str(e)}"


def _job_layer(job_name: str) -> str:
    if "gold" in job_name.lower() or "03" in job_name:
        return "gold"
    if "silver" in job_name.lower() or "02" in job_name:
        return "silver"
    if "events" in job_name.lower() or "04" in job_name:
        return "events"
    return "bronze"


def _manifest_keys_from_logs(logs_text: str, layer: str) -> list[str]:
    uri_pattern = re.compile(
        rf"s3://{re.escape(FULL_BUCKET_NAME)}/"
        rf"(auditoria/{layer}/[^\s`]+?/manifest\.json)"
    )
    return list(dict.fromkeys(reversed(uri_pattern.findall(logs_text))))


def _manifest_key_candidates(
    job_name: str, run_id: str, ingestion_date_str: str, logs_text: str
) -> tuple[str, list[str]]:
    layer = _job_layer(job_name)
    audit_folder = f"auditoria/{layer}"
    candidates = _manifest_keys_from_logs(logs_text, layer)
    candidates.append(
        f"{audit_folder}/ingestion_date={ingestion_date_str}/"
        f"run_id={run_id}/manifest.json"
    )
    return layer, list(dict.fromkeys(candidates))


def fetch_s3_manifest(
    job_name: str, run_id: str, ingestion_date_str: str, logs_text: str = ""
) -> tuple[dict | None, str | None]:
    """Busca o manifesto correto da execução, usando o caminho registrado nos logs."""
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    layer, candidates = _manifest_key_candidates(
        job_name, run_id, ingestion_date_str, logs_text
    )

    def read_candidates(keys):
        for key in keys:
            try:
                response = s3_client.get_object(Bucket=FULL_BUCKET_NAME, Key=key)
            except ClientError as error:
                error_code = error.response.get("Error", {}).get("Code")
                if error_code in {"404", "NoSuchKey", "NotFound"}:
                    continue
                raise
            manifest_data = json.loads(response["Body"].read().decode("utf-8"))
            return manifest_data, key
        return None, None

    manifest_data, manifest_key = read_candidates(candidates)
    if manifest_data:
        return manifest_data, manifest_key

    paginator = s3_client.get_paginator("list_objects_v2")
    matching_keys = []
    for page in paginator.paginate(
        Bucket=FULL_BUCKET_NAME, Prefix=f"auditoria/{layer}/"
    ):
        matching_keys.extend(
            item["Key"]
            for item in page.get("Contents", [])
            if item["Key"].endswith(f"/run_id={run_id}/manifest.json")
        )
    return read_candidates(matching_keys)


def _build_layer_summary(layer: str, manifest_data: dict):
    if layer == "events":
        return _build_events_summary(manifest_data)
    if layer == "gold":
        return _build_gold_summary(manifest_data)
    if layer == "silver":
        return _build_silver_summary(manifest_data)
    return _build_bronze_summary(manifest_data)


def _build_events_summary(manifest_data: dict | None):
    title = "## 5. Resumo do Processamento de Eventos"
    if not manifest_data:
        return (
            title,
            " Manifesto de eventos não encontrado ou indisponível no S3.",
        )

    latency = manifest_data.get("latency_metrics_seconds", {})
    domain_counts = manifest_data.get("metas_events_by_domain", {})
    content = f"""| Métrica de Eventos | Valor |
| :--- | :---: |
| **Status** | `{manifest_data.get("status", "N/A")}` |
| **Total processado** | **{manifest_data.get("total_events_processed", "N/A")}** |
| **Eventos de alunos** | **{manifest_data.get("alunos_events_count", "N/A")}** |
| **Eventos de metas** | **{manifest_data.get("metas_events_count", "N/A")}** |
| **Metas por município / UF / Brasil** | **{domain_counts.get("municipio", "N/A")} / {domain_counts.get("uf", "N/A")} / {domain_counts.get("brasil", "N/A")}** |
| **Latência média / mínima / máxima** | **{latency.get("avg_sec", "N/A")} / {latency.get("min_sec", "N/A")} / {latency.get("max_sec", "N/A")} s** |
| **Engine** | {manifest_data.get("streaming_engine", "N/A")} |
"""
    return title, content


def _format_manifest_metric(value, skipped, formatter):
    if value is not None:
        return formatter(value)
    if skipped:
        return "N/A (sem processamento nesta execução)"
    return "N/A (não informado no manifesto)"


def _format_level_counts(counts):
    return "<br>".join(f"{level}: {count:,}" for level, count in counts.items())


def _build_gold_summary(manifest_data: dict):
    title = "## 5. Resumo da Camada Gold"
    if not manifest_data:
        return (
            title,
            " Dados do manifesto Gold em processamento ou indisponíveis no S3.",
        )

    skipped = str(manifest_data.get("status", "")).startswith("SKIPPED")
    in_rows = _format_manifest_metric(
        manifest_data.get("input_rows_silver"), skipped, lambda value: f"{value:,}"
    )
    out_ind = _format_manifest_metric(
        manifest_data.get("output_rows_indicadores"),
        skipped,
        lambda value: f"{value:,}",
    )
    out_metas = _format_manifest_metric(
        manifest_data.get("output_rows_metas"), skipped, lambda value: f"{value:,}"
    )
    out_feat = _format_manifest_metric(
        manifest_data.get("output_rows_features"), skipped, lambda value: f"{value:,}"
    )
    indicator_breakdown = _format_level_counts(
        manifest_data.get("output_rows_indicadores_by_level", {})
    )
    meta_breakdown = _format_level_counts(
        manifest_data.get("output_rows_metas_by_level", {})
    )
    feature_breakdown = _format_level_counts(
        manifest_data.get("output_rows_features_by_level", {})
    )
    cv_status = _format_cross_validation_status(manifest_data, skipped)
    meta_ref = manifest_data.get("target_meta_pactuada", "80.0%")
    gold_prefixes = manifest_data.get("gold_s3_prefixes", {})
    destinations = (
        "<br>".join(f"{name}: `{prefix}`" for name, prefix in gold_prefixes.items())
        if gold_prefixes
        else f"`s3://{FULL_BUCKET_NAME}/gold/`"
    )
    content = f"""| Métrica Analítica / Governança | Valor Registrado | Observação de Engenharia |
| :--- | :---: | :--- |
| **Linhas Lidas da Silver** | **{in_rows}** | Registros curados da tabela `alunos_curado` |
| **Indicadores Ponderados (Município/UF/Brasil)** | **{out_ind}** | {indicator_breakdown or "Agregação ponderada pelo peso do aluno"} |
| **Comparativos de Metas (Município/UF/Brasil)** | **{out_metas}** | {meta_breakdown or "Comparação com a meta oficial de 80%"} |
| **Feature Stores (Município/UF/Brasil)** | **{out_feat}** | {feature_breakdown or "Registros gravados nos conjuntos de características"} |
| **Validação Cruzada (Microdado vs. Inep)** | **{cv_status}** | Batimento da taxa recalculada vs. oficial |
| **Meta Oficial de Referência** | **{meta_ref}** | Meta de 2030; redes compatíveis conforme dicionário oficial |
| **Destinos S3 Gold** | {destinations} | Localização dos conjuntos de saída |
"""
    return title, content


def _format_cross_validation_status(manifest_data, skipped):
    cv_passed = manifest_data.get("cross_validation_passed")
    cv_diff = manifest_data.get("cross_validation_diff_percent")
    if cv_passed is None or cv_diff is None:
        if skipped:
            return "Não reexecutada (conjuntos existentes mantidos)"
        return "N/A (não informado no manifesto)"
    if cv_passed:
        return f"Dentro da tolerância ({cv_diff:.3f}%)"
    return f"Fora da tolerância ({cv_diff:.3f}%)"


def _build_bronze_summary(manifest_data: dict):
    title = "## 5. Metadados da origem e resultado da ingestão Bronze"
    if not manifest_data or "tables_metrics" not in manifest_data:
        return (
            title,
            " Dados do manifesto Bronze em processamento ou indisponíveis no S3.",
        )

    comparison_rows = []
    for item in manifest_data["tables_metrics"]:
        entity = item.get("entity", "N/A")
        bq_rows = item.get("bq_rows")
        s3_rows = item.get("s3_rows")
        status = item.get("status", "N/A")
        partitions = item.get("partitions", 0)
        size_mb = item.get("size_mb", 0.0)
        bq_str = f"{bq_rows:,}" if isinstance(bq_rows, int) and bq_rows >= 0 else "N/A"
        s3_str = f"{s3_rows:,}" if isinstance(s3_rows, int) and s3_rows >= 0 else "N/A"
        comparison_rows.append(
            f"| `{entity}` | {bq_str} | {s3_str} | `{status}` | {partitions} part. ({size_mb} MB) |"
        )
    content = (
        "| Entidade | Linhas no BigQuery (Metadados) | Linhas gravadas no S3 (quando medidas) | Status | Particionamento e tamanho |\n| :--- | :---: | :---: | :---: | :---: |\n"
        + "\n".join(comparison_rows)
    )
    return title, content


def _build_silver_summary(manifest_data: dict):
    title = "## 5. Resumo da Camada Silver"
    if not manifest_data:
        return (
            title,
            " Dados do manifesto Silver em processamento ou indisponíveis no S3.",
        )

    skipped = str(manifest_data.get("status", "")).startswith("SKIPPED")
    input_rows = manifest_data.get("input_rows_alunos", manifest_data.get("input_rows"))
    output_rows = manifest_data.get(
        "output_rows_curado", manifest_data.get("output_rows")
    )
    match_ratio = manifest_data.get(
        "join_match_ratio_percent", manifest_data.get("match_ratio")
    )
    quality_gate_passed = manifest_data.get("quality_gate_passed")
    if quality_gate_passed is None:
        gate_status = manifest_data.get("gate_status")
        if gate_status in {"APPROVED", "FAILED"}:
            quality_gate_passed = gate_status == "APPROVED"

    if skipped:
        qg_status = "Não reexecutado (Silver existente preservada)"
    elif quality_gate_passed is True:
        qg_status = "Dentro do limite"
    elif quality_gate_passed is False:
        qg_status = "Abaixo do limite"
    else:
        qg_status = "Não informado no manifesto"

    def metric_or_not_run(value, formatter):
        if value is not None:
            return formatter(value)
        if skipped:
            return "N/A (sem processamento nesta execução)"
        return "N/A (não informado no manifesto)"

    in_rows = metric_or_not_run(input_rows, lambda value: f"{value:,}")
    out_rows = metric_or_not_run(output_rows, lambda value: f"{value:,}")
    match_ratio_text = metric_or_not_run(match_ratio, lambda value: f"{value:.2f}%")
    silver_uri = manifest_data.get(
        "silver_s3_prefix",
        manifest_data.get("s3_path", f"s3://{FULL_BUCKET_NAME}/silver/alunos_curado/"),
    )
    null_handling = (
        "Não reexecutado (conjunto existente mantido)" if skipped else "Preservado"
    )
    goal_status = (
        "Não reexecutado (conjunto existente mantido)"
        if skipped
        else "Tripla categorização"
    )
    content = f"""| Métrica de Curadoria / Qualidade | Valor Registrado | Observação de Engenharia |
| :--- | :---: | :--- |
| **Linhas de Alunos (Entrada)** | **{in_rows}** | Registros brutos lidos do manifesto Bronze |
| **Linhas Curadas (Saída)** | **{out_rows}** | Registros finais enriquecidos e validados |
| **Taxa de correspondência do JOIN** | **{match_ratio_text}** | Percentual de registros correspondentes |
| **Resultado da verificação do JOIN** | **{qg_status}** | Limite configurado em `check_join_ratio`; não reexecutada quando a camada foi ignorada |
| **Tratamento de NULOs** | **{null_handling}** | Valores existentes são mantidos quando o job é ignorado |
| **Status das Metas** | **{goal_status}** | 'Atingiu', 'Não Atingiu' ou 'Sem Meta Cadastrada' |
| **Destino S3** | `{silver_uri}` | Formato Delta Lake, sem partição por data configurada pelo job |
"""
    return title, content


def generate_job_report(
    job_name: str, run_id: str, output_dir: str = "reports/job_executions"
) -> str:
    """Consulta o Glue, S3 e CloudWatch e gera o relatório Markdown completo para Bronze, Silver ou Gold."""
    print(f" Gerando relatório de execução para {job_name} (RunId: {run_id[:8]})...")

    glue_client = boto3.client("glue", region_name=AWS_REGION)

    try:
        job_run_resp = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        job_run = job_run_resp.get("JobRun", {})

        status = job_run.get("JobRunState", "UNKNOWN")
        execution_time = job_run.get("ExecutionTime", 0)
        dpu_seconds = job_run.get("DPUSeconds", 0)
        error_message = job_run.get("ErrorMessage", "")

        started_on = job_run.get("StartedOn")
        started_str = _format_utc_timestamp(started_on)
        started_utc = started_on or datetime.datetime.now(datetime.timezone.utc)
        started_utc = _as_utc(started_utc)
        ingestion_date_str = started_utc.strftime("%Y-%m-%d")

        completed_on = job_run.get("CompletedOn")
        completed_str = _format_utc_timestamp(completed_on)

        # Cálculo do custo de execução
        dpu_hours = (
            dpu_seconds / 3600.0 if dpu_seconds else (execution_time / 3600.0) * 2.0
        )
        cost_usd = dpu_hours * COST_PER_DPU_HOUR

        logs_text = fetch_stdout_cloudwatch_logs(run_id)
        manifest_data, manifest_key = fetch_s3_manifest(
            job_name, run_id, ingestion_date_str, logs_text
        )
        layer = _job_layer(job_name)

        if layer == "gold":
            origin_desc = "Camada Silver (Parquet / S3 em alunos_curado/)"
        elif layer == "silver":
            origin_desc = "Camada Bronze (Parquet / S3 via Manifesto)"
        elif layer == "events":
            origin_desc = "S3 Landing Zone (arquivos NDJSON de eventos)"
        else:
            origin_desc = "GCP BigQuery (basedosdados.br_inep_avaliacao_alfabetizacao)"

        manifest_uri = (
            f"s3://{FULL_BUCKET_NAME}/{manifest_key}"
            if manifest_key
            else f"s3://{FULL_BUCKET_NAME}/auditoria/{layer}/"
            f"ingestion_date={ingestion_date_str}/run_id={run_id}/manifest.json"
        )

        error_section = ""
        if error_message:
            error_section = f"\n###  Mensagem de Erro Registrada pelo Glue:\n```text\n{error_message}\n```\n"

        section_5_title, section_5_content = _build_layer_summary(layer, manifest_data)

        today_date = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )

        report_content = f"""#  Relatório de Execução do Job: `{job_name}`

## 1.  Informações Gerais e Contexto
* **Data/Hora UTC:** {completed_str}
* **Ambiente AWS:** Account ID `{AWS_ACCOUNT_ID or 'N/A'}` | Região `{AWS_REGION}`
* **AWS Glue Job:** `{job_name}`
* **JobRunId:** `{run_id}`
* **Status Final:** `{status}`
* **Bucket S3 Alvo:** `s3://{FULL_BUCKET_NAME}`
* **Manifesto de Auditoria S3:** `{manifest_uri}`

---

## 2. Métricas de execução e custo (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `{status}` |
| **Duração Total** | **{execution_time} segundos** (~{execution_time/60.0:.2f} min) |
| **DPU-Horas Medidas** | **{dpu_hours:.4f} DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ {cost_usd:.4f}** |
| **Início / Término** | {started_str} ➔ {completed_str} |

---

## 3.  Consumo e Origem dos Dados
* **Camada de Origem:** `{origin_desc}`
* **Custos de serviços de origem:** Não medidos por este relatório.
* **Rastreabilidade:** Manifestos JSON registram os dados de execução.

---

## 4.  Saída da Aplicação (CloudWatch Stdout)
{error_section}```text
{logs_text}
```

---

{section_5_title}
{section_5_content}

---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
"""

        os.makedirs(output_dir, exist_ok=True)
        filename = f"report_{today_date}_{job_name}.md"
        file_path = os.path.join(output_dir, filename)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(report_content)

        print(f"  Relatório salvo em: {file_path}")
        return file_path

    except Exception as e:
        print(f" Falha ao gerar relatório para o Job {job_name}: {str(e)}")
        return ""


if __name__ == "__main__":
    if len(sys.argv) > 2:
        generate_job_report(sys.argv[1], sys.argv[2])
    else:
        print("Uso: python report_generator.py <JOB_NAME> <RUN_ID>")
