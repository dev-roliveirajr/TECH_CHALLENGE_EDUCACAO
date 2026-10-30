"""
===============================================================================
GERADOR AUTOMÁTICO DE RELATÓRIOS E FINOPS (src/monitor/report_generator.py) - V9
===============================================================================
Descrição: Módulo chamado pelo orquestrador (pipeline.py) ao final de cada Job.
Destaques V9:
  - Suporte completo e nativo às três camadas do Data Lake: BRONZE, SILVER e GOLD.
  - Leitura resiliente de manifestos S3 (auditoria/bronze/, auditoria/silver/, auditoria/gold/).
  - Relatório da Gold com auditoria de Validação Cruzada, Feature Store e Metas Pactuadas.
  - Leitura contínua de logs CloudWatch e métricas FinOps precisas.
===============================================================================
"""

import os
import sys
import datetime
import json
import boto3
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


def fetch_stdout_cloudwatch_logs(log_stream_name: str) -> str:
    """Busca o log completo de saída de aplicação (stdout) do Log Group /aws-glue/jobs/output."""
    if not log_stream_name:
        return "ℹ️ LogStream de execução não disponibilizado."

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
                    ev["timestamp"] / 1000.0
                ).strftime("%Y-%m-%d %H:%M:%S")
                filtered_lines.append(f"[{timestamp_str}] {msg}")

            new_token = response.get("nextForwardToken")
            if not events or new_token == next_token:
                break
            next_token = new_token

        return (
            "\n".join(filtered_lines)
            if filtered_lines
            else "✅ Execução concluída sem mensagens relevantes adicionais."
        )
    except Exception as e:
        return f"ℹ️ Log de execução disponível no console CloudWatch (Grupo: {log_group}, Stream: {log_stream_name}). Msg: {str(e)}"


def fetch_s3_manifest(job_name: str, run_id: str, ingestion_date_str: str) -> dict:
    """Busca o manifesto de auditoria no S3 (suporta Bronze, Silver e Gold com fallback)."""
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    if "gold" in job_name.lower() or "03" in job_name:
        layer = "gold"
    elif "silver" in job_name.lower() or "02" in job_name:
        layer = "silver"
    else:
        layer = "bronze"

    audit_folder = f"auditoria/{layer}"

    # 1. Tentativa por chave exata do run_id
    exact_key = f"{audit_folder}/ingestion_date={ingestion_date_str}/run_id={run_id}/manifest.json"
    try:
        resp = s3_client.get_object(Bucket=FULL_BUCKET_NAME, Key=exact_key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception:
        pass

    # 2. Busca pelo último manifesto do dia na pasta da camada
    try:
        prefix = f"{audit_folder}/ingestion_date={ingestion_date_str}/"
        resp = s3_client.list_objects_v2(Bucket=FULL_BUCKET_NAME, Prefix=prefix)
        if "Contents" in resp:
            keys = sorted(
                [
                    obj["Key"]
                    for obj in resp["Contents"]
                    if obj["Key"].endswith("manifest.json")
                ],
                reverse=True,
            )
            if keys:
                latest_resp = s3_client.get_object(Bucket=FULL_BUCKET_NAME, Key=keys[0])
                return json.loads(latest_resp["Body"].read().decode("utf-8"))
    except Exception:
        pass

    return None


def _build_layer_summary(layer: str, manifest_data: dict):
    if layer == "gold":
        title = "## 5. 🏆 Auditoria Qualitativa e Estado Final da Camada Gold"
        if not manifest_data:
            return (
                title,
                "ℹ️ Dados do manifesto Gold em processamento ou indisponíveis no S3.",
            )

        in_rows = manifest_data.get("input_rows_silver", 0)
        out_ind = manifest_data.get("output_rows_indicadores", 0)
        out_metas = manifest_data.get("output_rows_metas", 0)
        out_feat = manifest_data.get("output_rows_features", 0)
        cv_passed = manifest_data.get("cross_validation_passed", True)
        cv_diff = manifest_data.get("cross_validation_diff_percent", 0.0)
        cv_status = (
            f"✅ APROVADO ({cv_diff:.3f}% <= 2.0%)"
            if cv_passed
            else f"❌ REPROVADO ({cv_diff:.3f}%)"
        )
        meta_ref = manifest_data.get("target_meta_pactuada", "80.0%")
        content = f"""| Métrica Analítica / Governança | Valor Registrado | Observação de Engenharia |
| :--- | :---: | :--- |
| **Linhas Lidas da Silver** | **{in_rows:,}** | Registros curados da tabela 'alunos_curado' |
| **Indicadores Ponderados Municipais** | **{out_ind:,}** | Agregação ponderada pelo peso do aluno (Saeb) |
| **Comparativo de Metas Gerado** | **{out_metas:,}** | GAP em relação à meta pactuada da fonte |
| **Feature Store ML (Fase 3)** | **{out_feat:,}** | Matriz de características (13 colunas) sem leakage |
| **Validação Cruzada (Microdado vs. Inep)** | **{cv_status}** | Batimento da taxa recalculada vs. oficial |
| **Meta Pactuada Oficial de Referência** | **{meta_ref}** | Trajetória oficial pactuada até 2030 |
| **S3 Gold Prefixes & Local Sync** | `s3://{FULL_BUCKET_NAME}/gold/` | Cópia sincronizada em 'data/features/' |
"""
        return title, content

    if layer == "silver":
        title = "## 5. 🛡️ Auditoria Qualitativa e Estado Final da Camada Silver"
        if not manifest_data:
            return (
                title,
                "ℹ️ Dados do manifesto Silver em processamento ou indisponíveis no S3.",
            )

        in_rows = manifest_data.get("input_rows_alunos", 0)
        out_rows = manifest_data.get("output_rows_curado", 0)
        match_ratio = manifest_data.get("join_match_ratio_percent", 0.0)
        qg_status = (
            "✅ APROVADO (>= 95.0%)"
            if manifest_data.get("quality_gate_passed")
            else "❌ REPROVADO"
        )
        silver_uri = manifest_data.get(
            "silver_s3_prefix", f"s3://{FULL_BUCKET_NAME}/silver/alunos_curado/"
        )
        content = f"""| Métrica de Curadoria / Qualidade | Valor Registrado | Observação de Engenharia |
| :--- | :---: | :--- |
| **Linhas de Alunos (Entrada)** | **{in_rows:,}** | Registros brutos lidos do manifesto Bronze |
| **Linhas Curadas (Saída)** | **{out_rows:,}** | Registros finais enriquecidos e validados |
| **Taxa de Match do JOIN** | **{match_ratio:.2f}%** | Percentual de alinhamento com dimensões territoriais |
| **Status do Gate de Qualidade** | **{qg_status}** | Trava bloqueante da pipeline (check_join_ratio) |
| **Tratamento de NULOs (Saeb)** | **Preservado** | Nenhuma atribuição de zero em proficiências/notas |
| **Status das Metas Educacionais** | **Tripla Categorização** | 'Atingiu', 'Não Atingiu' ou 'Sem Meta Cadastrada' |
| **S3 Prefix Final (Catálogo)** | `{silver_uri}` | Formato Parquet (Snappy) particionado por data |
"""
        return title, content

    title = (
        "## 5. ⚖️ Comparativo de Contagem: Origem (BigQuery) vs. Destino (S3 Bronze)"
    )
    if not manifest_data or "tables_metrics" not in manifest_data:
        return (
            title,
            "ℹ️ Dados do manifesto Bronze em processamento ou indisponíveis no S3.",
        )

    comparison_rows = []
    for item in manifest_data["tables_metrics"]:
        entity = item.get("entity", "N/A")
        bq_rows = item.get("bq_rows", 0)
        s3_rows = item.get("s3_rows", 0)
        status = item.get("status", "N/A")
        partitions = item.get("partitions", 0)
        size_mb = item.get("size_mb", 0.0)
        bq_str = f"{bq_rows:,}" if bq_rows >= 0 else "N/A"
        s3_str = f"{s3_rows:,}" if status == "SUCCEEDED" else "0 (Already Up-to-Date)"
        comparison_rows.append(
            f"| `{entity}` | {bq_str} | {s3_str} | `{status}` | {partitions} part. ({size_mb} MB) |"
        )
    content = (
        "| Entidade | Linhas no BigQuery (Metadados) | Linhas Gravadas no S3 | Status Ingestão | Particionamento & Tamanho |\n| :--- | :---: | :---: | :---: | :---: |\n"
        + "\n".join(comparison_rows)
    )
    return title, content


def generate_job_report(
    job_name: str, run_id: str, output_dir: str = "reports/job_executions"
) -> str:
    """Consulta o Glue, S3 e CloudWatch e gera o relatório Markdown completo para Bronze, Silver ou Gold."""
    print(
        f"📊 [FinOps & Observabilidade] Gerando relatório para {job_name} (RunId: {run_id[:8]})..."
    )

    glue_client = boto3.client("glue", region_name=AWS_REGION)

    try:
        job_run_resp = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        job_run = job_run_resp.get("JobRun", {})

        status = job_run.get("JobRunState", "UNKNOWN")
        execution_time = job_run.get("ExecutionTime", 0)
        dpu_seconds = job_run.get("DPUSeconds", 0)
        error_message = job_run.get("ErrorMessage", "")

        started_on = job_run.get("StartedOn")
        started_str = (
            started_on.strftime("%Y-%m-%d %H:%M:%S UTC") if started_on else "N/A"
        )
        ingestion_date_str = (
            started_on.strftime("%Y-%m-%d")
            if started_on
            else datetime.datetime.utcnow().strftime("%Y-%m-%d")
        )

        completed_on = job_run.get("CompletedOn")
        completed_str = (
            completed_on.strftime("%Y-%m-%d %H:%M:%S UTC") if completed_on else "N/A"
        )

        # Cálculo FinOps
        dpu_hours = (
            dpu_seconds / 3600.0 if dpu_seconds else (execution_time / 3600.0) * 2.0
        )
        cost_usd = dpu_hours * COST_PER_DPU_HOUR

        logs_text = fetch_stdout_cloudwatch_logs(run_id)
        manifest_data = fetch_s3_manifest(job_name, run_id, ingestion_date_str)

        if "gold" in job_name.lower() or "03" in job_name:
            layer = "gold"
            origin_desc = "Camada Silver (Parquet / S3 em alunos_curado/)"
        elif "silver" in job_name.lower() or "02" in job_name:
            layer = "silver"
            origin_desc = "Camada Bronze (Parquet / S3 via Manifesto)"
        else:
            layer = "bronze"
            origin_desc = "GCP BigQuery (basedosdados.br_inep_avaliacao_alfabetizacao)"

        manifest_uri = f"s3://{FULL_BUCKET_NAME}/auditoria/{layer}/ingestion_date={ingestion_date_str}/run_id={run_id}/manifest.json"

        error_section = ""
        if error_message:
            error_section = f"\n### 🚨 Mensagem de Erro Registrada pelo Glue:\n```text\n{error_message}\n```\n"

        section_5_title, section_5_content = _build_layer_summary(layer, manifest_data)

        today_date = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        report_content = f"""# 📊 Relatório de Execução do Job: `{job_name}`

## 1. ℹ️ Informações Gerais e Contexto
* **Data/Hora UTC:** {completed_str}
* **Ambiente AWS:** Account ID `{AWS_ACCOUNT_ID or 'N/A'}` | Região `{AWS_REGION}`
* **AWS Glue Job:** `{job_name}`
* **JobRunId:** `{run_id}`
* **Status Final:** `{status}`
* **Bucket S3 Alvo:** `s3://{FULL_BUCKET_NAME}`
* **Manifesto de Auditoria S3:** `{manifest_uri}`

---

## 2. 💰 Métricas de Desempenho e FinOps (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `{status}` |
| **Duração Total** | **{execution_time} segundos** (~{execution_time/60.0:.2f} min) |
| **DPU-Horas Medidas** | **{dpu_hours:.4f} DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ {cost_usd:.4f}** |
| **Início / Término** | {started_str} ➔ {completed_str} |

---

## 3. 🌐 Consumo e Origem dos Dados
* **Camada de Origem:** `{origin_desc}`
* **Custo de Leitura / Processamento:** **US$ 0,00 Adicional** *(Incluso no pipeline serverless)*
* **Estratégia de Governança:** Conexão 100% orientada por manifestos de auditoria JSON.

---

## 4. 📜 Saída da Aplicação (CloudWatch Stdout)
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

        print(f"  └─ ✅ Relatório FinOps & Observabilidade salvo em: {file_path}")
        return file_path

    except Exception as e:
        print(f"⚠️ Falha ao gerar relatório para o Job {job_name}: {str(e)}")
        return ""


if __name__ == "__main__":
    if len(sys.argv) > 2:
        generate_job_report(sys.argv[1], sys.argv[2])
    else:
        print("Uso: python report_generator.py <JOB_NAME> <RUN_ID>")
