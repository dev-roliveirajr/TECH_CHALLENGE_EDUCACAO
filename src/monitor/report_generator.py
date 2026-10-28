"""
===============================================================================
GERADOR AUTOMÁTICO DE RELATÓRIOS E FINOPS (src/monitor/report_generator.py) - V6
===============================================================================
Descrição: Módulo chamado pelo orquestrador (pipeline.py) ao final de cada Job.
Novidades V6:
  - Garantia da instrução load_dotenv(override=True).
  - Resolução inteligente de chaves do Manifesto no S3 (busca por RunId e fallback por data).
  - Preenchimento garantido da Seção 5 (Comparativo BigQuery vs S3 Bronze).
===============================================================================
"""

import os
import sys
import datetime
import json
import boto3
from dotenv import load_dotenv

# Garantia de carregamento com override para ambientes de laboratório
load_dotenv(override=True)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "tc-alfabetizacao")

# Nome resolvido do Bucket
if AWS_ACCOUNT_ID and AWS_ACCOUNT_ID not in S3_BUCKET_NAME:
    FULL_BUCKET_NAME = f"{S3_BUCKET_NAME}-{AWS_ACCOUNT_ID}-{AWS_REGION}"
else:
    FULL_BUCKET_NAME = S3_BUCKET_NAME

COST_PER_DPU_HOUR = 0.44  # Valor padrão AWS Glue G.1X DPU-Hora em USD

def fetch_stdout_cloudwatch_logs(log_stream_name: str) -> str:
    """Busca o log de saída de aplicação (stdout) diretamente do Log Group /aws-glue/jobs/output."""
    if not log_stream_name:
        return "ℹ️ LogStream de execução não disponibilizado."

    logs_client = boto3.client("logs", region_name=AWS_REGION)
    log_group = "/aws-glue/jobs/output"
    
    try:
        response = logs_client.get_log_events(
            logGroupName=log_group,
            logStreamName=log_stream_name,
            startFromHead=True,
            limit=200
        )
        events = response.get("events", [])
        if not events:
            return "ℹ️ Nenhum log registrado no LogStream do CloudWatch."
            
        filtered_lines = []
        noise_indicators = ["FutureWarning", "google.api_core", "WARN NativeCodeLoader"]
        
        for ev in events:
            msg = ev["message"].strip()
            if any(noise in msg for noise in noise_indicators):
                continue
            timestamp_str = datetime.datetime.fromtimestamp(ev["timestamp"] / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
            filtered_lines.append(f"[{timestamp_str}] {msg}")
            
        return "\n".join(filtered_lines) if filtered_lines else "✅ Execução concluída sem mensagens relevantes adicionais."
    except Exception as e:
        return f"ℹ️ Log de execução disponível no console CloudWatch (Grupo: {log_group}, Stream: {log_stream_name}). Msg: {str(e)}"

def fetch_s3_manifest(run_id: str, ingestion_date_str: str) -> tuple[dict, str]:
    """
    Busca o manifesto de auditoria no S3 para incorporar os detalhes no relatório.
    Usa tentativa por RunId exato e fallback resiliente listando os manifestos da data.
    """
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    exact_key = f"auditoria/bronze/ingestion_date={ingestion_date_str}/run_id={run_id}/manifest.json"
    
    try:
        resp = s3_client.get_object(Bucket=FULL_BUCKET_NAME, Key=exact_key)
        content = resp["Body"].read().decode("utf-8")
        return json.loads(content), f"s3://{FULL_BUCKET_NAME}/{exact_key}"
    except Exception:
        pass

    # Fallback: Busca o manifesto mais recente gravado no prefixo do dia
    try:
        prefix = f"auditoria/bronze/ingestion_date={ingestion_date_str}/"
        res = s3_client.list_objects_v2(Bucket=FULL_BUCKET_NAME, Prefix=prefix)
        contents = res.get("Contents", [])
        manifest_objs = [obj for obj in contents if obj["Key"].endswith("manifest.json")]
        if manifest_objs:
            # Ordena pelo horário de modificação mais recente
            manifest_objs.sort(key=lambda x: x["LastModified"], reverse=True)
            latest_key = manifest_objs[0]["Key"]
            resp = s3_client.get_object(Bucket=FULL_BUCKET_NAME, Key=latest_key)
            content = resp["Body"].read().decode("utf-8")
            return json.loads(content), f"s3://{FULL_BUCKET_NAME}/{latest_key}"
    except Exception:
        pass

    return None, f"s3://{FULL_BUCKET_NAME}/{exact_key}"

def generate_job_report(job_name: str, run_id: str, output_dir: str = "reports/job_executions") -> str:
    """Consulta o Glue, S3 e CloudWatch usando JobName e RunId e gera o relatório Markdown completo."""
    print(f"📊 [FinOps & Observabilidade] Gerando relatório consolidado para {job_name} (RunId: {run_id[:8]})...")
    
    glue_client = boto3.client("glue", region_name=AWS_REGION)
    
    try:
        job_run_resp = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        job_run = job_run_resp.get("JobRun", {})
        
        status = job_run.get("JobRunState", "UNKNOWN")
        execution_time = job_run.get("ExecutionTime", 0)
        dpu_seconds = job_run.get("DPUSeconds", 0)
        error_message = job_run.get("ErrorMessage", "")
        
        started_on = job_run.get("StartedOn")
        started_str = started_on.strftime("%Y-%m-%d %H:%M:%S UTC") if started_on else "N/A"
        ingestion_date_str = started_on.strftime("%Y-%m-%d") if started_on else datetime.datetime.utcnow().strftime("%Y-%m-%d")
        
        completed_on = job_run.get("CompletedOn")
        completed_str = completed_on.strftime("%Y-%m-%d %H:%M:%S UTC") if completed_on else "N/A"
        
        # Cálculo FinOps
        dpu_hours = dpu_seconds / 3600.0 if dpu_seconds else (execution_time / 3600.0) * 2.0
        cost_usd = dpu_hours * COST_PER_DPU_HOUR
        
        # Coleta de Logs do CloudWatch (/aws-glue/jobs/output)
        logs_text = fetch_stdout_cloudwatch_logs(run_id)
        
        # Busca o Manifesto no S3 com busca resiliente
        manifest_data, manifest_uri = fetch_s3_manifest(run_id, ingestion_date_str)
        
        error_section = ""
        if error_message:
            error_section = f"\n### 🚨 Mensagem de Erro Registrada pelo Glue:\n```text\n{error_message}\n```\n"

        # Montagem da Tabela Comparativa (Origem BigQuery vs. Destino S3)
        comparison_rows = []
        if manifest_data and "tables_metrics" in manifest_data:
            for item in manifest_data["tables_metrics"]:
                ent = item.get("entity", "N/A")
                bq_rows = item.get("bq_rows", 0)
                s3_rows = item.get("s3_rows", 0)
                st = item.get("status", "N/A")
                parts = item.get("partitions", 0)
                size_mb = item.get("size_mb", 0.0)
                
                bq_str = f"{bq_rows:,}" if bq_rows >= 0 else "N/A"
                if "SKIPPED" in st:
                    s3_str = "0 (Already Up-to-Date)"
                else:
                    s3_str = f"{s3_rows:,}" if s3_rows >= 0 else "OK"
                
                comparison_rows.append(
                    f"| `{ent}` | {bq_str} | {s3_str} | `{st}` | {parts} part. ({size_mb} MB) |"
                )
        else:
            comparison_rows.append("| *N/A* | Dados indisponíveis no manifesto | N/A | N/A | N/A |")

        comparison_table_md = "\n".join(comparison_rows)
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

## 3. 🌐 Consumo e Origem GCP BigQuery
* **Dataset Origem:** `basedosdados.br_inep_avaliacao_alfabetizacao` (7 Entidades Oficiais)
* **Custo de Leitura BigQuery (On-Demand):** **US$ 0,00** *(Até 1 TB/mês no Free Tier)*
* **Estratégia de Segurança:** Credenciais recuperadas 100% em memória RAM via AWS Secrets Manager (`pipeline-educacao/gcp/bigquery`). Zero arquivos em disco (`/tmp`).

---

## 4. 📜 Saída da Aplicação (CloudWatch Stdout)
{error_section}```text
{logs_text}
```

---

## 5. ⚖️ Comparativo de Contagem: Origem (BigQuery) vs. Destino (S3 Bronze)
| Entidade | Linhas no BigQuery (Metadados) | Linhas Gravadas no S3 | Status Ingestão | Particionamento & Tamanho |
| :--- | :---: | :---: | :---: | :---: |
{comparison_table_md}

---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
"""

        os.makedirs(output_dir, exist_ok=True)
        filename = f"report_{job_name}_{today_date}.md"
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
