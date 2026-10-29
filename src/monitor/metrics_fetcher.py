"""
===============================================================================
COLETOR DE MÉTRICAS E FINOPS (monitor/metrics_fetcher.py)
===============================================================================
Descrição: Script executado via CLI no terminal local que consulta a API do AWS
Glue e CloudWatch para extrair a duração real, DPU-horas e estimativa de custo
das execuções para compor a tabela de FinOps do README.
===============================================================================
"""

import os
import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
COST_PER_DPU_HOUR = 0.44  # Custo médio da AWS por DPU-Hora no AWS Glue

glue_client = boto3.client("glue", region_name=AWS_REGION)

JOBS = ["01_bronze_ingestion", "02_silver_processing", "03_gold_aggregation"]


def fetch_job_metrics():
    print("=" * 60)
    print("📊 RELATÓRIO DE MÉTRICAS REAIS DE EXECUÇÃO E FINOPS (AWS GLUE)")
    print("=" * 60)

    total_cost_usd = 0.0

    for job_name in JOBS:
        try:
            runs = glue_client.get_job_runs(JobName=job_name, MaxResults=1)["JobRuns"]
            if not runs:
                print(f"⚠️ Nenhuma execução encontrada para {job_name}")
                continue

            last_run = runs[0]
            status = last_run["JobRunState"]
            execution_time_sec = last_run.get("ExecutionTime", 0)
            dpu_seconds = last_run.get("DPUSeconds", 0)

            # Cálculo de Custo FinOps
            dpu_hours = dpu_seconds / 3600.0
            cost_usd = dpu_hours * COST_PER_DPU_HOUR
            total_cost_usd += cost_usd

            print(f"🔹 Job: {job_name}")
            print(f"   ├─ Status: {status}")
            print(f"   ├─ Duração: {execution_time_sec} segundos")
            print(f"   ├─ DPU-Horas Medidas: {dpu_hours:.4f} DPU-h")
            print(f"   └─ Custo Estimado: ${cost_usd:.4f} USD")
            print("-" * 60)

        except Exception as e:
            print(f"❌ Falha ao buscar métricas do Job {job_name}: {str(e)}")

    print(f"\n💰 CUSTO TOTAL ESTIMADO DA PIPELINE: ${total_cost_usd:.4f} USD")
    print("=" * 60)


if __name__ == "__main__":
    fetch_job_metrics()
