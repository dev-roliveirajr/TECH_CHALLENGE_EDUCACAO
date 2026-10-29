"""
===============================================================================
ORQUESTRADOR LOCAL DE JOBS AWS GLUE (main.py)
===============================================================================
Descrição: Script executado via CLI no terminal local que atua como plano de
controle. Dispara os Jobs no AWS Glue usando boto3, monitora o status via
polling e aplica travas de fallback (interrompe o fluxo em caso de falha).
===============================================================================
"""

import os
import sys
import time
import shutil
import tempfile
import datetime
import boto3
import pandas as pd
from dotenv import load_dotenv
from monitor.report_generator import generate_job_report

# Carrega variáveis de ambiente do .env
load_dotenv(override=True)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

FULL_BUCKET_NAME = f"{S3_BUCKET_NAME}-{AWS_ACCOUNT_ID}-{AWS_REGION}"
POLLING_INTERVAL_SECONDS = int(os.getenv("GLUE_POLLING_INTERVAL", "10"))


def get_glue_client():
    """Inicializa o cliente boto3 do AWS Glue."""
    return boto3.client("glue", region_name=AWS_REGION)


def get_logs_client():
    """Inicializa o cliente boto3 do AWS CloudWatch Logs."""
    return boto3.client("logs", region_name=AWS_REGION)


def _print_job_log_events(events, seen_event_ids, noise_indicators):
    for event in events:
        event_id = event.get("eventId") or (event["timestamp"], event["message"])
        if event_id in seen_event_ids:
            continue
        seen_event_ids.add(event_id)

        message = event["message"].strip()
        if any(noise in message for noise in noise_indicators):
            continue

        timestamp = datetime.datetime.fromtimestamp(
            event["timestamp"] / 1000.0
        ).strftime("%H:%M:%S")
        print(f"  [{timestamp}] {message}")


def _fetch_and_print_job_logs(
    logs_client, run_id, next_token, seen_event_ids, noise_indicators
):
    kwargs = {"logGroupName": "/aws-glue/jobs/output", "logStreamName": run_id}
    if next_token:
        kwargs["nextToken"] = next_token
    else:
        kwargs["startFromHead"] = True

    log_response = logs_client.get_log_events(**kwargs)
    _print_job_log_events(
        log_response.get("events", []), seen_event_ids, noise_indicators
    )
    return log_response.get("nextForwardToken")


def get_s3_client():
    """Inicializa o cliente boto3 do AWS S3."""
    return boto3.client("s3", region_name=AWS_REGION)


def sync_gold_features_locally():
    """
    Sincroniza e consolida os datasets Parquet da Camada Gold do S3 para a pasta local
    'data/features/', unificando as partições dispersas em arquivos .parquet únicos:
      - data/features/gold_features_alfabetizacao.parquet
      - data/features/gold_indicadores_municipais.parquet
      - data/features/gold_desempenho_vs_metas.parquet
    """
    print("\n--------------------------------------------------")
    print(
        "🔄 Sincronizando e consolidando datasets Gold localmente em 'data/features/'..."
    )
    print("--------------------------------------------------")

    s3_client = get_s3_client()
    local_output_dir = os.path.join(os.getcwd(), "data", "features")
    os.makedirs(local_output_dir, exist_ok=True)

    gold_datasets = {
        "gold_features_alfabetizacao": "gold/gold_features_alfabetizacao/",
        "gold_indicadores_municipais": "gold/gold_indicadores_municipais/",
        "gold_desempenho_vs_metas": "gold/gold_desempenho_vs_metas/",
    }

    for dataset_name, s3_prefix in gold_datasets.items():
        temp_dir = tempfile.mkdtemp()
        try:
            resp = s3_client.list_objects_v2(Bucket=FULL_BUCKET_NAME, Prefix=s3_prefix)
            if "Contents" not in resp:
                print(f"⚠️ Aviso: Prefixo {s3_prefix} não encontrado no S3.")
                continue

            parquet_keys = [
                obj["Key"]
                for obj in resp["Contents"]
                if obj["Key"].endswith(".parquet")
                and not obj["Key"].split("/")[-1].startswith(".")
            ]

            if not parquet_keys:
                print(f"⚠️ Aviso: Nenhum arquivo Parquet encontrado em {s3_prefix}.")
                continue

            # Baixa as partes do S3 para o diretório temporário
            downloaded_files = []
            for i, key in enumerate(parquet_keys):
                local_tmp_file = os.path.join(temp_dir, f"part_{i}.parquet")
                s3_client.download_file(FULL_BUCKET_NAME, key, local_tmp_file)
                downloaded_files.append(local_tmp_file)

            # Consolidando arquivos usando Pandas
            dfs = [pd.read_parquet(f) for f in downloaded_files]
            df_consolidated = pd.concat(dfs, ignore_index=True)

            # Salvando como arquivo único .parquet
            target_file_path = os.path.join(local_output_dir, f"{dataset_name}.parquet")
            df_consolidated.to_parquet(target_file_path, index=False)
            print(
                f"  └─ ✅ Dataset consolidado salvo com sucesso: {target_file_path} ({len(df_consolidated):,} linhas)"
            )

        except Exception as e:
            print(f"⚠️ Alerta: Falha ao sincronizar {dataset_name}: {str(e)}")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    print("--------------------------------------------------\n")


def _complete_glue_job(
    job_name,
    run_id,
    job_run,
    status,
    logs_client,
    next_token,
    seen_event_ids,
    noise_indicators,
):
    try:
        _fetch_and_print_job_logs(
            logs_client, run_id, next_token, seen_event_ids, noise_indicators
        )
    except Exception:
        pass

    execution_time = job_run.get("ExecutionTime", 0)
    dpu_seconds = job_run.get("DPUSeconds", 0)

    print("\n--------------------------------------------------")
    if status == "SUCCEEDED":
        print(f"✅ Job {job_name} finalizou com SUCESSO!")
    else:
        error_message = job_run.get("ErrorMessage", "Sem detalhes adicionais.")
        print(f"❌ ERRO CRÍTICO: Job {job_name} terminou com status: {status}")
        print(f"   Detalhes: {error_message}")

    print(f"📊 Duração: {execution_time}s | DPU-Segundos: {dpu_seconds}")
    print("--------------------------------------------------")

    if generate_job_report:
        try:
            report_path = generate_job_report(job_name, run_id)
            print(f"📄 Relatório FinOps gerado com sucesso: {report_path}\n")
        except Exception as rep_err:
            print(
                f"⚠️ Alerta: Falha ao gerar relatório para {job_name}: {str(rep_err)}\n"
            )

    return status


def run_glue_job(job_name: str) -> str:
    """
    Dispara a execução de um AWS Glue Job e aguarda até a finalização.

    Retorna:
        status (str): 'SUCCEEDED', 'FAILED', 'STOPPED' ou 'TIMEOUT'
    """
    glue_client = get_glue_client()
    logs_client = get_logs_client()

    print(f"\n🚀 Disparando AWS Glue Job: {job_name}")

    try:
        response = glue_client.start_job_run(JobName=job_name)
        run_id = response["JobRunId"]
        print(f"📌 JobRunId gerado: {run_id}")
        print(
            "📡 Acompanhando logs em tempo real (CloudWatch /aws-glue/jobs/output):\n"
        )

        next_token = None
        seen_event_ids = set()
        noise_indicators = ["FutureWarning", "google.api_core", "WARN NativeCodeLoader"]

        while True:
            # 1. Tenta capturar e imprimir novos logs do CloudWatch em tempo real
            try:
                next_token = _fetch_and_print_job_logs(
                    logs_client,
                    run_id,
                    next_token,
                    seen_event_ids,
                    noise_indicators,
                )
            except Exception:
                # O LogStream pode levar alguns segundos para ser criado na AWS ao iniciar o Job
                pass

            # 2. Checa o status do Job na API do Glue
            job_status_resp = glue_client.get_job_run(JobName=job_name, RunId=run_id)
            job_run = job_status_resp["JobRun"]
            status = job_run["JobRunState"]

            if status in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"]:
                return _complete_glue_job(
                    job_name,
                    run_id,
                    job_run,
                    status,
                    logs_client,
                    next_token,
                    seen_event_ids,
                    noise_indicators,
                )

            time.sleep(POLLING_INTERVAL_SECONDS)

    except Exception as e:
        print(f"❌ Falha ao tentar disparar o Glue Job {job_name}: {str(e)}")
        return "FAILED"


def main():
    """Pipeline de Orquestração Sequencial das Camadas Medalhão."""
    print("=" * 60)
    print("🚀 INICIANDO PIPELINE DE ORQUESTRACAO...")
    print("=" * 60)

    # 1. Execução da Camada Bronze
    print("\n" + "=" * 60)
    print("🔄 Inicio da Ingestão de dados para a camada Bronze...")
    print("=" * 60)
    status_bronze = run_glue_job("01_bronze_ingestion")
    if status_bronze != "SUCCEEDED":
        print(
            "\n🛑 PARADA DE EMERGÊNCIA: Camada Bronze falhou. Interrompendo pipeline."
        )
        sys.exit(1)

    # 2. Execução da Camada Silver (Com validações de qualidade e JOINs)
    print("\n" + "=" * 60)
    print("🔄 Inicio da Qualificação de dados para a camada Silver...")
    print("=" * 60)
    status_silver = run_glue_job("02_silver_processing")
    if status_silver != "SUCCEEDED":
        print(
            "\n🛑 PARADA DE EMERGÊNCIA: Camada Silver falhou (validação de qualidade/JOINs). Interrompendo pipeline."
        )
        sys.exit(1)

    # 3. Execução da Camada Gold (KPIs e Feature Store para IA)
    print("\n" + "=" * 60)
    print("🔄 Inicio da Agregação de dados para a camada Gold...")
    print("=" * 60)
    status_gold = run_glue_job("03_gold_aggregation")
    if status_gold != "SUCCEEDED":
        print("\n🛑 ALERTA: Camada Gold falhou.")
        sys.exit(1)

    # 4. Sincronização e consolidação local dos datasets da Gold em 'data/features/'
    sync_gold_features_locally()

    print("\n" + "=" * 60)
    print("🎉 PIPELINE EXECUTADA COM SUCESSO EM TODAS AS CAMADAS!")
    print("=" * 60)


if __name__ == "__main__":
    main()
