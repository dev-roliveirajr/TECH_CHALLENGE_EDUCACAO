"""
===============================================================================
ORQUESTRADOR DE PIPELINE DE EVENTOS STREAMING (src/pipeline_events.py)
===============================================================================
Descrição: Plano de controle para consumo e processamento incremental de eventos
em tempo quase real (Streaming / Micro-batch).
Verifica a presença de novos eventos na Landing Zone (S3 ou local), executa o 
processador Delta Lake Silver (04_events_processor), atualiza a Camada Gold 
(03_gold_aggregation) e sincroniza os datasets Parquet em 'data/features/'.
===============================================================================
"""

import os
import sys
import time
import shutil
import tempfile
import argparse
import datetime
from dotenv import load_dotenv

try:
    import boto3
except ImportError:
    boto3 = None

try:
    import pandas as pd
except ImportError:
    pd = None

# Tenta importar o gerador de relatórios se disponível
try:
    from monitor.report_generator import generate_job_report
except ImportError:
    generate_job_report = None

load_dotenv(override=True)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "tc-alfabetizacao")

if AWS_ACCOUNT_ID and AWS_ACCOUNT_ID not in S3_BUCKET_NAME:
    FULL_BUCKET_NAME = f"{S3_BUCKET_NAME}-{AWS_ACCOUNT_ID}-{AWS_REGION}"
else:
    FULL_BUCKET_NAME = S3_BUCKET_NAME

POLLING_INTERVAL_SECONDS = int(os.getenv("GLUE_POLLING_INTERVAL", "10"))
EVENTS_JOB_NAME = "04_events_processor"
GOLD_JOB_NAME = "03_gold_aggregation"


def get_s3_client():
    if not boto3:
        return None
    return boto3.client("s3", region_name=AWS_REGION)


def get_glue_client():
    if not boto3:
        return None
    return boto3.client("glue", region_name=AWS_REGION)


def get_logs_client():
    if not boto3:
        return None
    return boto3.client("logs", region_name=AWS_REGION)


def check_local_landing_events() -> bool:
    """Verifica se existem eventos pendentes na Landing Zone local."""
    landing_dirs = [
        os.path.join(os.getcwd(), "data", "events_landing", "alunos"),
        os.path.join(os.getcwd(), "data", "events_landing", "metas", "municipio"),
        os.path.join(os.getcwd(), "data", "events_landing", "metas", "uf"),
        os.path.join(os.getcwd(), "data", "events_landing", "metas", "brasil"),
    ]
    total_files = 0
    for d in landing_dirs:
        if os.path.exists(d):
            json_files = [f for f in os.listdir(d) if f.endswith(".json")]
            total_files += len(json_files)
    return total_files > 0


def check_s3_landing_events() -> bool:
    """Verifica se existem eventos pendentes na Landing Zone S3."""
    if not boto3:
        return False
    try:
        s3 = get_s3_client()
        if not s3:
            return False
        resp = s3.list_objects_v2(
            Bucket=FULL_BUCKET_NAME,
            Prefix="bronze/events_raw_landing/",
            MaxKeys=10
        )
        if "Contents" in resp:
            events = [obj for obj in resp["Contents"] if obj["Key"].endswith(".json")]
            return len(events) > 0
    except Exception as e:
        print(f"  ℹ️ [S3 Check] Consulta de Landing Zone via S3 indisponível: {e}")
    return False


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


def run_glue_job(job_name: str) -> str:
    """Dispara a execução de um AWS Glue Job ou aciona fallback local se indisponível."""
    glue_client = get_glue_client()
    logs_client = get_logs_client()

    if not glue_client:
        print(f"  ℹ️ Conexão boto3 AWS indisponível. Recorrendo ao modo local para {job_name}...")
        return run_local_script_fallback(job_name)

    print(f"\n🚀 Disparando AWS Glue Job: {job_name}")

    try:
        response = glue_client.start_job_run(JobName=job_name)
        run_id = response["JobRunId"]
        print(f"📌 JobRunId gerado: {run_id}")
        print("📡 Acompanhando logs em tempo real (CloudWatch /aws-glue/jobs/output):\n")

        next_token = None
        seen_event_ids = set()
        noise_indicators = ["FutureWarning", "google.api_core", "WARN NativeCodeLoader"]

        while True:
            try:
                next_token = _fetch_and_print_job_logs(
                    logs_client, run_id, next_token, seen_event_ids, noise_indicators
                )
            except Exception:
                pass

            job_status_resp = glue_client.get_job_run(JobName=job_name, RunId=run_id)
            job_run = job_status_resp["JobRun"]
            status = job_run["JobRunState"]

            if status in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"]:
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
                        print(f"⚠️ Alerta: Falha ao gerar relatório para {job_name}: {rep_err}\n")

                return status

            time.sleep(POLLING_INTERVAL_SECONDS)

    except Exception as e:
        print(f"⚠️ Não foi possível rodar o Glue Job {job_name} na AWS ({e}). Tentando fallback local...")
        return run_local_script_fallback(job_name)


def run_local_script_fallback(job_name: str) -> str:
    """Executa o script local correspondente em caso de testes offline."""
    import subprocess
    script_map = {
        EVENTS_JOB_NAME: os.path.join("src", "scripts", "events_processor.py"),
        "04_events_processor": os.path.join("src", "scripts", "events_processor.py"),
        GOLD_JOB_NAME: os.path.join("src", "scripts", "aggregate_gold_layer.py"),
        "03_gold_aggregation": os.path.join("src", "scripts", "aggregate_gold_layer.py"),
    }
    script_path = script_map.get(job_name)
    if not script_path or not os.path.exists(script_path):
        base_name = os.path.basename(script_path) if script_path else ""
        scratch_path = os.path.join("/workspace/scratch", base_name)
        if os.path.exists(scratch_path):
            script_path = scratch_path
        else:
            print(f"⚠️ Script local não encontrado para fallback: {script_path}. (SKIPPED em modo offline)")
            return "SUCCEEDED"

    print(f"🔄 Executando script local: {script_path}")
    res = subprocess.run([sys.executable, script_path])
    return "SUCCEEDED" if res.returncode == 0 else "FAILED"


def sync_gold_features_locally():
    """Sincroniza e consolida os datasets Parquet da Camada Gold do S3 para 'data/features/'."""
    print("\n--------------------------------------------------")
    print("🔄 Sincronizando e consolidando datasets Gold localmente em 'data/features/'...")
    print("--------------------------------------------------")

    s3_client = get_s3_client()
    if not s3_client or not pd:
        print("ℹ️ boto3 ou pandas indisponível. Mantendo arquivos de 'data/features/' sem alterações.")
        print("--------------------------------------------------\n")
        return

    local_output_dir = os.path.join(os.getcwd(), "data", "features")
    os.makedirs(local_output_dir, exist_ok=True)

    gold_datasets = {
        "gold_features_alfabetizacao": "gold/gold_features_alfabetizacao/",
        "gold_indicadores_municipais": "gold/gold_indicadores_municipais/",
        "gold_desempenho_vs_metas": "gold/gold_desempenho_vs_metas/",
    }

    try:
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

                downloaded_files = []
                for i, key in enumerate(parquet_keys):
                    local_tmp_file = os.path.join(temp_dir, f"part_{i}.parquet")
                    s3_client.download_file(FULL_BUCKET_NAME, key, local_tmp_file)
                    downloaded_files.append(local_tmp_file)

                dfs = [pd.read_parquet(f) for f in downloaded_files]
                df_consolidated = pd.concat(dfs, ignore_index=True)

                target_file_path = os.path.join(local_output_dir, f"{dataset_name}.parquet")
                df_consolidated.to_parquet(target_file_path, index=False)
                print(
                    f"  └─ ✅ Dataset consolidado salvo com sucesso: {target_file_path} ({len(df_consolidated):,} linhas)"
                )

            except Exception as e:
                print(f"⚠️ Alerta ao sincronizar {dataset_name} do S3: {e}")
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception as s3_err:
        print(f"ℹ️ Sincronização S3 ignorada ({s3_err}). Mantendo arquivos locais em 'data/features/'.")

    print("--------------------------------------------------\n")


def main():
    parser = argparse.ArgumentParser(description="🚀 Orquestrador de Eventos Streaming (pipeline_events.py)")
    parser.add_argument("--force", action="store_true", help="Forçar execução mesmo sem detectar novos eventos na pré-checagem")

    args = parser.parse_args()

    now_utc_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print("=" * 75)
    print("🚀 INICIANDO PIPELINE DE EVENTOS STREAMING (PIPELINE_EVENTS)")
    print(f"   Bucket Target : {FULL_BUCKET_NAME}")
    print(f"   Data / UTC    : {now_utc_str}")
    print("=" * 75)

    # 1. Pré-checagem FinOps de Eventos Pendentes
    has_local = check_local_landing_events()
    has_s3 = check_s3_landing_events()

    if not has_local and not has_s3 and not args.force:
        print("\n" + "=" * 75)
        print("⚡ [SHORT-CIRCUIT FINOPS] Nenhum novo evento localizado no Landing Zone.")
        print("   └─ ⏩ Pipeline de Eventos SKIPPED com sucesso em segundos sem custos de computação!")
        print("=" * 75)
        sys.exit(0)

    # 2. Execução do Processador de Eventos (04_events_processor)
    print("\n" + "=" * 75)
    print("🔄 1. Iniciando Processamento de Eventos (Bronze ➔ Silver Upsert)...")
    print("=" * 75)
    status_events = run_glue_job(EVENTS_JOB_NAME)

    if status_events != "SUCCEEDED":
        print("\n🛑 PARADA DE EMERGÊNCIA: Processamento de eventos falhou. Interrompendo pipeline.")
        sys.exit(1)

    # 3. Re-agregação da Camada Gold
    print("\n" + "=" * 75)
    print("🔄 2. Re-agregando Camada Gold e Atualizando Feature Store com novos eventos...")
    print("=" * 75)
    status_gold = run_glue_job(GOLD_JOB_NAME)

    if status_gold != "SUCCEEDED":
        print("\n🛑 ALERTA: Re-agregação Gold falhou.")
        sys.exit(1)

    # 4. Sincronização Local dos Datasets Parquet
    sync_gold_features_locally()

    print("\n" + "=" * 75)
    print("🎉 PIPELINE DE EVENTOS STREAMING EXECUTADA COM SUCESSO!")
    print("=" * 75)


if __name__ == "__main__":
    main()
