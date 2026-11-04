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
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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


def send_failure_email_notification(
    pipeline_type, job_name, run_id, error_message, report_path=None
):
    """
    Envia e-mail de notificação de falha utilizando smtplib nativo do Python,
    anexando os detalhes da falha e o relatório Markdown gerado.
    """
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    notification_email = os.getenv("NOTIFICATION_EMAIL")

    if not (smtp_server and smtp_user and smtp_password and notification_email):
        print(
            " Notificação por e-mail ignorada: Variáveis SMTP não configuradas no .env."
        )
        return

    subject = f"TC - EDUCAÇÃO: FALHA NA EXECUÇÃO DA PIPELINE [{pipeline_type.upper()}]"

    report_content = ""
    if report_path and os.path.exists(report_path):
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                report_content = f.read()
        except Exception as r_err:
            report_content = (
                f"Não foi possível ler o relatório em {report_path}: {r_err}"
            )

    now_utc = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    body_text = f"""===============================================================================
 ALERTA DE CRITICAL FAILURE DA PIPELINE DE DADOS (EVENTS STREAMING)
===============================================================================
Pipeline      : {pipeline_type.upper()}
AWS Glue Job  : {job_name}
Run ID        : {run_id}
Timestamp UTC : {now_utc}
Status        : FAILED
===============================================================================
Mensagem do Erro / Causa Raiz:
{error_message}

===============================================================================
 RELATÓRIO DE EXECUÇÃO DO JOB (MARKDOWN)
===============================================================================
{report_content if report_content else "Relatório não gerado ou indisponível."}
"""

    try:
        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = notification_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body_text, "plain", "utf-8"))

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, notification_email, msg.as_string())
        server.quit()
        print(f" E-mail de notificação de falha enviado  para: {notification_email}")
    except Exception as mail_err:
        print(f" Alerta ao enviar e-mail de notificação: {mail_err}")


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
        os.path.join(os.getcwd(), "data", "events_landing", "meta_municipio"),
        os.path.join(os.getcwd(), "data", "events_landing", "meta_uf"),
        os.path.join(os.getcwd(), "data", "events_landing", "meta_brasil"),
    ]
    return any(
        filename.endswith(".json")
        for directory in landing_dirs
        if os.path.exists(directory)
        for _, _, filenames in os.walk(directory)
        for filename in filenames
    )


def check_s3_landing_events() -> bool:
    """Verifica se existem eventos pendentes na Landing Zone S3."""
    if not boto3:
        raise RuntimeError("boto3 é necessário para verificar eventos no S3.")

    s3 = get_s3_client()
    if not s3:
        raise RuntimeError(
            "Não foi possível criar o cliente S3 para verificar eventos."
        )

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=FULL_BUCKET_NAME,
        Prefix="bronze/events_raw_landing/",
    ):
        if any(obj["Key"].endswith(".json") for obj in page.get("Contents", [])):
            return True
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


def _report_completed_job(job_name: str, run_id: str, job_run: dict) -> str:
    status = job_run["JobRunState"]
    execution_time = job_run.get("ExecutionTime", 0)
    dpu_seconds = job_run.get("DPUSeconds", 0)
    report_path = None

    if generate_job_report:
        try:
            report_path = generate_job_report(job_name, run_id)
            print(f" Relatório salvo: {report_path}\n")
        except Exception as report_error:
            print(
                f" Alerta: Falha ao gerar relatório para {job_name}: "
                f"{report_error}\n"
            )

    print("\n--------------------------------------------------")
    if status == "SUCCEEDED":
        print(f"Job {job_name} concluído.")
    else:
        error_message = job_run.get("ErrorMessage", "Sem detalhes adicionais.")
        print(f" Erro: Job {job_name} terminou com status: {status}")
        print(f"   Detalhes: {error_message}")
        send_failure_email_notification(
            "EVENTS", job_name, run_id, error_message, report_path
        )

    print(f" Duração: {execution_time}s | DPU-Segundos: {dpu_seconds}")
    print("--------------------------------------------------")
    return status


def _poll_glue_job(glue_client, logs_client, job_name: str, run_id: str) -> str:
    next_token = None
    seen_event_ids = set()
    noise_indicators = ["FutureWarning", "google.api_core", "WARN NativeCodeLoader"]
    terminal_statuses = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"}

    while True:
        try:
            next_token = _fetch_and_print_job_logs(
                logs_client, run_id, next_token, seen_event_ids, noise_indicators
            )
        except Exception:
            pass

        response = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        job_run = response["JobRun"]
        if job_run["JobRunState"] in terminal_statuses:
            return _report_completed_job(job_name, run_id, job_run)

        time.sleep(POLLING_INTERVAL_SECONDS)


def run_glue_job(job_name: str) -> str:
    """Dispara a execução de um AWS Glue Job ou aciona fallback local se indisponível."""
    glue_client = get_glue_client()
    logs_client = get_logs_client()

    if not glue_client:
        print(
            f"   Conexão boto3 AWS indisponível. Recorrendo ao modo local para {job_name}..."
        )
        return run_local_script_fallback(job_name)

    print(f"\n Disparando AWS Glue Job: {job_name}")

    try:
        response = glue_client.start_job_run(JobName=job_name)
        run_id = response["JobRunId"]
        print(f" JobRunId gerado: {run_id}")
        print(" Acompanhando logs em tempo real (CloudWatch /aws-glue/jobs/output):\n")
        return _poll_glue_job(glue_client, logs_client, job_name, run_id)
    except Exception as error:
        print(
            f" Não foi possível rodar o Glue Job {job_name} na AWS "
            f"({error}). Tentando fallback local..."
        )
        print(f" Detalhes do erro: {error}")
        return run_local_script_fallback(job_name)


def run_local_script_fallback(job_name: str) -> str:
    """Executa o script local correspondente em caso de testes offline."""
    import subprocess

    script_map = {
        EVENTS_JOB_NAME: os.path.join("src", "scripts", "events_processor.py"),
        "04_events_processor": os.path.join("src", "scripts", "events_processor.py"),
        GOLD_JOB_NAME: os.path.join("src", "scripts", "aggregate_gold_layer.py"),
        "03_gold_aggregation": os.path.join(
            "src", "scripts", "aggregate_gold_layer.py"
        ),
    }
    script_path = script_map.get(job_name)
    if not script_path or not os.path.exists(script_path):
        base_name = os.path.basename(script_path) if script_path else ""
        scratch_path = os.path.join("/workspace/scratch", base_name)
        if os.path.exists(scratch_path):
            script_path = scratch_path
        else:
            print(
                f" Script local não encontrado para fallback: {script_path}. (SKIPPED em modo offline)"
            )
            return "SUCCEEDED"

    print(f" Executando script local: {script_path}")
    res = subprocess.run([sys.executable, script_path])
    res_status = "SUCCEEDED" if res.returncode == 0 else "FAILED"
    if res_status == "FAILED":
        send_failure_email_notification(
            "EVENTS",
            job_name,
            "local_fallback",
            f"Falha na execução local do script {script_path}",
            None,
        )
    return res_status


def sync_gold_features_locally():
    """Sincroniza e consolida os datasets Parquet da Camada Gold do S3 para 'data/features/'."""
    print("\n--------------------------------------------------")
    print(
        " Sincronizando e consolidando datasets Gold localmente em 'data/features/'..."
    )
    print("--------------------------------------------------")

    s3_client = get_s3_client()
    if not s3_client or not pd:
        print(
            " boto3 ou pandas indisponível. Mantendo arquivos de 'data/features/' sem alterações."
        )
        print("--------------------------------------------------\n")
        return

    local_output_dir = os.path.join(os.getcwd(), "data", "features")
    os.makedirs(local_output_dir, exist_ok=True)

    gold_datasets = {
        "gold_features_alfabetizacao": "gold/gold_features_alfabetizacao/",
        "gold_features_alfabetizacao_uf": "gold/gold_features_alfabetizacao_uf/",
        "gold_features_alfabetizacao_brasil": "gold/gold_features_alfabetizacao_brasil/",
        "gold_indicadores_municipais": "gold/gold_indicadores_municipais/",
        "gold_indicadores_uf": "gold/gold_indicadores_uf/",
        "gold_indicadores_brasil": "gold/gold_indicadores_brasil/",
        "gold_desempenho_vs_metas": "gold/gold_desempenho_vs_metas/",
        "gold_desempenho_vs_metas_uf": "gold/gold_desempenho_vs_metas_uf/",
        "gold_desempenho_vs_metas_brasil": "gold/gold_desempenho_vs_metas_brasil/",
    }

    try:
        for dataset_name, s3_prefix in gold_datasets.items():
            temp_dir = tempfile.mkdtemp()
            try:
                paginator = s3_client.get_paginator("list_objects_v2")
                parquet_keys = [
                    obj["Key"]
                    for page in paginator.paginate(
                        Bucket=FULL_BUCKET_NAME,
                        Prefix=s3_prefix,
                    )
                    for obj in page.get("Contents", [])
                    if obj["Key"].endswith(".parquet")
                    and not obj["Key"].split("/")[-1].startswith(".")
                ]
                if not parquet_keys:
                    print(f" Aviso: Nenhum arquivo Parquet encontrado em {s3_prefix}.")
                    continue

                downloaded_files = []
                for i, key in enumerate(parquet_keys):
                    local_tmp_file = os.path.join(temp_dir, f"part_{i}.parquet")
                    s3_client.download_file(FULL_BUCKET_NAME, key, local_tmp_file)
                    downloaded_files.append(local_tmp_file)

                dfs = [pd.read_parquet(f) for f in downloaded_files]
                df_consolidated = pd.concat(dfs, ignore_index=True)

                target_file_path = os.path.join(
                    local_output_dir, f"{dataset_name}.parquet"
                )
                df_consolidated.to_parquet(target_file_path, index=False)
                print(
                    f"  └─  Dataset consolidado salvo : {target_file_path} ({len(df_consolidated):,} linhas)"
                )

            except Exception as e:
                print(f" Alerta ao sincronizar {dataset_name} do S3: {e}")
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception as s3_err:
        print(
            f" Sincronização S3 ignorada ({s3_err}). Mantendo arquivos locais em 'data/features/'."
        )

    print("--------------------------------------------------\n")


def main():
    parser = argparse.ArgumentParser(
        description=" Orquestrador de Eventos Streaming (pipeline_events.py)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Forçar execução mesmo sem detectar novos eventos na pré-checagem",
    )

    args = parser.parse_args()

    now_utc_str = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    print("=" * 75)
    print(" INICIANDO PIPELINE DE EVENTOS STREAMING (PIPELINE_EVENTS)")
    print(f"   Bucket Target : {FULL_BUCKET_NAME}")
    print(f"   Data / UTC    : {now_utc_str}")
    print("=" * 75)

    # 1. Verifica se há eventos pendentes
    has_local = check_local_landing_events()
    has_s3 = check_s3_landing_events()

    if not has_local and not has_s3 and not args.force:
        print("\n" + "=" * 75)
        print(" [SEM DADOS NOVOS] Nenhum novo evento localizado no Landing Zone.")
        print("   └─  Pipeline de Eventos SKIPPED   sem iniciar o processamento Spark.")
        print("=" * 75)
        sys.exit(0)

    # 2. Execução do Processador de Eventos (04_events_processor)
    print("\n" + "=" * 75)
    print(" 1. Iniciando Processamento de Eventos (Bronze ➔ Silver Upsert)...")
    print("=" * 75)
    status_events = run_glue_job(EVENTS_JOB_NAME)

    if status_events != "SUCCEEDED":
        print(
            "\n Execução interrompida: Processamento de eventos falhou. Interrompendo pipeline."
        )
        sys.exit(1)

    # 3. Re-agregação da Camada Gold
    print("\n" + "=" * 75)
    print(
        " 2. Re-agregando Camada Gold e Atualizando Feature Store com novos eventos..."
    )
    print("=" * 75)
    status_gold = run_glue_job(GOLD_JOB_NAME)

    if status_gold != "SUCCEEDED":
        print("\n ALERTA: Re-agregação Gold falhou.")
        sys.exit(1)

    # 4. Sincronização Local dos Datasets Parquet
    sync_gold_features_locally()

    print("\n" + "=" * 75)
    print("Pipeline de eventos concluído.")
    print("=" * 75)


if __name__ == "__main__":
    main()
