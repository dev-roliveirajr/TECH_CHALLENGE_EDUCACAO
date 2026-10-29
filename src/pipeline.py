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
import datetime
import boto3
from dotenv import load_dotenv
from monitor.report_generator import generate_job_report


# Carrega variáveis de ambiente do .env
load_dotenv(override=True)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
POLLING_INTERVAL_SECONDS = int(os.getenv("GLUE_POLLING_INTERVAL", "10"))

def get_glue_client():
    """Inicializa o cliente boto3 do AWS Glue."""
    return boto3.client("glue", region_name=AWS_REGION)

def get_logs_client():
    """Inicializa o cliente boto3 do AWS CloudWatch Logs."""
    return boto3.client("logs", region_name=AWS_REGION)

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
        print(f"📡 Acompanhando logs em tempo real (CloudWatch /aws-glue/jobs/output):\n")
        
        log_group = "/aws-glue/jobs/output"
        next_token = None
        seen_event_ids = set()
        noise_indicators = ["FutureWarning", "google.api_core", "WARN NativeCodeLoader"]

        while True:
            # 1. Tenta capturar e imprimir novos logs do CloudWatch em tempo real
            try:
                kwargs = {
                    "logGroupName": log_group,
                    "logStreamName": run_id
                }
                if next_token:
                    kwargs["nextToken"] = next_token
                else:
                    kwargs["startFromHead"] = True

                log_resp = logs_client.get_log_events(**kwargs)
                next_token = log_resp.get("nextForwardToken")
                events = log_resp.get("events", [])

                for ev in events:
                    ev_id = ev.get("eventId") or (ev["timestamp"], ev["message"])
                    if ev_id in seen_event_ids:
                        continue
                    seen_event_ids.add(ev_id)

                    msg = ev["message"].strip()
                    if any(noise in msg for noise in noise_indicators):
                        continue

                    ts = datetime.datetime.fromtimestamp(ev["timestamp"] / 1000.0).strftime("%H:%M:%S")
                    print(f"  [{ts}] {msg}")
            except Exception:
                # O LogStream pode levar alguns segundos para ser criado na AWS ao iniciar o Job
                pass

            # 2. Checa o status do Job na API do Glue
            job_status_resp = glue_client.get_job_run(JobName=job_name, RunId=run_id)
            job_run = job_status_resp["JobRun"]
            status = job_run["JobRunState"]
            
            if status in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"]:
                # Garante uma última varredura dos logs após finalizar
                try:
                    kwargs = {"logGroupName": log_group, "logStreamName": run_id}
                    if next_token:
                        kwargs["nextToken"] = next_token
                    else:
                        kwargs["startFromHead"] = True
                    log_resp = logs_client.get_log_events(**kwargs)
                    for ev in log_resp.get("events", []):
                        ev_id = ev.get("eventId") or (ev["timestamp"], ev["message"])
                        if ev_id not in seen_event_ids:
                            seen_event_ids.add(ev_id)
                            msg = ev["message"].strip()
                            if not any(noise in msg for noise in noise_indicators):
                                ts = datetime.datetime.fromtimestamp(ev["timestamp"] / 1000.0).strftime("%H:%M:%S")
                                print(f"  [{ts}] {msg}")
                except Exception:
                    pass

                execution_time = job_run.get("ExecutionTime", 0)
                dpu_seconds = job_run.get("DPUSeconds", 0)
                
                print(f"\n--------------------------------------------------")
                if status == "SUCCEEDED":
                    print(f"✅ Job {job_name} finalizou com SUCESSO!")
                else:
                    error_message = job_run.get("ErrorMessage", "Sem detalhes adicionais.")
                    print(f"❌ ERRO CRÍTICO: Job {job_name} terminou com status: {status}")
                    print(f"   Detalhes: {error_message}")

                print(f"📊 Duração: {execution_time}s | DPU-Segundos: {dpu_seconds}")
                print(f"--------------------------------------------------")
                
                # Invoca a geração do Relatório FinOps e Observabilidade (.md)
                if generate_job_report:
                    try:
                        report_path = generate_job_report(job_name, run_id)
                    except Exception as rep_err:
                        print(f"⚠️ Alerta: Falha ao gerar relatório para {job_name}: {str(rep_err)}\n")
                
                return status
            
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
#    print("\n" + "=" * 60)
#    print("🔄 Inicio da Ingestão de dados para a camada Bronze...")
#    print("=" * 60)
#    status_bronze = run_glue_job("01_bronze_ingestion")
#    if status_bronze != "SUCCEEDED":
#        print("\n🛑 PARADA DE EMERGÊNCIA: Camada Bronze falhou. Interrompendo pipeline.")
#        sys.exit(1)
        
    # 2. Execução da Camada Silver (Com validações de qualidade e JOINs)
    print("\n" + "=" * 60)
    print("🔄 Inicio da Qualificação de dados para a camada Silver...")
    print("=" * 60)
    status_silver = run_glue_job("02_silver_processing")
    if status_silver != "SUCCEEDED":
        print("\n🛑 PARADA DE EMERGÊNCIA: Camada Silver falhou (validação de qualidade/JOINs). Interrompendo pipeline.")
        sys.exit(1)
        
    # 3. Execução da Camada Gold (KPIs e Feature Store para IA)
#    print("\n" + "=" * 60)
#    print("🔄 Inicio da Agregação de dados para a camada Gold...")
#    print("=" * 60)
#    status_gold = run_glue_job("03_gold_aggregation")
#    if status_gold != "SUCCEEDED":
#        print("\n🛑 ALERTA: Camada Gold falhou.")
#        sys.exit(1)
        
    print("\n" + "=" * 60)
    print("🎉 PIPELINE EXECUTADA COM SUCESSO EM TODAS AS CAMADAS!")
    print("=" * 60)

if __name__ == "__main__":
    main()
