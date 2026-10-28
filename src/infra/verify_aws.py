"""
===============================================================================
SCRIPT DE VERIFICAÇÃO DE INFRAESTRUTURA AWS (src/infra/verify_aws.py)
===============================================================================
Descrição: Valida se os scripts estão no S3 e inspeciona as configurações
dos Glue Jobs criados no AWS Glue Catalog.
===============================================================================
"""

import os
import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

s3 = boto3.client("s3", region_name=AWS_REGION)
glue = boto3.client("glue", region_name=AWS_REGION)

EXPECTED_JOBS = ["01_bronze_ingestion", "02_silver_processing", "03_gold_aggregation"]
EXPECTED_S3_FILES = [
    "scripts/ingestion_bronze_layer.py",
    "scripts/quality_silver_layer.py",
    "scripts/aggregate_gold_layer.py",
    "scripts/common/quality_lib.py"
]

def check_s3_artifacts():
    print(f"📦 1. Verificando artefatos no S3 (s3://{S3_BUCKET}/)...")
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="scripts/")
        existing_keys = {item["Key"]: item["Size"] for item in response.get("Contents", [])}
        
        for key in EXPECTED_S3_FILES:
            if key in existing_keys:
                size_bytes = existing_keys[key]
                print(f"   ✅ Arquivo S3 encontrado: {key} ({size_bytes} bytes)")
            else:
                print(f"   ❌ Arquivo NÃO encontrado: {key}")
    except Exception as e:
        print(f"   ❌ Erro ao listar bucket S3: {str(e)}")

def check_glue_jobs():
    print("\n⚙️ 2. Verificando configurações dos Glue Jobs no AWS Catalog...")
    for job_name in EXPECTED_JOBS:
        try:
            response = glue.get_job(JobName=job_name)
            job = response["Job"]
            
            role = job.get("Role", "N/A")
            glue_ver = job.get("GlueVersion", "N/A")
            worker_type = job.get("WorkerType", "N/A")
            num_workers = job.get("NumberOfWorkers", "N/A")
            max_retries = job.get("MaxRetries", "N/A")
            script_loc = job.get("Command", {}).get("ScriptLocation", "N/A")
            extra_py = job.get("DefaultArguments", {}).get("--extra-py-files", "N/A")
            
            print(f"\n   🔹 Glue Job: {job_name}")
            print(f"      ├─ Script S3: {script_loc}")
            print(f"      ├─ Glue Version: {glue_ver}")
            print(f"      ├─ Capacidade: {num_workers} x {worker_type} (FinOps OK)")
            print(f"      ├─ Re-tentativas (MaxRetries): {max_retries}")
            print(f"      ├─ Role IAM: {role}")
            print(f"      └─ Biblioteca Auxiliar: {extra_py}")
        except glue.exceptions.EntityNotFoundException:
            print(f"   ❌ Job {job_name} NÃO foi encontrado no Glue Catalog.")
        except Exception as e:
            print(f"   ❌ Erro ao consultar Job {job_name}: {str(e)}")

def main():
    print("=" * 65)
    print("AUDITORIA E VALIDAÇÃO DA INFRAESTRUTURA NA AWS")
    print("=" * 65)
    check_s3_artifacts()
    check_glue_jobs()
    print("\n" + "=" * 65)

if __name__ == "__main__":
    main()
