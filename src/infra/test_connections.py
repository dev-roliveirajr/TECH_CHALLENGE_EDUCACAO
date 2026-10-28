import os
import boto3
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
GCP_PROJECT = os.getenv("GCP_PROJECT_BILLING")
TABLE_ID = "basedosdados.br_inep_avaliacao_alfabetizacao.uf"

def test_aws_s3():
    print("1. Testando conexão com AWS S3...")
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        test_file = "test_connection.txt"
        s3.put_object(Bucket=S3_BUCKET, Key=test_file, Body=b"ok")
        s3.delete_object(Bucket=S3_BUCKET, Key=test_file)
        print("   ✅ AWS S3: Conexão e permissão de escrita/exclusão OK!")
    except Exception as e:
        print(f"   ❌ Erro AWS S3: {str(e)}")

def test_aws_glue():
    print("2. Testando acesso à API do AWS Glue...")
    try:
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.get_jobs()
        print("   ✅ AWS Glue: Acesso à API OK!")
    except Exception as e:
        print(f"   ❌ Erro AWS Glue: {str(e)}")



def test_gcp_bigquery():
    print("3. Testando conexão com GCP BigQuery...")
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not cred_path:
        print("   ⚠️ GCP BigQuery: Variable 'GOOGLE_APPLICATION_CREDENTIALS' não configurada no .env.")
        return

    # Garante conversão de caminho relativo para absoluto baseado na raiz de execução
    abs_path = os.path.abspath(cred_path)

    if not os.path.exists(abs_path):
        print(f"   ❌ Erro GCP BigQuery: Arquivo não encontrado no caminho: '{abs_path}'")
        print("      💡 Dicas para corrigir:")
        print("         1. Confira o nome exato no .env (ex: gcp-key.json).")
        print("         2. Verifique se o VS Code não adicionou a extensão dupla (ex: gcp-key.json.json).")
        print("         3. Certifique-se de estar executando o comando da raiz do projeto.")
        return

    # Atualiza a variável de ambiente com o caminho absoluto validado
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = abs_path

    try:
        bq_client = bigquery.Client(project=GCP_PROJECT) if GCP_PROJECT else bigquery.Client()
        table = bq_client.get_table(TABLE_ID)
        print(f"   ✅ GCP BigQuery: Tabela '{TABLE_ID}' acessada com sucesso!")
        print(f"      Última modificação no GCP: {table.modified}")
    except Exception as e:
        print(f"   ❌ Erro ao consultar BigQuery: {str(e)}")

def main():
    print("=" * 60)
    print("TESTE DE CONEXÃO COM AWS E GCP BIGQUERY")
    print("=" * 60)
    test_aws_s3()
    test_aws_glue()
    test_gcp_bigquery()

if __name__ == "__main__":
    main()
