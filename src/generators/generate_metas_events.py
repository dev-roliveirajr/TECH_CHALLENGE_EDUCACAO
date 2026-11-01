"""
===============================================================================
GERADOR MODULAR DE EVENTOS STREAMING - METAS (src/generators/generate_metas_events.py)
===============================================================================
Descrição: Simula a emissão assíncrona de eventos de atualização de metas
pactuadas de alfabetização (Municípios, UFs e Brasil) diretamente para a Landing Zone S3.
===============================================================================
"""

import os
import sys
import time
import json
import uuid
import random
import datetime
import argparse
from dotenv import load_dotenv

try:
    import boto3
except ImportError:
    boto3 = None

# Carrega variáveis de ambiente (.env)
load_dotenv(override=True)

# Amostras para simulação de UFs e Municípios
UFS_BRASIL = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT",
    "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"
]

SAMPLE_MUNICIPIOS = [
    "1100015", "1100205", "1200401", "1302603", "1400100", "1501402", "1600303",
    "1721000", "2111300", "2207702", "2304400", "2408102", "2507507", "2611606",
    "2704302", "2800308", "2927408", "3106200", "3205309", "3304557", "3550308",
    "4106902", "4205407", "4314902", "5002704", "5103403", "5208707", "5300108"
]


def generate_single_meta_event(domain="municipio", custom_id=None):
    """Gera um único payload de evento de repactuação de meta educacional."""
    event_time_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    
    # Simulação de repactuação realista da trajetória de metas (2024 - 2030)
    meta_2024 = round(random.uniform(55.0, 68.0), 2)
    meta_2025 = round(meta_2024 + random.uniform(2.0, 5.0), 2)
    meta_2026 = round(meta_2025 + random.uniform(2.0, 5.0), 2)
    meta_2027 = round(meta_2026 + random.uniform(2.0, 5.0), 2)
    meta_2028 = round(meta_2027 + random.uniform(2.0, 5.0), 2)
    meta_2029 = round(meta_2028 + random.uniform(2.0, 5.0), 2)
    meta_2030 = round(max(80.0, meta_2029 + random.uniform(2.0, 5.0)), 2)

    payload = {
        "event_id": str(uuid.uuid4()),
        "event_type": "META_UPDATE",
        "domain": domain,
        "event_time": event_time_utc,
        "ano": 2024,
        "rede": random.choice(["Publica", "Municipal", "Estadual"]),
        "taxa_alfabetizacao_pactuada": meta_2024,
        "meta_alfabetizacao_2024": meta_2024,
        "meta_alfabetizacao_2025": meta_2025,
        "meta_alfabetizacao_2026": meta_2026,
        "meta_alfabetizacao_2027": meta_2027,
        "meta_alfabetizacao_2028": meta_2028,
        "meta_alfabetizacao_2029": meta_2029,
        "meta_alfabetizacao_2030": meta_2030,
        "percentual_participacao": round(random.uniform(85.0, 98.0), 2),
        "_source": "streaming_live_simulation"
    }

    if domain == "municipio":
        payload["id_municipio"] = custom_id or random.choice(SAMPLE_MUNICIPIOS)
    elif domain == "uf":
        payload["sigla_uf"] = custom_id or random.choice(UFS_BRASIL)
    elif domain == "brasil":
        payload["pais"] = "Brasil"

    return payload


def main():
    parser = argparse.ArgumentParser(
        description="🚀 Gerador de Eventos Streaming de Repactuação de Metas Educacionais"
    )
    parser.add_argument(
        "-d", "--domain",
        type=str,
        choices=["municipio", "uf", "brasil"],
        default="municipio",
        help="Domínio da meta a ser repactuada: 'municipio', 'uf' ou 'brasil' (default: municipio)"
    )
    parser.add_argument(
        "-n", "--quantity",
        type=int,
        default=20,
        help="Quantidade total de eventos de metas a serem gerados (default: 20)"
    )
    parser.add_argument(
        "-r", "--rate",
        type=float,
        default=0.0,
        help="Taxa de emissão (eventos por segundo). Use 0 para geração instantânea em lote."
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default=None,
        help="Nome do Bucket S3 de destino. Se omitido, resolve S3_BUCKET_NAME + AWS_ACCOUNT_ID + AWS_REGION do ambiente."
    )

    args = parser.parse_args()

    # Resolução Dinâmica e Robusta do Bucket S3
    raw_bucket = args.s3_bucket or os.getenv("S3_BUCKET_NAME", "tc-alfabetizacao")
    aws_account_id = os.getenv("AWS_ACCOUNT_ID", "")
    aws_region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1")

    if aws_account_id and aws_account_id not in raw_bucket:
        s3_bucket = f"{raw_bucket}-{aws_account_id}-{aws_region}"
    else:
        s3_bucket = raw_bucket

    s3_client = boto3.client("s3", region_name=aws_region) if boto3 is not None else None

    print("=" * 75)
    print(f"🚀 INICIANDO GERADOR DE EVENTOS STREAMING: METAS ({args.domain.upper()})")
    print(f"   * Domínio Alvo     : meta_alfabetizacao_{args.domain}")
    print(f"   * Quantidade Alvo  : {args.quantity:,} eventos")
    print(f"   * Taxa de Emissão  : {args.rate} ev/s ({'Instantâneo' if args.rate <= 0 else 'Cadenciado'})")
    print(f"   * Destino AWS S3   : s3://{s3_bucket}/bronze/events_raw_landing/meta_{args.domain}/")
    print("=" * 75)

    delay = 1.0 / args.rate if args.rate > 0 else 0
    start_time = time.time()
    events_generated = 0

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    ref_ano = now_utc.strftime("%Y")
    ref_mes = now_utc.strftime("%m")
    ref_dia = now_utc.strftime("%d")

    batch_filename = f"events_metas_{args.domain}_{now_utc.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}.json"
    events_list = []

    for i in range(1, args.quantity + 1):
        event = generate_single_meta_event(domain=args.domain)
        events_list.append(event)
        events_generated += 1

        if i % 10 == 0 or i == args.quantity:
            elapsed = time.time() - start_time
            rate_actual = events_generated / elapsed if elapsed > 0 else 0
            print(f" 📡 Progresso: {events_generated:,}/{args.quantity:,} eventos gerados ({rate_actual:.1f} ev/s)...")

        if delay > 0:
            time.sleep(delay)

    # Compilação NDJSON em memória (RAM) - Zero gravação local em data/
    ndjson_payload = "\n".join(json.dumps(ev, ensure_ascii=False) for ev in events_list) + "\n"
    payload_bytes = ndjson_payload.encode("utf-8")
    payload_size_kb = len(payload_bytes) / 1024.0

    print(f"\n📦 Lote de eventos de metas compilado em memória ({payload_size_kb:.2f} KB | {events_generated:,} registros NDJSON)")

    # Envio Direto para o Amazon S3 Landing Zone
    s3_key = f"bronze/events_raw_landing/meta_{args.domain}/ref_ano={ref_ano}/ref_mes={ref_mes}/ref_dia={ref_dia}/{batch_filename}"
    print(f"☁️ [AWS S3 Direct Upload] Enviando lote para a Landing Zone no S3...")

    if s3_client:
        try:
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=payload_bytes,
                ContentType="application/x-ndjson"
            )
            print(f"   └─ ✅ Upload concluído com sucesso: s3://{s3_bucket}/{s3_key}")
        except Exception as s3_err:
            print(f"   └─ ❌ Erro ao enviar para o S3 ({s3_bucket}): {str(s3_err)}")
    else:
        print(f"   └─ ℹ️ boto3 não disponível no ambiente local. O payload NDJSON ({payload_size_kb:.2f} KB) foi processado com sucesso!")

    print("\n" + "=" * 75)
    print(f"✅ GERADOR DE EVENTOS (METAS {args.domain.upper()}) CONCLUÍDO COM SUCESSO!")
    print(f"   Total de eventos: {events_generated:,} | Tempo decorrido: {time.time() - start_time:.2f}s")
    print("=" * 75)


if __name__ == "__main__":
    main()
