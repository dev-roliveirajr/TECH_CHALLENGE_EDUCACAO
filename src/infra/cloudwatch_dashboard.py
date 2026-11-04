"""
Provisionamento do dashboard de observabilidade AWS Glue no CloudWatch.

Este módulo mantém a definição dos widgets fora do script principal de deploy.
As métricas avançadas são publicadas no namespace Glue após habilitar
--enable-observability-metrics=true nos Glue Jobs.
"""

import json

from botocore.exceptions import ClientError


def _metric(
    metric_name,
    job_name,
    metric_type="count",
    observability_group=None,
    label=None,
    stat=None,
):
    """Monta uma definição de métrica CloudWatch com dimensões Glue."""
    dimensions = [
        "JobName",
        job_name,
        "JobRunId",
        "ALL",
        "Type",
        metric_type,
    ]
    if observability_group:
        dimensions.extend(["ObservabilityGroup", observability_group])

    metric = ["Glue", metric_name, *dimensions]
    options = {}
    if label:
        options["label"] = label
    if stat:
        options["stat"] = stat
    if options:
        metric.append(options)
    return metric


def _metric_widget(
    title,
    metrics,
    x,
    y,
    width=12,
    height=6,
    stat="Sum",
    period=300,
    y_axis_label=None,
    y_axis_max=None,
):
    """Cria um widget de métricas CloudWatch."""
    properties = {
        "metrics": metrics,
        "view": "timeSeries",
        "stacked": False,
        "region": None,  # preenchido pelo chamador
        "title": title,
        "period": period,
        "stat": stat,
    }
    if y_axis_label or y_axis_max is not None:
        properties["yAxis"] = {"left": {}}
        if y_axis_label:
            properties["yAxis"]["left"]["label"] = y_axis_label
        if y_axis_max is not None:
            properties["yAxis"]["left"]["max"] = y_axis_max

    return {
        "type": "metric",
        "x": x,
        "y": y,
        "width": width,
        "height": height,
        "properties": properties,
    }


def setup_cloudwatch_dashboard(
    cw_client,
    aws_region,
    aws_account_id,
    raw_bucket_name,
    jobs,
    dashboard_name=None,
):
    """
    Cria/atualiza o dashboard de observabilidade da pipeline.

    Parâmetros:
        cw_client: cliente boto3 do CloudWatch.
        aws_region: região AWS do deploy.
        aws_account_id: ID da conta AWS (usado nos ARNs dos alarmes).
        raw_bucket_name: nome-base do bucket, usado no nome dos alarmes.
        jobs: lista com os nomes dos Glue Jobs.
        dashboard_name: nome opcional do dashboard.
    """
    dashboard_name = dashboard_name or (f"{raw_bucket_name}-Observability-Dashboard")
    jobs = list(jobs)

    print(f"\n Provisionando CloudWatch Dashboard " f"('{dashboard_name}')...")

    # Métricas básicas agregadas por JobName + JobRunId=ALL + Type=count.
    elapsed_metrics = [
        _metric(
            "glue.driver.aggregate.elapsedTime",
            job,
            label=job,
        )
        for job in jobs
    ]
    for index, metric in enumerate(elapsed_metrics, start=1):
        metric[-1]["id"] = f"duration{index}"
        metric[-1]["visible"] = False

    elapsed_metrics.append(
        [
            {
                "expression": "METRICS()/1000",
                "label": "Duração (segundos)",
                "id": "duration_seconds",
                "period": 300,
            }
        ]
    )

    tasks_metrics = [
        _metric(
            "glue.driver.aggregate.numCompletedTasks",
            job,
            label=job,
        )
        for job in jobs
    ]

    # Volumes agregados suportados pelas métricas básicas do Glue.
    read_bytes_metrics = [
        _metric(
            "glue.driver.aggregate.bytesRead",
            job,
            label=job,
        )
        for job in jobs
    ]
    read_records_metrics = [
        _metric(
            "glue.driver.aggregate.recordsRead",
            job,
            label=job,
        )
        for job in jobs
    ]

    s3_write_bytes_metrics = [
        _metric(
            "glue.ALL.s3.filesystem.write_bytes",
            job,
            metric_type="gauge",
            label=job,
        )
        for job in jobs
    ]

    # Métricas avançadas: gauges de utilização e performance.
    worker_utilization_metrics = [
        _metric(
            "glue.driver.workerUtilization",
            job,
            metric_type="gauge",
            observability_group="resource_utilization",
            label=job,
        )
        for job in jobs
    ]
    skewness_metrics = [
        _metric(
            "glue.driver.skewness.job",
            job,
            metric_type="gauge",
            observability_group="job_performance",
            label=job,
        )
        for job in jobs
    ]
    executor_memory_metrics = [
        _metric(
            "glue.ALL.memory.heap.used.percentage",
            job,
            metric_type="gauge",
            observability_group="resource_utilization",
            label=job,
        )
        for job in jobs
    ]
    executor_disk_metrics = [
        _metric(
            "glue.ALL.disk.used.percentage",
            job,
            metric_type="gauge",
            observability_group="resource_utilization",
            label=job,
        )
        for job in jobs
    ]

    # Sucesso/falha de execuções, com dimensões de observabilidade.
    run_errors_metrics = [
        _metric(
            "glue.error.ALL",
            job,
            metric_type="count",
            observability_group="error",
            label=f"{job} - erros",
        )
        for job in jobs
    ]
    run_success_metrics = [
        _metric(
            "glue.succeed.ALL",
            job,
            metric_type="count",
            observability_group="error",
            label=f"{job} - sucesso",
        )
        for job in jobs
    ]

    widgets = [
        {
            "type": "text",
            "x": 0,
            "y": 0,
            "width": 24,
            "height": 3,
            "properties": {
                "markdown": (
                    "# Monitoramento do pipeline de dados\n"
                    "**AWS Glue | Execução | Recursos**\n\n"
                    f"Região: `{aws_region}` · Bucket: "
                    f"`{raw_bucket_name}`"
                )
            },
        },
        _metric_widget(
            "Duração ETL por Glue Job (segundos)",
            elapsed_metrics,
            0,
            3,
            y_axis_label="Segundos",
        ),
        _metric_widget(
            "Tarefas Spark concluídas",
            tasks_metrics,
            12,
            3,
        ),
        _metric_widget(
            "Volume de dados lidos (bytes)",
            read_bytes_metrics,
            0,
            9,
            y_axis_label="Bytes",
        ),
        _metric_widget(
            "Registros lidos",
            read_records_metrics,
            12,
            9,
        ),
        _metric_widget(
            "Bytes gravados no Amazon S3",
            s3_write_bytes_metrics,
            0,
            15,
            stat="Sum",
            y_axis_label="Bytes",
        ),
        _metric_widget(
            "🧩 Utilização dos workers",
            worker_utilization_metrics,
            12,
            15,
            stat="Average",
            y_axis_label="Fração de utilização (0 a 1)",
            y_axis_max=1,
        ),
        _metric_widget(
            "Memória heap dos executores utilizada (%)",
            executor_memory_metrics,
            0,
            21,
            stat="Average",
            y_axis_label="Percentual",
            y_axis_max=100,
        ),
        _metric_widget(
            "💽 Disco dos executores utilizado (%)",
            executor_disk_metrics,
            12,
            21,
            stat="Average",
            y_axis_label="Percentual",
            y_axis_max=100,
        ),
        _metric_widget(
            "Skewness por Glue Job",
            skewness_metrics,
            0,
            33,
            stat="Maximum",
            y_axis_label="Índice de skewness",
        ),
        _metric_widget(
            "Erros em execuções dos Glue Jobs",
            run_errors_metrics,
            12,
            33,
        ),
        _metric_widget(
            "Execuções concluídas",
            run_success_metrics,
            0,
            39,
        ),
        {
            "type": "alarm",
            "x": 0,
            "y": 45,
            "width": 24,
            "height": 4,
            "properties": {
                "title": "Alarmes de falha dos Glue Jobs",
                "alarms": [
                    (
                        f"arn:aws:cloudwatch:{aws_region}:"
                        f"{aws_account_id}:alarm:"
                        f"{raw_bucket_name}-GlueJobFailure-{job}"
                    )
                    for job in jobs
                ],
            },
        },
        {
            "type": "text",
            "x": 0,
            "y": 49,
            "width": 24,
            "height": 4,
            "properties": {
                "markdown": (
                    "### Capacidade e custo de execução\n"
                    "Configuração atual dos Jobs: **2 workers G.1X "
                    "(2 DPUs provisionadas por execução)**. "
                    "Os gráficos de utilização ajudam a identificar "
                    "subutilização ou pressão de recursos.\n\n"
                    "**Importante:** este painel não apresenta custo real "
                    "nem `DPUSeconds`. Na configuração fixa atual, não se "
                    "deve tratar `tempo × DPUs` como consumo faturável exato. "
                    "O valor `DPUSeconds` depende de FLEX ou Auto Scaling; "
                    "não habilitamos Auto Scaling apenas para obter essa métrica."
                )
            },
        },
    ]

    for widget in widgets:
        if widget["type"] == "metric":
            widget["properties"]["region"] = aws_region

    dashboard_body = {"widgets": widgets}

    try:
        response = cw_client.put_dashboard(
            DashboardName=dashboard_name,
            DashboardBody=json.dumps(dashboard_body),
        )
        dashboard_errors = response.get("DashboardValidationMessages", [])
        if dashboard_errors:
            print(
                "  └─  Dashboard criado com avisos de validação: " f"{dashboard_errors}"
            )
        else:
            print(
                f"  └─  CloudWatch Dashboard " f"'{dashboard_name}' criado/atualizado."
            )
    except ClientError as error:
        print("  └─  Erro ao criar/atualizar CloudWatch Dashboard: " f"{error}")
        raise
