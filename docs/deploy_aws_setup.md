# AWS infrastructure deployment

`src/infra/deploy_aws_setup.py` provisions and updates the AWS resources used by
the data pipeline. Running it makes changes in AWS; it is not a dry-run tool.

## Prerequisites

- Set `S3_BUCKET_NAME` to the bucket name or base name.
- Provide AWS credentials through the Boto3 credential chain. The account ID
  is obtained from AWS STS; if `AWS_ACCOUNT_ID` is set, it must match.
- Set `AWS_DEFAULT_REGION` or `AWS_REGION` when the target is not `us-east-1`.
- Set `AWS_GLUE_ROLE_ARN` to a role name such as `LabRole` or to an IAM role ARN
  in the authenticated account.
- Provide a GCP service-account JSON file using
  `GOOGLE_APPLICATION_CREDENTIALS`, or place `gcp_key.json` or `gcp-key.json`
  in the project root.
- Ensure the local deployment scripts exist and the AWS identity can access
  the required AWS services.

The script loads the project's `.env` file with precedence over existing
environment variables. It checks the bucket name, local scripts, and GCP
credential file before calling AWS services.

## Resources and operations

| Resource | Name or configuration |
|---|---|
| S3 bucket | Uses `S3_BUCKET_NAME`. If `AWS_ACCOUNT_ID` is configured and not already in the name, the script appends the account ID and region. |
| Secrets Manager secret | `GCP_BIGQUERY_KEY_SECRET_NAME`; default `pipeline-educacao/gcp/bigquery`. Created or updated with the local service-account JSON. |
| Glue database | `{S3_BUCKET_NAME}_db`. |
| Glue jobs | `01_bronze_ingestion`, `02_silver_processing`, `03_gold_aggregation`, and `04_events_processor`. |
| CloudWatch dashboard | `{S3_BUCKET_NAME}-Observability-Dashboard`. |
| CloudWatch alarms | `{S3_BUCKET_NAME}-GlueJobFailure-{job_name}` for each Glue job. |

The deployment performs these operations in sequence:

1. Resolve the AWS account through STS and check any configured account ID.
2. Create or update the Secrets Manager secret.
3. Check or create the S3 bucket. Apply bucket-owner-enforced ownership,
   public-access blocking, and default SSE-S3 encryption to the bucket.
4. Configure the lifecycle rule `ExpireRawLandingZoneEventsAfter7Days` for
   `bronze/events_raw_landing/`. The expiration period comes from
   `LIFECYCLE_RULES_EXPIRATION_DAYS` and defaults to 7 days. Other lifecycle
   rules are retained.
5. Upload the four Glue job scripts and `quality_lib.py`. If the configured
   Spark BigQuery Connector JAR is absent, download and validate it before
   uploading it.
6. Create or update the Glue database and jobs.
7. Create or update the CloudWatch dashboard and alarms.

The dashboard displays Glue job duration, task and data-volume metrics,
resource utilization, execution errors and successes, and alarm states. The
alarms monitor the Glue `glue.error.ALL` metric. They do not configure SNS or
email notifications.

## Running the deployment

From the project root, activate the project's Python environment and run:

```bash
python src/infra/deploy_aws_setup.py
```

The script stops and reports an error when a required operation fails. It does
not roll back resources already created or updated, so inspect the AWS
resources before retrying a deployment that stopped partway through.
