## Lambda Function which consumes messages from the SQS queue which contains all events.
resource "aws_lambda_function" "upload_lambda" {
  description   = "Lambda Function which consumes messages from the SQS queue related to newly uploaded files."
  function_name = "${var.environment_name}-${var.service_name}-upload-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  role          = aws_iam_role.upload_service_v2_lambda_role.arn
  timeout       = 300
  # 512MB (up from 128MB) roughly 4x the CPU allocation — halves cold-start
  # init time and speeds the RDS-heavy package-creation path. Measured pre-
  # change: ~235ms init, ~550ms p95 per-batch processing with single-file
  # batches. Post-change trades a small per-ms cost for shorter end-to-end
  # finalize-to-Pusher latency.
  memory_size                    = 512
  s3_bucket                      = var.lambda_bucket
  s3_key                         = "${var.service_name}/upload/upload-v2-handler-${var.image_tag}.zip"
  reserved_concurrent_executions = 100 // Set a maximum concurrency to prevent overloading RDS interaction

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV                  = var.environment_name
      PENNSIEVE_DOMAIN     = data.terraform_remote_state.account.outputs.domain_name,
      MANIFEST_TABLE       = aws_dynamodb_table.manifest_dynamo_table.name,
      MANIFEST_FILE_TABLE  = aws_dynamodb_table.manifest_files_dynamo_table.name,
      FILE_FINALIZED_TOPIC = aws_sns_topic.file_finalized_topic.arn,
      JOBS_QUEUE_ID        = data.terraform_remote_state.platform_infrastructure.outputs.jobs_queue_id,
      DELETE_QUEUE_ID      = data.terraform_remote_state.platform_infrastructure.outputs.jobs_queue_v2_id,
      REGION               = var.aws_region,
      RDS_PROXY_ENDPOINT   = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
      LOG_LEVEL            = "info",
      // Live updates: the Event API's publish host where realtime is on
      // (realtime.tf); empty keeps Pusher.
      REALTIME_EVENTS_ENDPOINT = local.realtime_events_enabled ? data.terraform_remote_state.realtime[0].outputs.http_endpoint : "",
    }
  }
}

### SERVICE LAMBDA

## Lambda Function which consumes messages from the SQS queue which contains all events.
resource "aws_lambda_function" "service_lambda" {
  description   = "Lambda Function which consumes messages from the SQS queue related to newly uploaded files."
  function_name = "${var.environment_name}-${var.service_name}-service-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  role          = aws_iam_role.upload_service_v2_lambda_role.arn
  timeout       = 300
  memory_size   = 512
  s3_bucket     = var.lambda_bucket
  s3_key        = "${var.service_name}/service/upload-v2-service-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV                          = var.environment_name
      ARCHIVE_BUCKET               = aws_s3_bucket.manifest_archive_bucket.id,
      PENNSIEVE_DOMAIN             = data.terraform_remote_state.account.outputs.domain_name,
      MANIFEST_TABLE               = aws_dynamodb_table.manifest_dynamo_table.name,
      MANIFEST_FILE_TABLE          = aws_dynamodb_table.manifest_files_dynamo_table.name,
      REGION                       = var.aws_region
      RDS_PROXY_ENDPOINT           = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
      ARCHIVER_INVOKE_ARN          = aws_lambda_function.archive_lambda.arn,
      STORAGE_CREDENTIALS_ROLE_ARN = aws_iam_role.storage_credentials_role.arn,
      DEFAULT_STORAGE_BUCKET       = data.terraform_remote_state.platform_infrastructure.outputs.storage_bucket_id,
      UPLOAD_LAMBDA_ARN            = aws_lambda_function.upload_lambda.arn,
      UPLOAD_TRIGGER_QUEUE_URL     = aws_sqs_queue.upload_trigger_queue.url,
      LOG_LEVEL                    = "info",
      # Run-results imports: check a run's callback token with
      # workflow-service, find its node and account in account-service's
      # tables, and queue the copies.
      CALLBACK_VALIDATOR_FUNCTION  = data.terraform_remote_state.workflow_service.outputs.callback_validator_lambda_arn,
      COMPUTE_NODES_TABLE          = data.terraform_remote_state.account_service.outputs.compute_nodes_table_name,
      ACCOUNTS_TABLE               = data.terraform_remote_state.account_service.outputs.accounts_table_name,
      RUN_RESULTS_IMPORT_QUEUE_URL = aws_sqs_queue.run_results_import_queue.url,
    }
  }
}

### ARCHIVE MANIFEST LAMBDA
## Lambda Function which archives a manifest
resource "aws_lambda_function" "archive_lambda" {
  description   = "Lambda Function which archives a manifest when triggered by the service."
  function_name = "${var.environment_name}-${var.service_name}-archive-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  role          = aws_iam_role.upload_service_v2_lambda_role.arn
  timeout       = 600
  memory_size   = 128
  s3_bucket     = var.lambda_bucket
  s3_key        = "${var.service_name}/archiver/manifest-archiver-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV                 = var.environment_name
      ARCHIVE_BUCKET      = aws_s3_bucket.manifest_archive_bucket.id,
      PENNSIEVE_DOMAIN    = data.terraform_remote_state.account.outputs.domain_name,
      MANIFEST_TABLE      = aws_dynamodb_table.manifest_dynamo_table.name,
      MANIFEST_FILE_TABLE = aws_dynamodb_table.manifest_files_dynamo_table.name,
      REGION              = var.aws_region
      RDS_PROXY_ENDPOINT  = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
      LOG_LEVEL           = "info",
    }
  }
}

### RECONCILE-ORPHANS LAMBDA
#
# Two invocation modes through the same handler:
#   1. One-shot — `aws lambda invoke --payload '{"manifestNodeId":"<uuid>"}'`
#      against a specific manifest (typically to recover stuck Registered
#      rows from a prior incident).
#   2. Scheduled daily — EventBridge rule (see cloudwatch.tf) fires with
#      `{"gracePeriodHours": 6}`. Scans the StatusIndex GSI for stuck
#      Registered rows past the grace window, HEAD-checks each expected
#      storage key, enqueues recoverable files to upload_trigger_queue.
#      Never deletes; missing-object cases are metric'd + logged only.
resource "aws_lambda_function" "reconcile_lambda" {
  description   = "Recovers stuck-Registered manifest_files by HEAD-checking the storage bucket and enqueueing recoverable files to upload_trigger_queue. Safe to invoke manually or on schedule; never deletes."
  function_name = "${var.environment_name}-${var.service_name}-reconcile-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  role          = aws_iam_role.reconcile_lambda_role.arn
  timeout       = 600
  memory_size   = 512
  s3_bucket     = var.lambda_bucket
  s3_key        = "${var.service_name}/reconcile/upload-v2-reconcile-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV                      = var.environment_name
      MANIFEST_TABLE           = aws_dynamodb_table.manifest_dynamo_table.name
      MANIFEST_FILE_TABLE      = aws_dynamodb_table.manifest_files_dynamo_table.name
      UPLOAD_TRIGGER_QUEUE_URL = aws_sqs_queue.upload_trigger_queue.url
      DEFAULT_STORAGE_BUCKET   = data.terraform_remote_state.platform_infrastructure.outputs.storage_bucket_id
      REGION                   = var.aws_region
      RDS_PROXY_ENDPOINT       = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint
      LOG_LEVEL                = "info"
    }
  }
}

resource "aws_lambda_permission" "reconcile_allow_events" {
  statement_id  = "AllowEventBridgeInvokeReconcile"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reconcile_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.reconcile_schedule.arn
}

### ARCHIVE-SWEEPER LAMBDA
#
# Daily EventBridge sweep (see cloudwatch.tf) of manifest_table for rows
# older than MaxAgeDays (default 90) whose Status != Archived, firing an
# async Lambda.Invoke to archive_lambda for each. archive_lambda writes
# the manifest to CSV in the archive bucket and removes the manifest_files
# rows. Never mutates state itself; only dispatches.
resource "aws_lambda_function" "archive_sweeper_lambda" {
  description   = "Daily sweep of manifest_table: invokes archive_lambda for manifests older than MaxAgeDays (default 90) that aren't already Archived."
  function_name = "${var.environment_name}-${var.service_name}-archive-sweeper-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  role          = aws_iam_role.archive_sweeper_role.arn
  timeout       = 600
  memory_size   = 256
  s3_bucket     = var.lambda_bucket
  s3_key        = "${var.service_name}/archive-sweeper/upload-v2-archive-sweeper-${var.image_tag}.zip"

  environment {
    variables = {
      ENV                = var.environment_name
      MANIFEST_TABLE     = aws_dynamodb_table.manifest_dynamo_table.name
      ARCHIVE_LAMBDA_ARN = aws_lambda_function.archive_lambda.arn
      REGION             = var.aws_region
      LOG_LEVEL          = "info"
    }
  }
}

resource "aws_lambda_permission" "archive_sweeper_allow_events" {
  statement_id  = "AllowEventBridgeInvokeArchiveSweeper"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.archive_sweeper_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.archive_sweeper_schedule.arn
}

### RUN-IMPORT LAMBDA
#
# Copies a workflow run's staged results into dataset storage, one file per
# message on run_results_import_queue (see pkg/runresults): reads through the
# compute account's role (limited to the run target's prefix), writes through
# the storage credentials role (limited to the manifest's prefix), then hands
# the file to the upload lambda through upload_trigger_queue. Outside the VPC:
# it needs neither RDS nor the private subnets, only S3, STS, SQS and DynamoDB.
resource "aws_lambda_function" "run_import_lambda" {
  description   = "Copies workflow run results into dataset storage (run-results imports) and hands them to the upload lambda."
  function_name = "${var.environment_name}-${var.service_name}-run-import-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  role          = aws_iam_role.run_import_lambda_role.arn
  timeout       = 900
  # Memory buys CPU and network throughput (1769 MB is a full vCPU); a copy
  # streams through the lambda.
  memory_size = 1769
  s3_bucket   = var.lambda_bucket
  s3_key      = "${var.service_name}/runimport/upload-v2-runimport-${var.image_tag}.zip"

  environment {
    variables = {
      ENV                          = var.environment_name
      MANIFEST_FILE_TABLE          = aws_dynamodb_table.manifest_files_dynamo_table.name
      STORAGE_CREDENTIALS_ROLE_ARN = aws_iam_role.storage_credentials_role.arn
      UPLOAD_TRIGGER_QUEUE_URL     = aws_sqs_queue.upload_trigger_queue.url
      MAX_ATTEMPTS                 = local.run_import_max_attempts
      REGION                       = var.aws_region
      LOG_LEVEL                    = "info"
    }
  }
}
