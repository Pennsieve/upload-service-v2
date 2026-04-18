###############################
# RECONCILE-ORPHANS LAMBDA   #
###############################
#
# Two invocation modes (same function, same handler):
#
#  1. One-shot manual recovery — payload {"manifestNodeId": "<uuid>"}.
#     Operator invokes via `aws lambda invoke` against a specific manifest
#     (typically to recover stuck Registered rows from a prior incident).
#
#  2. Scheduled daily sweep — payload {"gracePeriodHours": 6} from an
#     EventBridge rule. Scans the StatusIndex GSI for all stuck Registered
#     rows past the grace period, HEAD-checks each expected storage key,
#     and enqueues any recoverable files to upload_trigger_queue. Never
#     deletes; missing-object cases are metric'd + logged only.

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

##############################
# RECONCILE LAMBDA IAM ROLE  #
##############################

resource "aws_iam_role" "reconcile_lambda_role" {
  name = "${var.environment_name}-${var.service_name}-reconcile-lambda-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "reconcile_lambda_policy_attachment" {
  role       = aws_iam_role.reconcile_lambda_role.name
  policy_arn = aws_iam_policy.reconcile_lambda_policy.arn
}

resource "aws_iam_policy" "reconcile_lambda_policy" {
  name   = "${var.environment_name}-${var.service_name}-reconcile-lambda-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  policy = data.aws_iam_policy_document.reconcile_lambda_policy_document.json
}

data "aws_iam_policy_document" "reconcile_lambda_policy_document" {
  # CloudWatch + VPC
  statement {
    sid    = "LambdaBaseExec"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:CreateLogGroup",
      "ec2:CreateNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface",
      "ec2:AssignPrivateIpAddresses",
      "ec2:UnassignPrivateIpAddresses",
    ]
    resources = ["*"]
  }

  # DynamoDB read on both manifest tables (scans StatusIndex GSI + reads
  # manifest metadata).
  statement {
    sid    = "ReconcileDynamoRead"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:BatchGetItem",
    ]
    resources = [
      aws_dynamodb_table.manifest_dynamo_table.arn,
      "${aws_dynamodb_table.manifest_dynamo_table.arn}/*",
      aws_dynamodb_table.manifest_files_dynamo_table.arn,
      "${aws_dynamodb_table.manifest_files_dynamo_table.arn}/*",
    ]
  }

  # Postgres connection via RDS Proxy (for storage-bucket resolution).
  statement {
    sid    = "ReconcileRDS"
    effect = "Allow"
    actions = [
      "rds-db:connect",
    ]
    resources = ["*"]
  }

  # Enqueue synthesized S3 events to upload_trigger_queue.
  statement {
    sid    = "ReconcileEnqueue"
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:SendMessageBatch",
    ]
    resources = [
      aws_sqs_queue.upload_trigger_queue.arn,
    ]
  }
}

# Static bucket read is needed to HEAD objects during verification.
# Dynamic workspace buckets are covered by the account-service managed
# policy attached below.
resource "aws_iam_role_policy_attachment" "reconcile_storage_bucket_read" {
  role       = aws_iam_role.reconcile_lambda_role.name
  policy_arn = data.terraform_remote_state.account_service.outputs.storage_read_policy_arn
}

###############################
# EVENTBRIDGE SCHEDULE (OPT 2) #
###############################
#
# Fires the reconcile-orphans lambda daily with a 6-hour grace period.
# The interleaved sync+upload in the agent keeps files in Registered for
# seconds to minutes per file even on large manifests, so 6 hours is a safe
# buffer for single-day legitimate uploads.

resource "aws_cloudwatch_event_rule" "reconcile_schedule" {
  name                = "${var.environment_name}-${var.service_name}-reconcile-daily-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  description         = "Daily sweep for stuck-Registered manifest files past the 6h grace period."
  schedule_expression = "cron(0 7 * * ? *)" # 07:00 UTC = 02:00 US/Central = 03:00 US/Eastern
}

resource "aws_cloudwatch_event_target" "reconcile_schedule_target" {
  rule      = aws_cloudwatch_event_rule.reconcile_schedule.name
  target_id = "reconcile-lambda"
  arn       = aws_lambda_function.reconcile_lambda.arn
  input     = jsonencode({ gracePeriodHours = 6 })
}

resource "aws_lambda_permission" "reconcile_allow_events" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reconcile_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.reconcile_schedule.arn
}

###############################
# ALARMS                      #
###############################

resource "aws_sns_topic" "reconcile_alerts" {
  name = "${var.environment_name}-${var.service_name}-reconcile-alerts-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
}

# Persistent missing-object count — files the reconciler repeatedly finds
# in Registered status with no corresponding S3 object. A spike indicates
# client-side upload failures that the agent isn't re-attempting, and
# warrants operator follow-up.
resource "aws_cloudwatch_metric_alarm" "orphans_missing" {
  alarm_name          = "${var.environment_name}-${var.service_name}-reconcile-orphans-missing-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  alarm_description   = "Reconciler found >10 manifest_files rows in Registered status with no S3 object in a single run. Likely client-side upload failures."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "OrphansMissing"
  namespace           = "UploadService/Reconcile"
  period              = 86400
  statistic           = "Sum"
  threshold           = 10
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.reconcile_alerts.arn]
}

# Internal errors during reconciliation itself (DB failure, IAM, etc.).
resource "aws_cloudwatch_metric_alarm" "reconciliation_errors" {
  alarm_name          = "${var.environment_name}-${var.service_name}-reconcile-errors-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  alarm_description   = "Reconciler encountered internal errors; investigate CloudWatch logs."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ReconciliationErrors"
  namespace           = "UploadService/Reconcile"
  period              = 86400
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.reconcile_alerts.arn]
}
