# Platform Events SQS Cloudwatch DLQ Alarm
resource "aws_cloudwatch_metric_alarm" "upload_sqs_dlq_cloudwatch_metric_alarm" {
  alarm_name                = "${var.environment_name}-upload-sqs-deadletter-queue-alarm-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "ApproximateNumberOfMessagesVisible"
  namespace                 = "AWS/SQS"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "1"
  alarm_description         = "This metric monitors Upload-V2 SQS DLQ for messages"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_id]
  ok_actions                = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_id]
  treat_missing_data        = "ignore"

  dimensions = {
    QueueName = aws_sqs_queue.upload_trigger_deadletter_queue.name
  }
}

// CREATE FARGATE TASK CLOUDWATCH LOG GROUP
resource "aws_cloudwatch_log_group" "fargate_cloudwatch_log_group" {
  name              = "/aws/fargate/${var.environment_name}-${var.service_name}-${var.tier}-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  retention_in_days = 14

  tags = local.common_tags
}

// Send logs from upload trigger service to datadog
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_fargate_group_subscription" {
  name            = "${aws_cloudwatch_log_group.fargate_cloudwatch_log_group.name}-subscription"
  log_group_name  = aws_cloudwatch_log_group.fargate_cloudwatch_log_group.name
  filter_pattern  = ""
  destination_arn = data.terraform_remote_state.region.outputs.datadog_delivery_stream_arn
  role_arn        = data.terraform_remote_state.region.outputs.cw_logs_to_datadog_logs_firehose_role_arn
}

// Create log group for upload lambda.
resource "aws_cloudwatch_log_group" "upload_lambda_loggroup" {
  name              = "/aws/lambda/${aws_lambda_function.upload_lambda.function_name}"
  retention_in_days = 30
  tags              = local.common_tags
}

// Send logs from upload trigger lambda to datadog
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_log_group_subscription" {
  name            = "${aws_cloudwatch_log_group.upload_lambda_loggroup.name}-subscription"
  log_group_name  = aws_cloudwatch_log_group.upload_lambda_loggroup.name
  filter_pattern  = ""
  destination_arn = data.terraform_remote_state.region.outputs.datadog_delivery_stream_arn
  role_arn        = data.terraform_remote_state.region.outputs.cw_logs_to_datadog_logs_firehose_role_arn
}

// Create log group for upload lambda.
resource "aws_cloudwatch_log_group" "upload_service_lambda_loggroup" {
  name              = "/aws/lambda/${aws_lambda_function.service_lambda.function_name}"
  retention_in_days = 30
  tags              = local.common_tags
}

// Send logs from upload trigger service to datadog
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_upload_service_group_subscription" {
  name            = "${aws_cloudwatch_log_group.upload_service_lambda_loggroup.name}-subscription"
  log_group_name  = aws_cloudwatch_log_group.upload_service_lambda_loggroup.name
  filter_pattern  = ""
  destination_arn = data.terraform_remote_state.region.outputs.datadog_delivery_stream_arn
  role_arn        = data.terraform_remote_state.region.outputs.cw_logs_to_datadog_logs_firehose_role_arn
}

// Create log group for archiver lambda.
resource "aws_cloudwatch_log_group" "archiver_lambda_loggroup" {
  name              = "/aws/lambda/${aws_lambda_function.archive_lambda.function_name}"
  retention_in_days = 7
  tags              = local.common_tags
}

// Send logs from upload trigger service to datadog
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_archiver_group_subscription" {
  name            = "${aws_cloudwatch_log_group.archiver_lambda_loggroup.name}-subscription"
  log_group_name  = aws_cloudwatch_log_group.archiver_lambda_loggroup.name
  filter_pattern  = ""
  destination_arn = data.terraform_remote_state.region.outputs.datadog_delivery_stream_arn
  role_arn        = data.terraform_remote_state.region.outputs.cw_logs_to_datadog_logs_firehose_role_arn
}

// Accounts SERVICE API GATEWAY
resource "aws_cloudwatch_log_group" "upload_service_gateway_log_group" {
  name = "${var.environment_name}/${var.service_name}/upload-api-gateway"

  retention_in_days = 30
}

// Log group for reconcile lambda.
resource "aws_cloudwatch_log_group" "reconcile_lambda_loggroup" {
  name              = "/aws/lambda/${aws_lambda_function.reconcile_lambda.function_name}"
  retention_in_days = 30
  tags              = local.common_tags
}

// Send reconcile lambda logs to datadog
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_reconcile_group_subscription" {
  name            = "${aws_cloudwatch_log_group.reconcile_lambda_loggroup.name}-subscription"
  log_group_name  = aws_cloudwatch_log_group.reconcile_lambda_loggroup.name
  filter_pattern  = ""
  destination_arn = data.terraform_remote_state.region.outputs.datadog_delivery_stream_arn
  role_arn        = data.terraform_remote_state.region.outputs.cw_logs_to_datadog_logs_firehose_role_arn
}

// Log group for archive-sweeper lambda.
resource "aws_cloudwatch_log_group" "archive_sweeper_lambda_loggroup" {
  name              = "/aws/lambda/${aws_lambda_function.archive_sweeper_lambda.function_name}"
  retention_in_days = 30
  tags              = local.common_tags
}

// Send archive-sweeper lambda logs to datadog
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_archive_sweeper_group_subscription" {
  name            = "${aws_cloudwatch_log_group.archive_sweeper_lambda_loggroup.name}-subscription"
  log_group_name  = aws_cloudwatch_log_group.archive_sweeper_lambda_loggroup.name
  filter_pattern  = ""
  destination_arn = data.terraform_remote_state.region.outputs.datadog_delivery_stream_arn
  role_arn        = data.terraform_remote_state.region.outputs.cw_logs_to_datadog_logs_firehose_role_arn
}

######################################
# RECONCILE SCHEDULE + ALARMS        #
######################################
#
# Fires the reconcile-orphans lambda daily with a 6-hour grace period. The
# interleaved sync+upload in the agent keeps files in Registered for seconds
# to minutes per file even on large manifests, so 6 hours is a safe buffer
# for single-day legitimate uploads.

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

# Persistent missing-object count — files the reconciler repeatedly finds in
# Registered status with no corresponding S3 object. A spike indicates
# client-side upload failures that the agent isn't re-attempting, and warrants
# operator follow-up.
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

######################################
# UPLOAD LAMBDA HEARTBEAT            #
######################################
#
# Keeps the upload lambda's SQS pollers and execution environment warm.
# Without this, idle periods (quiet hours / low traffic) scale pollers
# down; the first finalize after a lull then waits 10-20s for Lambda to
# scale them back up. Measured end-to-end finalize->Pusher delay dropped
# from 10-22s to 6-8s once a steady trickle kept pollers warm.
#
# Fires every minute. The upload handler (store.go Handler) detects
# heartbeat messages by their missing S3 Records and returns without
# processing. Cost: ~43k invocations/month on a 512MB lambda — pennies.

resource "aws_cloudwatch_event_rule" "upload_lambda_heartbeat" {
  name                = "${var.environment_name}-${var.service_name}-upload-heartbeat-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  description         = "Pings upload_trigger_queue every minute to keep the upload lambda's SQS pollers warm."
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "upload_lambda_heartbeat_target" {
  rule      = aws_cloudwatch_event_rule.upload_lambda_heartbeat.name
  target_id = "upload-trigger-queue-heartbeat"
  arn       = aws_sqs_queue.upload_trigger_queue.arn
  input     = jsonencode({ heartbeat = true })
}

######################################
# ARCHIVE-SWEEPER SCHEDULE + ALARMS  #
######################################
#
# Daily sweep of un-archived manifests past MaxAgeDays (default 90). Fires at
# 07:30 UTC, 30 min after the reconcile schedule to avoid overlap.

resource "aws_cloudwatch_event_rule" "archive_sweeper_schedule" {
  name                = "${var.environment_name}-${var.service_name}-archive-sweeper-daily-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  description         = "Daily sweep of old un-archived manifests. Fires archive_lambda for manifests older than 90 days (tunable via payload)."
  schedule_expression = "cron(30 7 * * ? *)"
}

resource "aws_cloudwatch_event_target" "archive_sweeper_target" {
  rule      = aws_cloudwatch_event_rule.archive_sweeper_schedule.name
  target_id = "archive-sweeper-lambda"
  arn       = aws_lambda_function.archive_sweeper_lambda.arn
  # maxInvokesPerRun kept low so parallel archive-lambdas don't saturate the
  # manifest_files GSI write-capacity. 500 was observed to trigger
  # ThrottlingException on StatusIndex until DynamoDB auto-scaled. The daily
  # run will drain any backlog across consecutive days rather than in a single
  # burst.
  input = jsonencode({
    maxAgeDays       = 90
    maxInvokesPerRun = 50
  })
}

resource "aws_cloudwatch_metric_alarm" "archive_sweeper_invoke_failures" {
  alarm_name          = "${var.environment_name}-${var.service_name}-archive-sweeper-invoke-failures-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  alarm_description   = "Archive-sweeper failed to invoke archive_lambda for one or more manifests. Check CloudWatch logs."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "InvokesFailed"
  namespace           = "UploadService/ArchiveSweeper"
  period              = 86400
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.reconcile_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "archive_sweeper_errors" {
  alarm_name          = "${var.environment_name}-${var.service_name}-archive-sweeper-errors-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  alarm_description   = "Archive-sweeper encountered internal errors; investigate CloudWatch logs."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "SweeperErrors"
  namespace           = "UploadService/ArchiveSweeper"
  period              = 86400
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.reconcile_alerts.arn]
}
