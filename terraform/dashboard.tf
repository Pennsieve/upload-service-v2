###########################################
# UPLOAD SERVICE — OPERATIONAL DASHBOARD  #
###########################################
#
# One consolidated view of the upload pipeline. Intended for "is this system
# healthy right now?" at a glance plus enough detail to drive a debug
# session when something goes wrong. Organized top-down from user-facing
# surfaces (API Gateway, service lambda) through the pipeline (SQS queues,
# upload lambda, Fargate) to the scheduled housekeeping (reconcile,
# archive-sweeper) and the storage layer (DynamoDB).
#
# Widget grid is 24 columns wide; rows can mix widths.

locals {
  dash_region = var.aws_region

  dash_lambda_functions = {
    service         = aws_lambda_function.service_lambda.function_name
    upload          = aws_lambda_function.upload_lambda.function_name
    archive         = aws_lambda_function.archive_lambda.function_name
    fargate_trigger = aws_lambda_function.fargate_trigger_lambda.function_name
    reconcile       = aws_lambda_function.reconcile_lambda.function_name
    archive_sweeper = aws_lambda_function.archive_sweeper_lambda.function_name
  }

  dash_queues = {
    upload_trigger     = aws_sqs_queue.upload_trigger_queue.name
    upload_trigger_dlq = aws_sqs_queue.upload_trigger_deadletter_queue.name
    imported_file      = aws_sqs_queue.imported_file_queue.name
    imported_file_dlq  = aws_sqs_queue.imported_file_deadletter_queue.name
  }
}

resource "aws_cloudwatch_dashboard" "upload_service_overview" {
  dashboard_name = "${var.environment_name}-upload-service-overview-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  dashboard_body = jsonencode({
    widgets = [
      # ========= Row 1: hero metrics =========
      {
        type = "text"
        x    = 0, y = 0, width = 24, height = 2
        properties = {
          markdown = "# Upload Service — ${var.environment_name}\nPipeline health across service lambda, upload lambda, SQS queues, DynamoDB, and the scheduled reconcile / archive-sweeper jobs. Alarm SNS topic: **${aws_sns_topic.reconcile_alerts.name}**."
        }
      },
      {
        type = "metric"
        x    = 0, y = 2, width = 8, height = 4
        properties = {
          title  = "Lambda errors (5m sum)"
          view   = "singleValue"
          stat   = "Sum"
          period = 300
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "Errors", "FunctionName", local.dash_lambda_functions.service, { label = "service" }],
            [".", ".", ".", local.dash_lambda_functions.upload, { label = "upload" }],
            [".", ".", ".", local.dash_lambda_functions.archive, { label = "archive" }],
            [".", ".", ".", local.dash_lambda_functions.fargate_trigger, { label = "fargate-trigger" }],
            [".", ".", ".", local.dash_lambda_functions.reconcile, { label = "reconcile" }],
            [".", ".", ".", local.dash_lambda_functions.archive_sweeper, { label = "archive-sweeper" }],
          ]
        }
      },
      {
        type = "metric"
        x    = 8, y = 2, width = 8, height = 4
        properties = {
          title  = "DLQ depth — any message here is a bug"
          view   = "singleValue"
          stat   = "Maximum"
          period = 300
          region = local.dash_region
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", local.dash_queues.upload_trigger_dlq, { label = "upload-trigger-dlq" }],
            [".", ".", ".", local.dash_queues.imported_file_dlq, { label = "imported-file-dlq" }],
          ]
        }
      },
      {
        type = "metric"
        x    = 16, y = 2, width = 8, height = 4
        properties = {
          title  = "Reconcile missing + errors (24h)"
          view   = "singleValue"
          stat   = "Sum"
          period = 86400
          region = local.dash_region
          metrics = [
            ["UploadService/Reconcile", "OrphansMissing", { label = "OrphansMissing" }],
            [".", "ReconciliationErrors", { label = "ReconciliationErrors" }],
            [".", "OrphansRecovered", { label = "OrphansRecovered" }],
            [".", "EnqueueFailed", { label = "EnqueueFailed" }],
          ]
        }
      },

      # ========= Row 2: service lambda (finalize endpoint) =========
      {
        type       = "text"
        x          = 0, y = 6, width = 24, height = 1
        properties = { markdown = "## Service lambda (finalize / manifest API)" }
      },
      {
        type = "metric"
        x    = 0, y = 7, width = 12, height = 5
        properties = {
          title   = "Service lambda latency"
          view    = "timeSeries"
          stacked = false
          region  = local.dash_region
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", local.dash_lambda_functions.service, { stat = "p50", label = "p50" }],
            ["...", { stat = "p95", label = "p95" }],
            ["...", { stat = "p99", label = "p99" }],
          ]
          yAxis = { left = { label = "ms", showUnits = false } }
        }
      },
      {
        type = "metric"
        x    = 12, y = 7, width = 6, height = 5
        properties = {
          title  = "Service lambda invocations"
          view   = "timeSeries"
          stat   = "Sum"
          period = 60
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", local.dash_lambda_functions.service],
          ]
        }
      },
      {
        type = "metric"
        x    = 18, y = 7, width = 6, height = 5
        properties = {
          title  = "Service lambda concurrent execs"
          view   = "timeSeries"
          stat   = "Maximum"
          period = 60
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "ConcurrentExecutions", "FunctionName", local.dash_lambda_functions.service],
          ]
        }
      },

      # ========= Row 3: upload lambda (S3-event consumer + finalize dispatch target) =========
      {
        type       = "text"
        x          = 0, y = 12, width = 24, height = 1
        properties = { markdown = "## Upload lambda (SQS consumer — ImportFiles)" }
      },
      {
        type = "metric"
        x    = 0, y = 13, width = 12, height = 5
        properties = {
          title  = "Upload lambda latency"
          view   = "timeSeries"
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", local.dash_lambda_functions.upload, { stat = "p50", label = "p50" }],
            ["...", { stat = "p95", label = "p95" }],
            ["...", { stat = "p99", label = "p99" }],
          ]
          yAxis = { left = { label = "ms", showUnits = false } }
        }
      },
      {
        type = "metric"
        x    = 12, y = 13, width = 6, height = 5
        properties = {
          title  = "Upload lambda invocations"
          view   = "timeSeries"
          stat   = "Sum"
          period = 60
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", local.dash_lambda_functions.upload],
          ]
        }
      },
      {
        type = "metric"
        x    = 18, y = 13, width = 6, height = 5
        properties = {
          title  = "Upload lambda throttles + errors"
          view   = "timeSeries"
          stat   = "Sum"
          period = 60
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "Errors", "FunctionName", local.dash_lambda_functions.upload, { label = "errors" }],
            [".", "Throttles", ".", ".", { label = "throttles" }],
          ]
        }
      },

      # ========= Row 4: SQS queues =========
      {
        type       = "text"
        x          = 0, y = 18, width = 24, height = 1
        properties = { markdown = "## SQS queues (backlog + flow rates)" }
      },
      {
        type = "metric"
        x    = 0, y = 19, width = 12, height = 5
        properties = {
          title  = "Queue backlog (ApproximateNumberOfMessagesVisible)"
          view   = "timeSeries"
          stat   = "Maximum"
          period = 60
          region = local.dash_region
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", local.dash_queues.upload_trigger, { label = "upload-trigger" }],
            [".", ".", ".", local.dash_queues.imported_file, { label = "imported-file" }],
            [".", ".", ".", local.dash_queues.upload_trigger_dlq, { label = "upload-trigger-DLQ" }],
            [".", ".", ".", local.dash_queues.imported_file_dlq, { label = "imported-file-DLQ" }],
          ]
        }
      },
      {
        type = "metric"
        x    = 12, y = 19, width = 12, height = 5
        properties = {
          title  = "Upload-trigger queue flow (sent vs deleted)"
          view   = "timeSeries"
          stat   = "Sum"
          period = 60
          region = local.dash_region
          metrics = [
            ["AWS/SQS", "NumberOfMessagesSent", "QueueName", local.dash_queues.upload_trigger, { label = "sent" }],
            [".", "NumberOfMessagesDeleted", ".", ".", { label = "deleted (processed)" }],
            [".", "ApproximateAgeOfOldestMessage", ".", ".", { label = "oldest-age (s)", yAxis = "right" }],
          ]
          yAxis = { right = { label = "seconds", showUnits = false } }
        }
      },

      # ========= Row 5: DynamoDB health =========
      {
        type       = "text"
        x          = 0, y = 24, width = 24, height = 1
        properties = { markdown = "## DynamoDB" }
      },
      {
        type = "metric"
        x    = 0, y = 25, width = 8, height = 5
        properties = {
          title  = "manifest_files RCU/WCU"
          view   = "timeSeries"
          stat   = "Sum"
          period = 60
          region = local.dash_region
          metrics = [
            ["AWS/DynamoDB", "ConsumedReadCapacityUnits", "TableName", aws_dynamodb_table.manifest_files_dynamo_table.name, { label = "RCU" }],
            [".", "ConsumedWriteCapacityUnits", ".", ".", { label = "WCU" }],
          ]
        }
      },
      {
        type = "metric"
        x    = 8, y = 25, width = 8, height = 5
        properties = {
          title  = "manifest_files throttles + errors"
          view   = "timeSeries"
          stat   = "Sum"
          period = 60
          region = local.dash_region
          metrics = [
            ["AWS/DynamoDB", "ThrottledRequests", "TableName", aws_dynamodb_table.manifest_files_dynamo_table.name, { label = "throttles" }],
            [".", "SystemErrors", ".", ".", { label = "system errors" }],
            [".", "UserErrors", ".", ".", { label = "user errors" }],
          ]
        }
      },
      {
        type = "metric"
        x    = 16, y = 25, width = 8, height = 5
        properties = {
          title  = "manifest_table RCU/WCU"
          view   = "timeSeries"
          stat   = "Sum"
          period = 60
          region = local.dash_region
          metrics = [
            ["AWS/DynamoDB", "ConsumedReadCapacityUnits", "TableName", aws_dynamodb_table.manifest_dynamo_table.name, { label = "RCU" }],
            [".", "ConsumedWriteCapacityUnits", ".", ".", { label = "WCU" }],
          ]
        }
      },

      # ========= Row 6: reconcile-orphans =========
      {
        type       = "text"
        x          = 0, y = 30, width = 24, height = 1
        properties = { markdown = "## reconcile-orphans (daily 07:00 UTC)" }
      },
      {
        type = "metric"
        x    = 0, y = 31, width = 12, height = 5
        properties = {
          title  = "Reconcile outcomes (per run)"
          view   = "timeSeries"
          stat   = "Sum"
          period = 86400
          region = local.dash_region
          metrics = [
            ["UploadService/Reconcile", "OrphansRecovered", { label = "recovered (enqueued)" }],
            [".", "OrphansMissing", { label = "missing (flipped to FailedOrphan)" }],
            [".", "EnqueueFailed", { label = "enqueue failed" }],
            [".", "ReconciliationErrors", { label = "internal errors" }],
          ]
        }
      },
      {
        type = "metric"
        x    = 12, y = 31, width = 6, height = 5
        properties = {
          title  = "Reconcile lambda duration"
          view   = "timeSeries"
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", local.dash_lambda_functions.reconcile, { stat = "p50", label = "p50" }],
            ["...", { stat = "p99", label = "p99" }],
          ]
          yAxis = { left = { label = "ms", showUnits = false } }
        }
      },
      {
        type = "metric"
        x    = 18, y = 31, width = 6, height = 5
        properties = {
          title  = "Reconcile lambda errors"
          view   = "timeSeries"
          stat   = "Sum"
          period = 3600
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "Errors", "FunctionName", local.dash_lambda_functions.reconcile],
          ]
        }
      },

      # ========= Row 7: archive-sweeper =========
      {
        type       = "text"
        x          = 0, y = 36, width = 24, height = 1
        properties = { markdown = "## archive-sweeper (daily 07:30 UTC, 90d grace)" }
      },
      {
        type = "metric"
        x    = 0, y = 37, width = 12, height = 5
        properties = {
          title  = "Archive-sweeper outcomes (per run)"
          view   = "timeSeries"
          stat   = "Sum"
          period = 86400
          region = local.dash_region
          metrics = [
            ["UploadService/ArchiveSweeper", "ManifestsEligible", { label = "eligible" }],
            [".", "InvokesAttempted", { label = "archive invokes attempted" }],
            [".", "InvokesFailed", { label = "archive invokes failed" }],
            [".", "SweeperErrors", { label = "internal errors" }],
          ]
        }
      },
      {
        type = "metric"
        x    = 12, y = 37, width = 12, height = 5
        properties = {
          title  = "Archive lambda activity"
          view   = "timeSeries"
          stat   = "Sum"
          period = 3600
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", local.dash_lambda_functions.archive, { label = "invocations" }],
            [".", "Errors", ".", ".", { label = "errors" }],
          ]
        }
      },

      # ========= Row 8: Fargate move task =========
      {
        type       = "text"
        x          = 0, y = 42, width = 24, height = 1
        properties = { markdown = "## Fargate move task (legacy upload-bucket path)" }
      },
      {
        type = "metric"
        x    = 0, y = 43, width = 12, height = 5
        properties = {
          title  = "fargate-trigger invocations + errors"
          view   = "timeSeries"
          stat   = "Sum"
          period = 300
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", local.dash_lambda_functions.fargate_trigger, { label = "invocations" }],
            [".", "Errors", ".", ".", { label = "errors" }],
          ]
        }
      },
      {
        type = "metric"
        x    = 12, y = 43, width = 12, height = 5
        properties = {
          title  = "fargate-trigger duration"
          view   = "timeSeries"
          region = local.dash_region
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", local.dash_lambda_functions.fargate_trigger, { stat = "p50", label = "p50" }],
            ["...", { stat = "p99", label = "p99" }],
          ]
          yAxis = { left = { label = "ms", showUnits = false } }
        }
      },
    ]
  })
}
