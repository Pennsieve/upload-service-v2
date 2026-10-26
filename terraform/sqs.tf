# Upload Bucket Trigger Queue

resource "aws_sqs_queue" "upload_trigger_queue" {
  name                      = "${var.environment_name}-upload_trigger-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  delay_seconds             = 1
  max_message_size          = 262144
  message_retention_seconds = 86400
  #  receive_wait_time_seconds  = 10
  visibility_timeout_seconds = 300
  redrive_policy             = "{\"deadLetterTargetArn\":\"${aws_sqs_queue.upload_trigger_deadletter_queue.arn}\",\"maxReceiveCount\":3}"
}

resource "aws_sqs_queue" "upload_trigger_deadletter_queue" {
  name                      = "${var.environment_name}-upload_trigger-deadletter-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  delay_seconds             = 5
  max_message_size          = 262144
  message_retention_seconds = 604800 // 1 week retention in dead letter queue
  receive_wait_time_seconds = 10
}

# Mapping SQS Source to Lambda Function
resource "aws_lambda_event_source_mapping" "upload_source_mapping" {
  event_source_arn = aws_sqs_queue.upload_trigger_queue.arn
  function_name    = aws_lambda_function.upload_lambda.arn
  batch_size       = 25
  # 1s (down from 5s): the window only matters when messages arrive
  # slower than 25/batch_size — bulk uploads hit the batch cap first
  # and invoke immediately regardless. For the small-drop case (trickle
  # from eager client-side finalize) this shaves ~4s off the observed
  # finalize->Pusher latency. Concurrency brake is still
  # reserved_concurrent_executions=100 on the upload lambda.
  maximum_batching_window_in_seconds = 1
  function_response_types            = ["ReportBatchItemFailures"]
}

# Lets the EventBridge heartbeat post to the upload trigger queue.
resource "aws_sqs_queue_policy" "upload_trigger_sqs_policy" {
  queue_url = aws_sqs_queue.upload_trigger_queue.id

  policy = <<POLICY
  {
  "Version": "2012-10-17",
  "Id": "sqspolicy",
  "Statement": [
    {
      "Sid": "AllowEventBridgeHeartbeat",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": ["sqs:SendMessage"],
      "Resource": "${aws_sqs_queue.upload_trigger_queue.arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${aws_cloudwatch_event_rule.upload_lambda_heartbeat.arn}"
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": [
        "lambda:CreateEventSourceMapping",
        "lambda:ListEventSourceMappings",
        "lambda:ListFunctions"
      ],
      "Resource": "${aws_sqs_queue.upload_trigger_queue.arn}"
    }
  ]
}
POLICY
}


# Run results imports (lambda/runimport): one message per file of a
# workflow run's results to copy into a dataset, queued by the service
# lambda's POST /manifest/run-results/import. The visibility timeout covers
# the copy lambda's 15 minutes; after maxReceiveCount the lambda has marked
# the file Failed itself, so the dead-letter queue is for inspection only.
locals {
  # How often a copy is tried; the run-import lambda marks the file Failed
  # on the last attempt (MAX_ATTEMPTS).
  run_import_max_attempts = 3
}

resource "aws_sqs_queue" "run_results_import_queue" {
  name                       = "${var.environment_name}-run-results-import-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  max_message_size           = 262144
  message_retention_seconds  = 345600 // 4 days
  visibility_timeout_seconds = 960
  # Messages name compute-account roles and ExternalIds.
  sqs_managed_sse_enabled = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.run_results_import_deadletter_queue.arn
    maxReceiveCount     = local.run_import_max_attempts
  })
}

resource "aws_sqs_queue" "run_results_import_deadletter_queue" {
  name                      = "${var.environment_name}-run-results-import-deadletter-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  max_message_size          = 262144
  message_retention_seconds = 604800 // 1 week
  sqs_managed_sse_enabled   = true
}

# Only the service lambda queues copies: a job names the role a copy reads
# with and where it writes, so no one else in the account may send one.
resource "aws_sqs_queue_policy" "run_results_import_queue_policy" {
  queue_url = aws_sqs_queue.run_results_import_queue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "OnlyTheServiceLambdaSends"
      Effect    = "Deny"
      Principal = "*"
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.run_results_import_queue.arn
      Condition = {
        ArnNotEquals = { "aws:PrincipalArn" = aws_iam_role.upload_service_v2_lambda_role.arn }
      }
    }]
  })
}

resource "aws_lambda_event_source_mapping" "run_results_import_source_mapping" {
  event_source_arn = aws_sqs_queue.run_results_import_queue.arn
  function_name    = aws_lambda_function.run_import_lambda.arn
  # One copy per invocation: a copy can take minutes, and a retry repeats
  # only its own file.
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
  scaling_config {
    maximum_concurrency = 20
  }
}
