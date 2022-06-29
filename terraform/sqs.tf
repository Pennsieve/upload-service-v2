# Upload Bucket Trigger Queue

resource "aws_sqs_queue" "upload_trigger_queue" {
  name                       = "${var.environment_name}-upload_trigger-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  delay_seconds              = 1
  max_message_size           = 262144
  message_retention_seconds  = 86400
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
  batch_size = 100
  maximum_batching_window_in_seconds = 5
}

# Grant SNS to post to SQS queue
resource "aws_sqs_queue_policy" "upload_trigger_sqs_policy" {
  queue_url = aws_sqs_queue.upload_trigger_queue.id

  policy = <<POLICY
  {
  "Version": "2012-10-17",
  "Id": "sqspolicy",
  "Statement": [
    {
      "Sid":"1",
      "Effect": "Allow",
      "Principal": {
         "Service": "s3.amazonaws.com"
      },
      "Action": ["sqs:SendMessage"],
      "Resource": "${aws_sqs_queue.upload_trigger_queue.arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${aws_sns_topic.imported_file_sns_topic.arn}"
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


####################
## IMPORTED QUEUE ##
####################
resource "aws_sqs_queue" "imported_file_queue" {
  name                       = "${var.environment_name}-imported-file-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  delay_seconds              = 1
  max_message_size           = 262144
  message_retention_seconds  = 86400
  #  receive_wait_time_seconds  = 10
  visibility_timeout_seconds = 300
  redrive_policy             = "{\"deadLetterTargetArn\":\"${aws_sqs_queue.imported_file_deadletter_queue.arn}\",\"maxReceiveCount\":3}"
}

resource "aws_sqs_queue" "imported_file_deadletter_queue" {
  name                      = "${var.environment_name}-imported_file-deadletter-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  delay_seconds             = 5
  max_message_size          = 262144
  message_retention_seconds = 604800 // 1 week retention in dead letter queue
  receive_wait_time_seconds = 10
}

# Mapping SQS Source to Lambda Function
resource "aws_lambda_event_source_mapping" "imported_file_mapping" {
  event_source_arn = aws_sqs_queue.imported_file_queue.arn
  function_name    = aws_lambda_function.fargate_trigger_lambda.function_name
  batch_size = 100
  maximum_batching_window_in_seconds = 300
}

# Grant SNS to post to SQS queue
resource "aws_sqs_queue_policy" "imported_file_sqs_policy" {
  queue_url = aws_sqs_queue.imported_file_queue.id

  policy = <<POLICY
  {
  "Version": "2012-10-17",
  "Id": "sqspolicy",
  "Statement": [
    {
      "Sid":"1",
      "Effect": "Allow",
      "Principal": {
         "Service": "s3.amazonaws.com"
      },
      "Action": ["sqs:SendMessage"],
      "Resource": "${aws_sqs_queue.imported_file_queue.arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${aws_lambda_function.upload_lambda.arn}"
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
      "Resource": "${aws_sqs_queue.imported_file_queue.arn}"
    }
  ]
}
POLICY
}