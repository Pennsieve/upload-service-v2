# Upload Bucket Trigger Queue

resource "aws_sqs_queue" "upload_trigger_queue" {
  name                       = "${var.environment_name}-upload_trigger-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  delay_seconds              = 5
  max_message_size           = 262144
  message_retention_seconds  = 86400
  kms_master_key_id          = "alias/${var.environment_name}-upload_trigger-queue-key-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  receive_wait_time_seconds  = 10
  visibility_timeout_seconds = 30
  redrive_policy             = "{\"deadLetterTargetArn\":\"${aws_sqs_queue.upload_trigger_deadletter_queue.arn}\",\"maxReceiveCount\":3}"
}

resource "aws_sqs_queue" "upload_trigger_deadletter_queue" {
  name                      = "${var.environment_name}-upload_trigger-deadletter-queue-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  delay_seconds             = 5
  kms_master_key_id         = "alias/${var.environment_name}-upload_trigger-queue-key-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  max_message_size          = 262144
  message_retention_seconds = 86400
  receive_wait_time_seconds = 10
}

resource "aws_sqs_queue_policy" "upload_trigger_sqs_queue_policy" {
  queue_url = aws_sqs_queue.upload_trigger_queue.id
  policy    = data.aws_iam_policy_document.upload_trigger_queue_policy_document.json
}