# Upload trigger KMS key

resource "aws_kms_alias" "upload_trigger_sqs_kms_key_alias" {
  name          = "alias/${var.environment_name}-upload_trigger-queue-key-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  target_key_id = aws_kms_key.upload_trigger_sqs_kms_key.key_id
}

resource "aws_kms_key" "upload_trigger_sqs_kms_key" {
  description             = "${var.environment_name}-upload_trigger-queue-key-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  deletion_window_in_days = 10
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.upload_trigger_queue_kms_key_policy_document.json
}