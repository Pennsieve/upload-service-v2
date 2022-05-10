data "aws_iam_policy_document" "upload_trigger_queue_kms_key_policy_document" {
  statement {
    sid       = "Enable IAM User Permissions"
    effect    = "Allow"
    actions   = ["kms:*"]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.terraform_remote_state.account.outputs.aws_account_id}:root"]
    }
  }

  statement {
    sid    = "Enable Cloudwatch Event Permissions"
    effect = "Allow"

    actions = [
      "kms:GenerateDataKey",
      "kms:Decrypt",
    ]

    resources = ["*"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "upload_trigger_queue_policy_document" {
  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.upload_trigger_queue.arn]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "uploads_bucket_force_ssl_iam_policy_document" {

  statement {
    sid    = "ForceSSLOnlyAccess"
    effect = "Deny"

    resources = [
      "arn:aws:s3:::pennsieve-${var.environment_name}-uploads-${data.terraform_remote_state.region.outputs.aws_region_shortname}",
      "arn:aws:s3:::pennsieve-${var.environment_name}-uploads-${data.terraform_remote_state.region.outputs.aws_region_shortname}/*",
    ]

    actions = [
      "s3:*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      values   = ["false"]
      variable = "aws:SecureTransport"
    }
  }
}