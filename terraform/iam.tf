##########################
# SQS Queue Key Policies #
##########################

data "aws_iam_policy_document" "upload_trigger_kms_key_policy_document" {
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
    sid    = "Allow specific lambda to use this key"
    effect = "Allow"

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:GenerateDataKey*"
    ]

    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.upload_trigger_lambda_role.arn]
    }

  }

  statement {
    sid    = "Enable S3 "
    effect = "Allow"

    actions = [
      "kms:GenerateDataKey",
      "kms:Decrypt",
    ]

    resources = ["*"]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }
  }

}

data "aws_iam_policy_document" "uploads_bucket_iam_policy_document" {

  statement {
    sid    = "ForceSSLOnlyAccess"
    effect = "Deny"

    resources = [
      "arn:aws:s3:::pennsieve-${var.environment_name}-uploads-v2-${data.terraform_remote_state.region.outputs.aws_region_shortname}",
      "arn:aws:s3:::pennsieve-${var.environment_name}-uploads-v2-${data.terraform_remote_state.region.outputs.aws_region_shortname}/*",
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


##############################
# UPLOAD-TRIGGER-LAMBDA   #
##############################
// 1. Lambda can assume the upload_trigger_lambda role
// 2. This role has a policy attachment
// 3. This policy has a policy document attached
// 4. This document outlines the permissions for the role

resource "aws_iam_role" "upload_trigger_lambda_role" {
  name = "${var.environment_name}-${var.service_name}-upload-trigger-lambda-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "upload_trigger_lambda_iam_policy_attachment" {
  role       = aws_iam_role.upload_trigger_lambda_role.name
  policy_arn = aws_iam_policy.upload_trigger_lambda_iam_policy.arn
}


resource "aws_iam_policy" "upload_trigger_lambda_iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-upload-trigger-lambda-iam-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  path   = "/"
  policy = data.aws_iam_policy_document.upload_trigger_lambda_iam_policy_document.json
}

data "aws_iam_policy_document" "upload_trigger_lambda_iam_policy_document" {
  statement {
    sid    = "UploadLambdaPermissions"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutDestination",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
      "ec2:CreateNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface",
      "ec2:AssignPrivateIpAddresses",
      "ec2:UnassignPrivateIpAddresses"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "SSMPermissions"
    effect = "Allow"

    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:GetParametersByPath",
    ]

    resources = ["arn:aws:ssm:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:parameter/${var.environment_name}/${var.service_name}/*"]
  }

  statement {
    sid    = "LambdaReadFromEventsPermission"
    effect = "Allow"

    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl"
    ]

    resources = [
      aws_sqs_queue.upload_trigger_queue.arn,
      "${aws_sqs_queue.upload_trigger_queue.arn}/*",
    ]
  }
}