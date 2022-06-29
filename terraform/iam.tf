##########################
# SQS Queue Key Policies #
##########################

data "aws_iam_policy_document" "upload_service_v2_kms_key_policy_document" {
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
      identifiers = [
        aws_iam_role.upload_service_v2_lambda_role.arn
      ]
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
# UPLOAD-SERVICE-LAMBDA   #
##############################
// 1. Lambda can assume the upload_trigger_lambda role
// 2. This role has a policy attachment
// 3. This policy has a policy document attached
// 4. This document outlines the permissions for the role

resource "aws_iam_role" "upload_service_v2_lambda_role" {
  name = "${var.environment_name}-${var.service_name}-upload-service-lambda-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

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

resource "aws_iam_role_policy_attachment" "upload_service_v2_lambda_iam_policy_attachment" {
  role       = aws_iam_role.upload_service_v2_lambda_role.name
  policy_arn = aws_iam_policy.upload_service_v2_lambda_iam_policy.arn
}

resource "aws_iam_policy" "upload_service_v2_lambda_iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-upload-service-lambda-iam-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  path   = "/"
  policy = data.aws_iam_policy_document.upload_service_v2_iam_policy_document.json
}

data "aws_iam_policy_document" "upload_service_v2_iam_policy_document" {

  statement {
    sid    = "SecretsManagerPermissions"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.docker_hub_credentials_arn,
      data.aws_kms_key.ssm_kms_key.arn,
    ]
  }

  statement {
    sid    = "UploadLambdaPermissions"
    effect = "Allow"
    actions = [
      "rds-db:connect",
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
    sid = "LambdaAccessToDynamoDB"
    effect = "Allow"

    actions = [
      "dynamodb:DescribeTable",
      "dynamodb:BatchGetItem",
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:BatchWriteItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem"
    ]

    resources = [
      aws_dynamodb_table.manifest_dynamo_table.arn,
      "${aws_dynamodb_table.manifest_dynamo_table.arn}/*",
      aws_dynamodb_table.manifest_files_dynamo_table.arn,
      "${aws_dynamodb_table.manifest_files_dynamo_table.arn}/*"
    ]

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

##############################
# MOVE-TRIGGER-LAMBDA   #
##############################

resource "aws_iam_role" "move_trigger_lambda_role" {
  name = "${var.environment_name}-${var.service_name}-move-trigger-lambda-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

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

resource "aws_iam_role_policy_attachment" "move_trigger_lambda_iam_policy_attachment" {
  role       = aws_iam_role.move_trigger_lambda_role.name
  policy_arn = aws_iam_policy.move_trigger_lambda_iam_policy.arn
}

resource "aws_iam_policy" "move_trigger_lambda_iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-move-trigger-lambda-iam-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  path   = "/"
  policy = data.aws_iam_policy_document.move_trigger_iam_policy_document.json
}

data "aws_iam_policy_document" "move_trigger_iam_policy_document" {

  statement {
    sid    = "SecretsManagerPermissions"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.docker_hub_credentials_arn,
      data.aws_kms_key.ssm_kms_key.arn,
    ]
  }

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
    sid    = "ECSTaskPermissions"
    effect = "Allow"
    actions = [
      "ecs:DescribeTasks",
      "ecs:RunTask",
      "ecs:ListTasks"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "ECSPassRole"
    effect = "Allow"
    actions = [
      "iam:PassRole",
    ]
    resources = [
      "*"
    ]
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
    sid = "LambdaAccessToDynamoDB"
    effect = "Allow"

    actions = [
      "dynamodb:DescribeTable",
      "dynamodb:BatchGetItem",
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:BatchWriteItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem"
    ]

    resources = [
      aws_dynamodb_table.manifest_dynamo_table.arn,
      "${aws_dynamodb_table.manifest_dynamo_table.arn}/*",
      aws_dynamodb_table.manifest_files_dynamo_table.arn,
      "${aws_dynamodb_table.manifest_files_dynamo_table.arn}/*"
    ]

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
      aws_sqs_queue.imported_file_queue.arn,
      "${aws_sqs_queue.imported_file_queue.arn}/*",
    ]
  }
}




// FARGATE TASK
# Create ECS Task IAM Role
resource "aws_iam_role" "fargate_task_iam_role" {
  name = "${var.environment_name}-${var.service_name}-fargate-task-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  path = "/service-roles/"

  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
    {
        "Action": "sts:AssumeRole",
        "Effect": "Allow",
        "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
        }
    }
    ]
}
EOF

}

resource "aws_iam_role_policy_attachment" "fargate_iam_role_policy_attachment" {
  role       = aws_iam_role.fargate_task_iam_role.id
  policy_arn = aws_iam_policy.iam_policy.arn
}

resource "aws_iam_policy" "iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  policy = data.aws_iam_policy_document.upload_service_v2_iam_policy_document.json
}

#data "aws_iam_policy_document" "iam_policy_document" {
#  statement {
#    sid    = "SecretsManagerPermissions"
#    effect = "Allow"
#
#    actions = [
#      "kms:Decrypt",
#      "secretsmanager:GetSecretValue",
#    ]
#
#    resources = [
#      data.terraform_remote_state.platform_infrastructure.outputs.docker_hub_credentials_arn,
#      data.aws_kms_key.ssm_kms_key.arn,
#    ]
#  }
#}
