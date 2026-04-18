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
      type = "AWS"
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
    sid    = "UploadsBucketAccess"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:GetObject",
      "s3:GetObjectAttributes",
      "s3:DeleteObject"
    ]

    resources = [
      aws_s3_bucket.uploads_s3_bucket.arn,
      "${aws_s3_bucket.uploads_s3_bucket.arn}/*",
    ]
  }

  statement {
    sid    = "ArchiverBucketAccess"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:GetObject",
      "s3:GetObjectAttributes",
      "s3:DeleteObject",
      "s3:PutObject",
      "s3:ListBucketMultipartUploads",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
      "s3:PutObjectTagging"
    ]

    resources = [
      aws_s3_bucket.manifest_archive_bucket.arn,
      "${aws_s3_bucket.manifest_archive_bucket.arn}/*",
    ]
  }

  // Allow upload handler to decrypt the SHA256 checksum on s3 objects
  statement {
    sid    = "AwsKmsKeyAccess"
    effect = "Allow"

    actions = [
      "kms:Decrypt"
    ]

    resources = [
      "arn:aws:kms:*:${data.terraform_remote_state.account.outputs.aws_account_id}:key/*",
    ]
  }

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

    resources = [
      "arn:aws:ssm:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:parameter/${var.environment_name}/${var.service_name}/*",
    "arn:aws:ssm:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:parameter/ops/*"]
  }

  statement {
    sid    = "LambdaAccessToDynamoDB"
    effect = "Allow"

    actions = [
      "dynamodb:DeleteItem",
      "dynamodb:DescribeTable",
      "dynamodb:BatchGetItem",
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:BatchWriteItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:PartiQLUpdate",
      "dynamodb:PartiQLSelect",
      "dynamodb:PartiQLInsert",
      "dynamodb:PartiQLDelete"
    ]

    resources = [
      aws_dynamodb_table.manifest_dynamo_table.arn,
      "${aws_dynamodb_table.manifest_dynamo_table.arn}/*",
      aws_dynamodb_table.manifest_files_dynamo_table.arn,
      "${aws_dynamodb_table.manifest_files_dynamo_table.arn}/*"
    ]

  }

  statement {
    sid    = "AllowPublishToMyTopic"
    effect = "Allow"

    actions = [
      "sns:Publish"
    ]

    resources = [
      aws_sns_topic.imported_file_sns_topic.arn
    ]
  }

  statement {
    sid    = "PostChangeLogMessages"
    effect = "Allow"

    actions = [
      "sqs:SendMessage",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.jobs_queue_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.jobs_queue_arn}/*",
    ]
  }

  // Interact with JobService KMS key
  statement {
    sid    = "KMSDecryptMessages"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.jobs_kms_key_arn,
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

  // The service lambda enqueues synthesized S3 "Object Created" events to
  // upload_trigger_queue from the finalize endpoint. The upload lambda's
  // existing SQS event source drains them and runs the same ImportFiles
  // flow as for real S3 notifications. Replaces the previous synchronous
  // lambda:Invoke path.
  statement {
    sid    = "ServiceLambdaEnqueueToUploadTrigger"
    effect = "Allow"

    actions = [
      "sqs:SendMessage",
      "sqs:SendMessageBatch",
    ]

    resources = [
      aws_sqs_queue.upload_trigger_queue.arn,
    ]
  }

  statement {
    sid    = "InvokeLambdaPermission"
    effect = "Allow"

    actions = [
      "lambda:InvokeFunction",
      "lambda:InvokeAsync"
    ]

    resources = [
      aws_lambda_function.archive_lambda.arn,
      aws_lambda_function.upload_lambda.arn,
    ]

  }

  statement {
    sid    = "AssumeUploadCredentialsRole"
    effect = "Allow"

    actions = [
      "sts:AssumeRole"
    ]

    resources = [
      aws_iam_role.upload_credentials_role.arn,
      aws_iam_role.storage_credentials_role.arn,
    ]
  }

}

##############################
# UPLOAD CREDENTIALS ROLE    #
##############################
# This role is assumed by the service Lambda to generate scoped temporary
# credentials for cross-account S3 uploads (used by data-target-pennsieve).

resource "aws_iam_role" "upload_credentials_role" {
  name = "${var.environment_name}-${var.service_name}-upload-credentials-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.upload_service_v2_lambda_role.arn
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "upload_credentials_policy_attachment" {
  role       = aws_iam_role.upload_credentials_role.name
  policy_arn = aws_iam_policy.upload_credentials_policy.arn
}

resource "aws_iam_policy" "upload_credentials_policy" {
  name   = "${var.environment_name}-${var.service_name}-upload-credentials-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  policy = data.aws_iam_policy_document.upload_credentials_policy_document.json
}

data "aws_iam_policy_document" "upload_credentials_policy_document" {
  statement {
    sid    = "UploadsBucketWriteAccess"
    effect = "Allow"

    actions = [
      "s3:PutObject",
      "s3:ListBucketMultipartUploads",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
      "s3:PutObjectTagging"
    ]

    resources = [
      aws_s3_bucket.uploads_s3_bucket.arn,
      "${aws_s3_bucket.uploads_s3_bucket.arn}/*"
    ]
  }
}

##############################
# STORAGE CREDENTIALS ROLE   #
##############################
# Assumed by the service lambda to mint temporary credentials scoped to a
# specific manifest's destination storage bucket + O{org}/D{ds}/{manifest}/*
# prefix. Lets agents upload direct-to-storage without staging through the
# upload bucket.

resource "aws_iam_role" "storage_credentials_role" {
  name = "${var.environment_name}-${var.service_name}-storage-credentials-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.upload_service_v2_lambda_role.arn
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Static storage buckets (platform-infra owned). Dynamic workspace buckets are
# attached below via the account-service managed policy, which is updated as
# storage nodes are provisioned.
resource "aws_iam_role_policy_attachment" "storage_credentials_static_buckets" {
  role       = aws_iam_role.storage_credentials_role.name
  policy_arn = aws_iam_policy.storage_credentials_static_policy.arn
}

resource "aws_iam_policy" "storage_credentials_static_policy" {
  name   = "${var.environment_name}-${var.service_name}-storage-credentials-static-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  policy = data.aws_iam_policy_document.storage_credentials_static_policy_document.json
}

data "aws_iam_policy_document" "storage_credentials_static_policy_document" {
  statement {
    sid    = "StaticStorageBucketsWriteAccess"
    effect = "Allow"

    actions = [
      "s3:PutObject",
      "s3:ListBucketMultipartUploads",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
      "s3:PutObjectTagging"
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.storage_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.sparc_storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.sparc_storage_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.rejoin_storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.rejoin_storage_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.precision_storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.precision_storage_bucket_arn}/*",
      data.terraform_remote_state.africa_south_region.outputs.af_south_s3_storage_bucket_arn,
      "${data.terraform_remote_state.africa_south_region.outputs.af_south_s3_storage_bucket_arn}/*",
    ]
  }
}

# Dynamic storage buckets (workspace-scoped, account-service managed). The
# same managed policy is also attached to the Fargate move role at the bottom
# of this file.
resource "aws_iam_role_policy_attachment" "storage_credentials_dynamic_buckets" {
  role       = aws_iam_role.storage_credentials_role.name
  policy_arn = data.terraform_remote_state.account_service.outputs.storage_write_policy_arn
}

##############################
# ARCHIVER-LAMBDA   #
##############################

data "aws_iam_policy_document" "archive_bucket_iam_policy_document" {

  statement {
    sid    = "ForceSSLOnlyAccess"
    effect = "Deny"

    resources = [
      "arn:aws:s3:::pennsieve-${var.environment_name}-manifest-archive-${data.terraform_remote_state.region.outputs.aws_region_shortname}",
      "arn:aws:s3:::pennsieve-${var.environment_name}-manifest-archive-${data.terraform_remote_state.region.outputs.aws_region_shortname}/*",
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
    sid    = "LambdaAccessToDynamoDB"
    effect = "Allow"

    actions = [
      "dynamodb:DescribeTable",
      "dynamodb:BatchGetItem",
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:BatchWriteItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:PartiQLUpdate",
      "dynamodb:PartiQLSelect",
      "dynamodb:PartiQLInsert",
      "dynamodb:PartiQLDelete"
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




##############################
# MOVE-FARGATE TASK   #
##############################
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
  policy = data.aws_iam_policy_document.upload_fargate_iam_policy_document.json
}

data "aws_iam_policy_document" "upload_fargate_iam_policy_document" {

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
    sid    = "LambdaAccessToDynamoDB"
    effect = "Allow"

    actions = [
      "dynamodb:DescribeTable",
      "dynamodb:BatchGetItem",
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:BatchWriteItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:PartiQLUpdate",
      "dynamodb:PartiQLSelect",
      "dynamodb:PartiQLInsert",
      "dynamodb:PartiQLDelete"
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

  statement {
    effect = "Allow"

    actions = [
      "s3:*",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.storage_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.sparc_storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.sparc_storage_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.rejoin_storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.rejoin_storage_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.precision_storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.precision_storage_bucket_arn}/*",
      data.terraform_remote_state.africa_south_region.outputs.af_south_s3_storage_bucket_arn,
      "${data.terraform_remote_state.africa_south_region.outputs.af_south_s3_storage_bucket_arn}/*",
      aws_s3_bucket.uploads_s3_bucket.arn,
      "${aws_s3_bucket.uploads_s3_bucket.arn}/*",
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:List*",
    ]

    resources = [
      "*",
    ]
  }
}

##############################
# COGNITO IDENTITY POOL      #
##############################

resource "aws_iam_role" "cognito_identity_auth_role" {
  name = "cognito_authenticated"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "cognito-identity.amazonaws.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "cognito-identity.amazonaws.com:aud": "${aws_cognito_identity_pool.pennsieve_auth.id}"
        },
        "ForAnyValue:StringLike": {
          "cognito-identity.amazonaws.com:amr": "authenticated"
        }
      }
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "cognito_identity_policy_attachment" {
  role       = aws_iam_role.cognito_identity_auth_role.id
  policy_arn = aws_iam_policy.upload_auth_identity_policy.arn
}

resource "aws_iam_policy" "upload_auth_identity_policy" {
  name   = "${var.environment_name}-${var.service_name}-upload-auth-identity-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  policy = data.aws_iam_policy_document.cognito_upload_identity_policy_document.json
}

data "aws_iam_policy_document" "cognito_upload_identity_policy_document" {
  statement {
    sid    = "UploadsBucketAccess"
    effect = "Allow"

    actions = [
      "s3:PutObject",
      "s3:ListBucketMultipartUploads",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
      "s3:PutObjectTagging"
    ]

    resources = [
      aws_s3_bucket.uploads_s3_bucket.arn,
      "${aws_s3_bucket.uploads_s3_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_role" "cognito_identity_unauth_role" {
  name = "cognito_unauthenticated"

  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "cognito-identity.amazonaws.com"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringEquals": {
                    "cognito-identity.amazonaws.com:aud": "${aws_cognito_identity_pool.pennsieve_auth.id}"
                },
                "ForAnyValue:StringLike": {
                    "cognito-identity.amazonaws.com:amr": "unauthenticated"
                }
            }
        }
    ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "cognito_identity_unauth_policy_attachment" {
  role       = aws_iam_role.cognito_identity_unauth_role.id
  policy_arn = aws_iam_policy.upload_unauth_identity_policy.arn
}

resource "aws_iam_policy" "upload_unauth_identity_policy" {
  name   = "${var.environment_name}-${var.service_name}-upload-unauth-identity-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  policy = data.aws_iam_policy_document.cognito_upload_unauth_identity_policy_document.json
}

data "aws_iam_policy_document" "cognito_upload_unauth_identity_policy_document" {
  statement {
    sid    = "UnauthCognitoAccess"
    effect = "Allow"

    actions = [
      "mobileanalytics:PutEvents",
      "cognito-sync:*"
    ]

    resources = [
      "*"
    ]
  }
}


resource "aws_iam_role_policy_attachment" "fargate_storage_bucket_write" {
  role       = aws_iam_role.fargate_task_iam_role.name
  policy_arn = data.terraform_remote_state.account_service.outputs.storage_write_policy_arn
}

# Service lambda needs read on dynamic workspace storage buckets for the
# finalize endpoint's HEAD verification. Managed policy is refreshed by
# account-service whenever storage nodes are added/removed.
resource "aws_iam_role_policy_attachment" "service_lambda_storage_bucket_read" {
  role       = aws_iam_role.upload_service_v2_lambda_role.name
  policy_arn = data.terraform_remote_state.account_service.outputs.storage_read_policy_arn
}

##############################
# RECONCILE LAMBDA ROLE      #
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

# Static storage bucket read is needed to HEAD objects during verification.
# Dynamic workspace buckets are covered by the account-service managed
# policy attached below.
resource "aws_iam_role_policy_attachment" "reconcile_storage_bucket_read" {
  role       = aws_iam_role.reconcile_lambda_role.name
  policy_arn = data.terraform_remote_state.account_service.outputs.storage_read_policy_arn
}

##############################
# ARCHIVE-SWEEPER ROLE       #
##############################

resource "aws_iam_role" "archive_sweeper_role" {
  name = "${var.environment_name}-${var.service_name}-archive-sweeper-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

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

resource "aws_iam_role_policy_attachment" "archive_sweeper_policy_attachment" {
  role       = aws_iam_role.archive_sweeper_role.name
  policy_arn = aws_iam_policy.archive_sweeper_policy.arn
}

resource "aws_iam_policy" "archive_sweeper_policy" {
  name   = "${var.environment_name}-${var.service_name}-archive-sweeper-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  policy = data.aws_iam_policy_document.archive_sweeper_policy_document.json
}

data "aws_iam_policy_document" "archive_sweeper_policy_document" {
  statement {
    sid    = "LambdaBaseExec"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:CreateLogGroup",
    ]
    resources = ["*"]
  }

  # manifest_table has no index on DateCreated/Status so the sweep uses Scan
  # with a FilterExpression. Table is small (one row per manifest) so
  # full-scan cost is acceptable for a daily job.
  statement {
    sid       = "ArchiveSweeperScan"
    effect    = "Allow"
    actions   = ["dynamodb:Scan"]
    resources = [aws_dynamodb_table.manifest_dynamo_table.arn]
  }

  statement {
    sid       = "ArchiveSweeperInvoke"
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.archive_lambda.arn]
  }
}
