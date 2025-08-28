## Lambda Function which consumes messages from the SQS queue which contains all events.
resource "aws_lambda_function" "upload_lambda" {
  description      = "Lambda Function which consumes messages from the SQS queue related to newly uploaded files."
  function_name    = "${var.environment_name}-${var.service_name}-upload-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  role             = aws_iam_role.upload_service_v2_lambda_role.arn
  timeout          = 300
  memory_size      = 128
  s3_bucket         = var.lambda_bucket
  s3_key            = "${var.service_name}/upload/upload-v2-handler-${var.image_tag}.zip"
  reserved_concurrent_executions = 100 // Set a maximum concurrency to prevent overloading RDS interaction

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV = var.environment_name
      PENNSIEVE_DOMAIN = data.terraform_remote_state.account.outputs.domain_name,
      MANIFEST_TABLE = aws_dynamodb_table.manifest_dynamo_table.name,
      MANIFEST_FILE_TABLE = aws_dynamodb_table.manifest_files_dynamo_table.name,
      IMPORTED_SNS_TOPIC = aws_sns_topic.imported_file_sns_topic.arn,
      JOBS_QUEUE_ID = data.terraform_remote_state.platform_infrastructure.outputs.jobs_queue_id,
      REGION = var.aws_region,
      RDS_PROXY_ENDPOINT = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
      LOG_LEVEL = "info",
    }
  }
}

### SERVICE LAMBDA

## Lambda Function which consumes messages from the SQS queue which contains all events.
resource "aws_lambda_function" "service_lambda" {
  description      = "Lambda Function which consumes messages from the SQS queue related to newly uploaded files."
  function_name    = "${var.environment_name}-${var.service_name}-service-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  role             = aws_iam_role.upload_service_v2_lambda_role.arn
  timeout          = 300
  memory_size      = 128
  s3_bucket         = var.lambda_bucket
  s3_key            = "${var.service_name}/service/upload-v2-service-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV = var.environment_name
      ARCHIVE_BUCKET = aws_s3_bucket.manifest_archive_bucket.id,
      PENNSIEVE_DOMAIN = data.terraform_remote_state.account.outputs.domain_name,
      MANIFEST_TABLE = aws_dynamodb_table.manifest_dynamo_table.name,
      MANIFEST_FILE_TABLE = aws_dynamodb_table.manifest_files_dynamo_table.name,
      REGION = var.aws_region
      RDS_PROXY_ENDPOINT = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
      ARCHIVER_INVOKE_ARN = aws_lambda_function.archive_lambda.arn,
      LOG_LEVEL = "info",
    }
  }
}

### ARCHIVE MANIFEST LAMBDA
## Lambda Function which archives a manifest
resource "aws_lambda_function" "archive_lambda" {
  description      = "Lambda Function which archives a manifest when triggered by the service."
  function_name    = "${var.environment_name}-${var.service_name}-archive-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  role             = aws_iam_role.upload_service_v2_lambda_role.arn
  timeout          = 600
  memory_size      = 128
  s3_bucket         = var.lambda_bucket
  s3_key            = "${var.service_name}/archiver/manifest-archiver-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV = var.environment_name
      ARCHIVE_BUCKET = aws_s3_bucket.manifest_archive_bucket.id,
      PENNSIEVE_DOMAIN = data.terraform_remote_state.account.outputs.domain_name,
      MANIFEST_TABLE = aws_dynamodb_table.manifest_dynamo_table.name,
      MANIFEST_FILE_TABLE = aws_dynamodb_table.manifest_files_dynamo_table.name,
      REGION = var.aws_region
      RDS_PROXY_ENDPOINT = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
      LOG_LEVEL = "info",
    }
  }
}


### MOVE TRIGGER
resource "aws_lambda_function" "fargate_trigger_lambda" {
  description      = "Lambda Function which triggers FARGATE to move files to final destination."
  function_name    = "${var.environment_name}-${var.service_name}-fargate-trigger-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  reserved_concurrent_executions = 1 // don't allow concurrent lambda's
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  role             = aws_iam_role.move_trigger_lambda_role.arn
  timeout          = 300
  memory_size      = 128
  s3_bucket         = var.lambda_bucket
  s3_key            = "${var.service_name}/trigger/upload-v2-move-trigger-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV = var.environment_name
      TASK_DEF_ARN = aws_ecs_task_definition.ecs_task_definition.arn,
      CLUSTER_ARN = data.terraform_remote_state.fargate.outputs.ecs_cluster_arn,
      SUBNET_IDS = join(",", data.terraform_remote_state.vpc.outputs.private_subnet_ids),
      SECURITY_GROUP = data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_fargate_security_group_id,
      REGION = var.aws_region,
      RDS_PROXY_ENDPOINT = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
      LOG_LEVEL = "info",
    }
  }
}
