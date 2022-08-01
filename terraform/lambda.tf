## Lambda Function which consumes messages from the SQS queue which contains all events.
resource "aws_lambda_function" "upload_lambda" {
  description      = "Lambda Function which consumes messages from the SQS queue related to newly uploaded files."
  function_name    = "${var.environment_name}-${var.service_name}-upload-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler          = "pennsieve_upload_handler"
  runtime          = "go1.x"
  role             = aws_iam_role.upload_service_v2_lambda_role.arn
  timeout          = 300
  memory_size      = 128
  source_code_hash = data.archive_file.upload_trigger_lambda_archive.output_base64sha256
  filename         = "${path.module}/../lambda/bin/pennsieve_upload_handler.zip"
  publish = true

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
      REGION = var.aws_region
      RDS_PROXY_ENDPOINT = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
    }
  }
}


data "archive_file" "upload_trigger_lambda_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/bin/handler"
  output_path = "${path.module}/../lambda/bin/pennsieve_upload_handler.zip"
}


### SERVICE LAMBDA

## Lambda Function which consumes messages from the SQS queue which contains all events.
resource "aws_lambda_function" "service_lambda" {
  description      = "Lambda Function which consumes messages from the SQS queue related to newly uploaded files."
  function_name    = "${var.environment_name}-${var.service_name}-service-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler          = "pennsieve_upload_service"
  runtime          = "go1.x"
  role             = aws_iam_role.upload_service_v2_lambda_role.arn
  timeout          = 300
  memory_size      = 128
  source_code_hash = data.archive_file.upload_service_lambda_archive.output_base64sha256
  filename         = "${path.module}/../lambda/bin/pennsieve_service_handler.zip"

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
      REGION = var.aws_region
      RDS_PROXY_ENDPOINT = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
    }
  }
}

data "archive_file" "upload_service_lambda_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/bin/service"
  output_path = "${path.module}/../lambda/bin/pennsieve_service_handler.zip"
}

#
#resource "aws_lambda_alias" "upload_service_lambda_live" {
#  name             = "live"
#  function_name    = aws_lambda_function.service_lambda.function_name
#  function_version = aws_lambda_function.service_lambda.version
#}
#
#resource "aws_lambda_provisioned_concurrency_config" "authorizer_lambda" {
#  function_name                     = aws_lambda_function.service_lambda.function_name
#  provisioned_concurrent_executions = 2
#  qualifier                         = aws_lambda_function.service_lambda.version
#}

### MOVE TRIGGER


resource "aws_lambda_function" "fargate_trigger_lambda" {
  description      = "Lambda Function which triggers FARGATE to move files to final destination."
  function_name    = "${var.environment_name}-${var.service_name}-fargate-trigger-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  reserved_concurrent_executions = 1 // don't allow concurrent lambda's
  handler          = "pennsieve_move_trigger"
  runtime          = "go1.x"
  role             = aws_iam_role.move_trigger_lambda_role.arn
  timeout          = 300
  memory_size      = 128
  source_code_hash = data.archive_file.move_trigger_lambda_archive.output_base64sha256
  filename         = "${path.module}/../lambda/bin/pennsieve_move_trigger_handler.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV = var.environment_name
      TASK_DEF_ARN = aws_ecs_task_definition.ecs_task_definition.arn
      CLUSTER_ARN = data.terraform_remote_state.fargate.outputs.ecs_cluster_arn
      SUBNET_IDS = join(",", data.terraform_remote_state.vpc.outputs.private_subnet_ids)
      SECURITY_GROUP = data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_fargate_security_group_id
      REGION = var.aws_region
      RDS_PROXY_ENDPOINT = data.terraform_remote_state.pennsieve_postgres.outputs.rds_proxy_endpoint,
    }
  }
}

data "archive_file" "move_trigger_lambda_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/bin/moveTrigger"
  output_path = "${path.module}/../lambda/bin/pennsieve_move_trigger_handler.zip"
}