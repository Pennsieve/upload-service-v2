## Lambda Function which consumes messages from the SQS queue which contains all events.
resource "aws_lambda_function" "upload-lambda" {
  description      = "Lambda Function which consumes messages from the SQS queue related to newly uploaded files."
  function_name    = "${var.environment_name}-${var.service_name}-upload-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler          = "pennsieve_upload_handler"
  runtime          = "go1.x"
  role             = aws_iam_role.upload_trigger_lambda_role.arn
  timeout          = 300
  memory_size      = 128
  source_code_hash = data.archive_file.upload_trigger_lambda_archive.output_base64sha256
  filename         = "${path.module}/../lambda/bin/pennsieve_upload_handler.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [data.terraform_remote_state.platform_infrastructure.outputs.upload_v2_security_group_id]
  }

  environment {
    variables = {
      ENV = "${var.environment_name}"
      PENNSIEVE_DOMAIN = data.terraform_remote_state.account.outputs.domain_name,
      #      WEBHOOK_SQS_QUEUE_NAME = aws_sqs_queue.webhook_integration_queue.name
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
resource "aws_lambda_function" "service-lambda" {
  description      = "Lambda Function which consumes messages from the SQS queue related to newly uploaded files."
  function_name    = "${var.environment_name}-${var.service_name}-service-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  handler          = "pennsieve_upload_service"
  runtime          = "go1.x"
  role             = aws_iam_role.upload_trigger_lambda_role.arn
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
      ENV = "${var.environment_name}"
      PENNSIEVE_DOMAIN = data.terraform_remote_state.account.outputs.domain_name,
    }
  }
}


data "archive_file" "upload_service_lambda_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/bin/service"
  output_path = "${path.module}/../lambda/bin/pennsieve_service_handler.zip"
}
