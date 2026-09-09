# T1 CloudWatch alarms (EPIC 868m2zvjt; standard sets from
# pennsieve-infra-dashboard/docs/alarm-coverage-plan.md). Complements the
# hand alarms in cloudwatch.tf: the upload-trigger DLQ depth alarm and the
# reconcile/archive-sweeper custom-metric alarms stay as they are, so only
# the imported-file DLQ is alarmed here. No alarm_actions yet.
module "service_alarms" {
  source = "git@github.com:Pennsieve/terraform-modules.git//service-alarms"

  environment_name = var.environment_name
  service_name     = var.service_name

  lambdas = {
    upload = {
      function_name   = aws_lambda_function.upload_lambda.function_name
      timeout_seconds = aws_lambda_function.upload_lambda.timeout
    }
    service = {
      function_name   = aws_lambda_function.service_lambda.function_name
      timeout_seconds = aws_lambda_function.service_lambda.timeout
    }
    archive = {
      function_name   = aws_lambda_function.archive_lambda.function_name
      timeout_seconds = aws_lambda_function.archive_lambda.timeout
    }
    fargate-trigger = {
      function_name   = aws_lambda_function.fargate_trigger_lambda.function_name
      timeout_seconds = aws_lambda_function.fargate_trigger_lambda.timeout
    }
    reconcile = {
      function_name   = aws_lambda_function.reconcile_lambda.function_name
      timeout_seconds = aws_lambda_function.reconcile_lambda.timeout
    }
    archive-sweeper = {
      function_name   = aws_lambda_function.archive_sweeper_lambda.function_name
      timeout_seconds = aws_lambda_function.archive_sweeper_lambda.timeout
    }
  }

  dynamodb_tables = {
    manifest       = aws_dynamodb_table.manifest_dynamo_table.name
    manifest-files = aws_dynamodb_table.manifest_files_dynamo_table.name
  }

  queues = {
    upload-trigger = {
      queue_name      = aws_sqs_queue.upload_trigger_queue.name
      max_age_seconds = 3600
    }
    imported-file = {
      queue_name      = aws_sqs_queue.imported_file_queue.name
      max_age_seconds = 3600
    }
  }

  dlqs = {
    imported-file = aws_sqs_queue.imported_file_deadletter_queue.name
  }
}
