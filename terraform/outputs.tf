output "service_lambda_arn" {
  value = aws_lambda_function.service_lambda.arn
}

output "service_lambda_invoke_arn" {
  value = aws_lambda_function.service_lambda.invoke_arn
}

output "service_lambda_function_name" {
  value = aws_lambda_function.service_lambda.function_name
}

output "upload_lambda_invoke_arn" {
  value = aws_lambda_function.upload_lambda.invoke_arn
}

output "manifest_table_arn" {
  value = aws_dynamodb_table.manifest_dynamo_table.arn
}

output "manifest_table_name" {
  value = aws_dynamodb_table.manifest_dynamo_table.name
}

output "file_finalized_topic_arn" {
  value = aws_sns_topic.file_finalized_topic.arn
}