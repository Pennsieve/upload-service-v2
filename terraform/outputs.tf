output "uploads_bucket_arn" {
  value = aws_s3_bucket.uploads_s3_bucket.arn
}

output "uploads_bucket_id" {
  value = aws_s3_bucket.uploads_s3_bucket.id
}

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

output "identity_pool_id" {
  value = aws_cognito_identity_pool.pennsieve_auth.id
}

output "manifest_table_arn" {
  value =aws_dynamodb_table.manifest_dynamo_table.arn
}

output "manifest_table_name" {
  value =aws_dynamodb_table.manifest_dynamo_table.name
}