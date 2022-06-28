resource "aws_ssm_parameter" "manifest_table_name" {
  name  = "/${var.environment_name}/${var.service_name}/manifest-table-name"
  type  = "String"
  value = aws_dynamodb_table.manifest_dynamo_table.name
}

resource "aws_ssm_parameter" "manifest_files_table_name" {
  name  = "/${var.environment_name}/${var.service_name}/manifest-files-table-name"
  type  = "String"
  value = aws_dynamodb_table.manifest_files_dynamo_table.name
}

resource "aws_ssm_parameter" "upload_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/upload-bucket"
  type  = "String"
  value = aws_s3_bucket.uploads_s3_bucket.id
}