output "uploads_bucket_arn" {
  value = aws_s3_bucket.uploads_s3_bucket.arn
}

output "uploads_bucket_id" {
  value = aws_s3_bucket.uploads_s3_bucket.id
}

output "upload_v2_service_security_group_arn" {
  value = aws_security_group.upload_v2_service_security_group.arn
}

output "upload_v2_service_security_group_id" {
  value = aws_security_group.upload_v2_service_security_group.id
}