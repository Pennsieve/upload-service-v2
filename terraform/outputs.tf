output "uploads_bucket_arn" {
  value = aws_s3_bucket.uploads_s3_bucket.arn
}

output "uploads_bucket_id" {
  value = aws_s3_bucket.uploads_s3_bucket.id
}