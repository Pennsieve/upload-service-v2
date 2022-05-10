## Create Pennsieve Uploads S3 Bucket
resource "aws_s3_bucket" "uploads_s3_bucket" {
  bucket = "pennsieve-${var.environment_name}-uploads-v2-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  lifecycle {
    prevent_destroy = true
  }

  tags = merge(
  local.common_tags,
  {
    "Name"         = "${var.environment_name}-uploads-s3-bucket-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "name"         = "${var.environment_name}-uploads-s3-bucket-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "service_name" = "upload-service-v2"
    "tier"         = "s3"
  },
  )
}

resource "aws_s3_bucket_policy" "uploads_s3_bucket_policy" {
  bucket = aws_s3_bucket.uploads_s3_bucket.bucket
  policy = data.aws_iam_policy_document.uploads_bucket_force_ssl_iam_policy_document.json
}

resource "aws_s3_bucket_server_side_encryption_configuration" "uploads_s3_bucket_encryption" {
  bucket = aws_s3_bucket.uploads_s3_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

