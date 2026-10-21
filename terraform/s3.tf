## Create Pennsieve Uploads S3 Bucket
# S3 event filter
# S3 Bucket for Manifest Archive
resource "aws_s3_bucket" "manifest_archive_bucket" {
  bucket = "pennsieve-${var.environment_name}-manifest-archive-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  lifecycle {
    prevent_destroy = true
  }

  tags = merge(
    local.common_tags,
    {
      "Name"         = "${var.environment_name}-uploads-v2-manifest-archive-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
      "name"         = "${var.environment_name}-uploads-v2-manifest-archive-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
      "service_name" = "upload-service-v2"
      "tier"         = "s3"
    },
  )
}

resource "aws_s3_bucket_policy" "manifest_archive_bucket_policy" {
  bucket = aws_s3_bucket.manifest_archive_bucket.bucket
  policy = data.aws_iam_policy_document.archive_bucket_iam_policy_document.json
}

// Enable Bucket Versioning on Manifest Archives
resource "aws_s3_bucket_versioning" "manifest_archive_versioning" {
  bucket = aws_s3_bucket.manifest_archive_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "manifest_archive_bucket_encryption" {
  bucket = aws_s3_bucket.manifest_archive_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}