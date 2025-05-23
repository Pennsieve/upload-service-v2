## Create Pennsieve Uploads S3 Bucket
resource "aws_s3_bucket" "uploads_s3_bucket" {
  bucket = "pennsieve-${var.environment_name}-uploads-v2-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  lifecycle {
    prevent_destroy = true
  }

  tags = merge(
  local.common_tags,
  {
    "Name"         = "${var.environment_name}-uploads-v2-s3-bucket-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "name"         = "${var.environment_name}-uploads-v2-s3-bucket-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "service_name" = "upload-service-v2"
    "tier"         = "s3"
  },
  )
}

resource "aws_s3_bucket_lifecycle_configuration" "upload_s3_bucket_lifecycle" {
  bucket = aws_s3_bucket.uploads_s3_bucket.bucket

  rule {
    id = "expire-partial-uploads"

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    status = "Enabled"
  }
}

resource "aws_s3_bucket_policy" "uploads_s3_bucket_policy" {
  bucket = aws_s3_bucket.uploads_s3_bucket.bucket
  policy = data.aws_iam_policy_document.uploads_bucket_iam_policy_document.json
}

resource "aws_s3_bucket_server_side_encryption_configuration" "uploads_s3_bucket_encryption" {
  bucket = aws_s3_bucket.uploads_s3_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# S3 event filter
resource "aws_s3_bucket_notification" "uploads_s3_notification" {
  bucket = aws_s3_bucket.uploads_s3_bucket.bucket

  queue {
    queue_arn     = aws_sqs_queue.upload_trigger_queue.arn
    events        = ["s3:ObjectCreated:*"]
  }
}


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