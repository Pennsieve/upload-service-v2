# Create Upload Manifest Dynamo Table
resource "aws_dynamodb_table" "manifest_dynamo_table" {
  name           = "${var.environment_name}-manifest-table-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "ManifestId"
  range_key      = "Created"

  attribute {
    name = "ManifestId"
    type = "S"
  }

  attribute {
    name = "Created"
    type = "N"
  }

  point_in_time_recovery {
    enabled = false
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(
  local.common_tags,
  {
    "Name"         = "${var.environment_name}-manifest-table-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "name"         = "${var.environment_name}-manifest-table-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "service_name" = var.service_name
  },
  )
}

# Create Manifest Files Dynamo Table
resource "aws_dynamodb_table" "manifest_files_dynamo_table" {
  name           = "${var.environment_name}-manifest-files-table-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "ManifestId"
  range_key      = "Created"

  attribute {
    name = "ManifestId"
    type = "S"
  }

  attribute {
    name = "Created"
    type = "N"
  }

  point_in_time_recovery {
    enabled = false
  }

  server_side_encryption {
    enabled = true
  }

  ttl {
    attribute_name = "TimeToExist"
    enabled        = true
  }

  tags = merge(
  local.common_tags,
  {
    "Name"         = "${var.environment_name}-manifest-files-table-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "name"         = "${var.environment_name}-manifest-files-table-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "service_name" = var.service_name
  },
  )
}