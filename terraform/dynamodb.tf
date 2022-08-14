## Create Upload Manifest Dynamo Table
resource "aws_dynamodb_table" "manifest_dynamo_table" {
  name           = "${var.environment_name}-manifest-table-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "ManifestId"

  attribute {
    name = "ManifestId"
    type = "S"
  }

  attribute {
    name = "UserId"
    type = "N"
  }

  attribute {
    name = "DatasetNodeId"
    type = "S"
  }

  global_secondary_index {
    name               = "DatasetManifestIndex"
    hash_key           = "DatasetNodeId"
    range_key          = "UserId"
    projection_type    = "ALL"
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
  range_key      = "UploadId"

  attribute {
    name = "ManifestId"
    type = "S"
  }

  attribute {
    name = "UploadId"
    type = "S"
  }

  attribute {
    name = "Status"
    type = "S"
  }

  attribute {
    name = "FilePath"
    type = "S"
  }

  attribute {
    name = "InProgress"
    type = "S"
  }

  // Used to query files to be moved to final destination
  global_secondary_index {
    name               = "StatusIndex"
    hash_key           = "Status"
    range_key          = "ManifestId"
    projection_type    = "INCLUDE"
    non_key_attributes = ["ManifestId", "UploadId", "FileName", "FilePath", "FileType"]
    read_capacity      = 0
    write_capacity     = 0
  }

  // Used check which files in manifest have been imported.
  // We are using a sparse index for this that only contains records that have inProgress flag set.
  global_secondary_index {
    name               = "InProgressIndex"
    hash_key           = "ManifestId"
    range_key          = "InProgress"
    projection_type    = "INCLUDE"
    non_key_attributes = ["FileName", "FilePath", "FileType", "Status"]
    read_capacity      = 0
    write_capacity     = 0
  }

  // Used to query packageIDs for files that should share package
  global_secondary_index {
    name               = "PathIndex"
    hash_key           = "ManifestId"
    range_key          = "FilePath"
    projection_type    = "INCLUDE"
    non_key_attributes = ["FileName", "UploadId", "MergePackageId"]
    read_capacity      = 0
    write_capacity     = 0
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