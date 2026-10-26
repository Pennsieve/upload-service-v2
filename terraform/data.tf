data "aws_caller_identity" "current" {}

data "aws_region" "current_region" {}

# Import Account Data
data "terraform_remote_state" "account" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import Region Data
data "terraform_remote_state" "region" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import VPC Data
data "terraform_remote_state" "vpc" {
  backend = "s3"

  config = {
    bucket  = "${var.aws_account}-terraform-state"
    key     = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/terraform.tfstate"
    region  = "us-east-1"
    profile = var.aws_account
  }
}

# Import Platform Infrastructure Data
data "terraform_remote_state" "platform_infrastructure" {
  backend = "s3"

  config = {
    bucket  = "${var.aws_account}-terraform-state"
    key     = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/platform-infrastructure/terraform.tfstate"
    region  = "us-east-1"
    profile = var.aws_account
  }
}

# Import Authentication-Service
# Import Postgres
data "terraform_remote_state" "pennsieve_postgres" {
  backend = "s3"

  config = {
    bucket  = "${var.aws_account}-terraform-state"
    key     = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/pennsieve-postgres/terraform.tfstate"
    region  = "us-east-1"
    profile = var.aws_account
  }
}

# Import AWS Default SecretsManager KMS Key
data "aws_kms_key" "ssm_kms_key" {
  key_id = "alias/aws/secretsmanager"
}

# IMPORT FARGATE CLUSTER DATA
# AFS1 Region
data "terraform_remote_state" "africa_south_region" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/af-south-1/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import API Gateway
data "terraform_remote_state" "api_gateway" {
  backend = "s3"

  config = {
    bucket  = "${var.aws_account}-terraform-state"
    key     = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/auth-service/terraform.tfstate"
    region  = "us-east-1"
    profile = var.aws_account
  }
}
# Import Account Service (storage node policies)
data "terraform_remote_state" "account_service" {
  backend = "s3"

  config = {
    bucket  = "${var.aws_account}-terraform-state"
    key     = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/account-service/terraform.tfstate"
    region  = "us-east-1"
    profile = var.aws_account
  }
}

# Import Workflow Service (the callback validator, for run-results imports)
data "terraform_remote_state" "workflow_service" {
  backend = "s3"

  config = {
    bucket  = "${var.aws_account}-terraform-state"
    key     = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/workflow-service/terraform.tfstate"
    region  = "us-east-1"
    profile = var.aws_account
  }
}
