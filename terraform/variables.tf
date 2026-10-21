variable "aws_account" {}

variable "aws_region" {}

variable "environment_name" {}

variable "service_name" {}

variable "vpc_name" {}

variable "domain_name" {}

variable "pennsieve_postgres_host" {}

variable "pool_endpoint" {
  default = "cognito-idp.us-east-1.amazonaws.com/us-east-1_FVLhJ7CQA"
}

variable "user_pool_2_client_id" {
  default = "703lm5d8odccu21pagcfjkeaea"
}

// Fargate Task
variable "image_tag" {
  default = "latest"
}

variable "send_logs_to_datadog" {
  default = false
}

variable "enable_dynamodb_backup" {
}

variable "lambda_bucket" {
  default = "pennsieve-cc-lambda-functions-use1"
}

variable "api_domain_name" {
}

# No longer used: the legacy upload routes these switched are gone. Kept so
# that stacks still passing them (prod, dev) keep planning; remove together
# with those arguments.
variable "legacy_upload_credentials_enabled" {
  default = true
}

variable "legacy_cognito_upload_enabled" {
  default = true
}

locals {
  domain_name = data.terraform_remote_state.account.outputs.domain_name
  hosted_zone = data.terraform_remote_state.account.outputs.public_hosted_zone_id

  common_tags = {
    aws_account      = var.aws_account
    aws_region       = data.aws_region.current_region.name
    environment_name = var.environment_name
  }

  cors_allowed_origins = var.environment_name == "prod" ? ["https://discover.pennsieve.io", "https://app.pennsieve.io"] : ["http://localhost:3000", "https://discover.pennsieve.net", "https://app.pennsieve.net"]

}
