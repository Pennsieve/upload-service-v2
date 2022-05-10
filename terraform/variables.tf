variable "aws_account" {}

variable "aws_region" {}

variable "environment_name" {}

variable "service_name" {}

variable "vpc_name" {}

variable "domain_name" {}

variable "pennsieve_postgres_host" {}




locals {
  domain_name = data.terraform_remote_state.account.outputs.domain_name
  hosted_zone = data.terraform_remote_state.account.outputs.public_hosted_zone_id

  common_tags = {
    aws_account      = var.aws_account
    aws_region       = data.aws_region.current_region.name
    environment_name = var.environment_name
  }
}
