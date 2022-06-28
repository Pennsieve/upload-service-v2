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
variable "container_memory" {
  default = "2048"
}

variable "container_cpu" {
  default = "0"
}

variable "image_url" {
  default = "pennsieve/upload_move_files"
}

variable "task_memory" {
  default = "2048"
}

variable "task_cpu" {
  default = "512"
}

variable "image_tag" {
  default = "latest"
}

variable "tier" {
  default = "upload-move"
}



locals {
  domain_name = data.terraform_remote_state.account.outputs.domain_name
  hosted_zone = data.terraform_remote_state.account.outputs.public_hosted_zone_id

  common_tags = {
    aws_account      = var.aws_account
    aws_region       = data.aws_region.current_region.name
    environment_name = var.environment_name
  }
}
