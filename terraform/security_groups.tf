# Create Upload-Service-v2 Service Security Group
resource "aws_security_group" "upload_v2_service_security_group" {
  name        = "${var.environment_name}-upload-v2-service-sg-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  description = "Security Group For ${var.environment_name}-upload-v2-service-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  vpc_id      = data.terraform_remote_state.vpc.outputs.vpc_id

  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = "true"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    self        = "true"
  }

  tags = merge(
  local.common_tags,
  {
    "Name"         = "${var.environment_name}-upload-v2-service-sg-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "name"         = "${var.environment_name}-upload-v2-service-sg-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "service_name" = "upload-v2"
  },
  )
}