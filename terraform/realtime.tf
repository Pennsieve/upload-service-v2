# Live updates over the environment's AppSync Event API (pennsieve-go-core
# pkg/realtime; docs/realtime-appsync-design.md). Where it's on, the upload
# Lambda publishes upload events to the API instead of Pusher. Environments
# cut over one at a time, once the web app there subscribes to AppSync: dev
# now; prod stays on Pusher (no realtime stack).
variable "realtime_events_enabled" {
  description = "Publish live updates to this environment's AppSync Event API (its realtime stack) instead of Pusher. Default: on in dev only."
  type        = bool
  default     = null
}

locals {
  realtime_events_enabled = coalesce(var.realtime_events_enabled, var.environment_name == "dev")
}

data "terraform_remote_state" "realtime" {
  count   = local.realtime_events_enabled ? 1 : 0
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/realtime/terraform.tfstate"
    region = "us-east-1"
  }
}

# The Event API publishes only with IAM (appsync:EventPublish on its
# namespaces).
resource "aws_iam_role_policy_attachment" "realtime_publish" {
  count      = local.realtime_events_enabled ? 1 : 0
  role       = aws_iam_role.upload_service_v2_lambda_role.name
  policy_arn = data.terraform_remote_state.realtime[0].outputs.publish_policy_arn
}
