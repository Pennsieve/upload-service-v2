// CREATE SNS TOPIC SUBSCRIPTION
# Ops alerts topic for the reconcile + archive-sweeper lambdas. Operators
# subscribe to this topic (email / Slack / etc.) to get notified when the
# CloudWatch alarms defined in cloudwatch.tf trigger.
resource "aws_sns_topic" "reconcile_alerts" {
  name = "${var.environment_name}-${var.service_name}-reconcile-alerts-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
}

# FileFinalized fan-out topic. Published to by the upload lambda after
# a file row lands in Postgres (see lambda/upload/handler/store.go
# ImportFiles). Consumers subscribe via their own SQS queues — scan-
# service is the first (filter: complianceTier=hipaa); metadata /
# AI-readiness services will follow.
#
# Publish is authorized via the upload lambda role's identity policy in
# iam.tf; no resource-based topic policy is needed for same-account use.
#
# Topic ARN is exported from outputs.tf and read by consumer stacks via
# terraform_remote_state.
resource "aws_sns_topic" "file_finalized_topic" {
  name         = "${var.environment_name}-${var.service_name}-file-finalized-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  display_name = "${var.environment_name}-${var.service_name}-file-finalized-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
}
