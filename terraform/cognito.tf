resource "aws_cognito_identity_pool" "pennsieve_auth" {
  identity_pool_name               = "${var.environment_name}-pennsieve-identity-pool-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  allow_unauthenticated_identities = false
  allow_classic_flow               = false

  cognito_identity_providers {
    client_id               = data.terraform_remote_state.authentication_service.outputs.token_pool_client_id
    provider_name           = "cognito-idp.${var.aws_region}.amazonaws.com/${data.terraform_remote_state.authentication_service.outputs.token_pool_id}"
    server_side_token_check = false
  }

  cognito_identity_providers {
    client_id               = data.terraform_remote_state.authentication_service.outputs.user_pool_2_id
    provider_name           = "cognito-idp.${var.aws_region}.amazonaws.com/${data.terraform_remote_state.authentication_service.outputs.user_pool_2_id}"
    server_side_token_check = false
  }
}

resource "aws_cognito_identity_pool_roles_attachment" "main" {
  identity_pool_id = aws_cognito_identity_pool.pennsieve_auth.id

  roles = {
    "authenticated" = aws_iam_role.cognito_identity_auth_role.arn
    "unauthenticated" = aws_iam_role.cognito_identity_unauth_role.arn
  }
}