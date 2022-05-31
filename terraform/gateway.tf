resource "aws_apigatewayv2_api" "upload-service-gateway" {
  name          = "serverless_upload_service"
  protocol_type = "HTTP"
  description = "API Gateway for Upload-Service V2"
  cors_configuration {}
  body          = templatefile("${path.module}/upload_service.yml", {
    upload_service_lambda_arn = aws_lambda_function.service-lambda.arn,
    user_pool_2_client_id = data.terraform_remote_state.authentication_service.outputs.user_pool_2_client_id,
    cognito_endpoint = "https://${var.pool_endpoint}"
  })
}

resource "aws_apigatewayv2_stage" "upload-service-gateway-stage" {
  api_id = aws_apigatewayv2_api.upload-service-gateway.id

  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.upload-service-log-group.arn

    format = jsonencode({
      requestId               = "$context.requestId"
      sourceIp                = "$context.identity.sourceIp"
      requestTime             = "$context.requestTime"
      protocol                = "$context.protocol"
      httpMethod              = "$context.httpMethod"
      resourcePath            = "$context.resourcePath"
      routeKey                = "$context.routeKey"
      status                  = "$context.status"
      responseLength          = "$context.responseLength"
      integrationErrorMessage = "$context.integrationErrorMessage"
    }
    )
  }
}

resource "aws_apigatewayv2_integration" "int" {
  api_id           = aws_apigatewayv2_api.upload-service-gateway.id
  integration_type = "AWS_PROXY"
  connection_type = "INTERNET"
  integration_method = "POST"
  integration_uri = aws_lambda_function.upload-lambda.invoke_arn
}

resource "aws_cloudwatch_log_group" "upload-service-log-group" {
  name =  "${var.environment_name}/${var.service_name}/${aws_apigatewayv2_api.upload-service-gateway.name}"

  retention_in_days = 30
}

resource "aws_lambda_permission" "upload-service-lambda-permission" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.service-lambda.function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.upload-service-gateway.execution_arn}/*/*"
}