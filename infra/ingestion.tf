# -----------------------------------------------------------------------------
# SQS Dead-Letter Queue (DLQ) & Processing Queue
# -----------------------------------------------------------------------------
resource "aws_sqs_queue" "dlq" {
  name                      = "${var.project_name}-${var.environment}-dlq"
  message_retention_seconds = 1209600 # 14 days
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "processing" {
  name                       = "${var.project_name}-${var.environment}-processing-queue"
  visibility_timeout_seconds = 180
  message_retention_seconds  = 345600 # 4 days
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
}

# -----------------------------------------------------------------------------
# Amazon API Gateway HTTP API -> Direct SQS Integration (No intermediate Lambda)
# -----------------------------------------------------------------------------
resource "aws_apigatewayv2_api" "webhook_api" {
  name          = "${var.project_name}-${var.environment}-webhook-api"
  protocol_type = "HTTP"
  description   = "Direct HTTP API Gateway to SQS ingestion endpoint for Calendly webhooks"
}

resource "aws_apigatewayv2_integration" "sqs_integration" {
  api_id              = aws_apigatewayv2_api.webhook_api.id
  integration_type    = "AWS_PROXY"
  integration_subtype = "SQS-SendMessage"
  credentials_arn     = aws_iam_role.apigw_sqs_role.arn

  request_parameters = {
    "QueueUrl"    = aws_sqs_queue.processing.url
    "MessageBody" = "$request.body"
  }
}

resource "aws_apigatewayv2_route" "calendly_webhook_route" {
  api_id    = aws_apigatewayv2_api.webhook_api.id
  route_key = "POST /webhooks/calendly"
  target    = "integrations/${aws_apigatewayv2_integration.sqs_integration.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.webhook_api.id
  name        = "$default"
  auto_deploy = true
}

# -----------------------------------------------------------------------------
# Ingestion AWS Lambda (Python 3.12) Polling SQS Processing Queue -> S3 Bronze
# Pre-built by `uv` into dist/ingestion_lambda.zip (zero runtime pip install)
# -----------------------------------------------------------------------------
resource "aws_s3_object" "ingestion_lambda_package" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "ingestion_lambda_${var.github_sha}/ingestion_lambda.zip"
  source = "${path.module}/../dist/ingestion_lambda.zip"
  etag   = filemd5("${path.module}/../dist/ingestion_lambda.zip")
}

resource "aws_lambda_function" "ingestion" {
  function_name    = "${var.project_name}-${var.environment}-ingestion"
  role             = aws_iam_role.ingestion_lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  s3_bucket        = aws_s3_bucket.glue_scripts.id
  s3_key           = aws_s3_object.ingestion_lambda_package.key
  source_code_hash = filebase64sha256("${path.module}/../dist/ingestion_lambda.zip")
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.bronze.bucket
      DLQ_URL       = aws_sqs_queue.dlq.url
    }
  }
}

resource "aws_lambda_event_source_mapping" "sqs_to_lambda" {
  event_source_arn                   = aws_sqs_queue.processing.arn
  function_name                      = aws_lambda_function.ingestion.arn
  batch_size                         = 10
  maximum_batching_window_in_seconds = 5
  function_response_types            = ["ReportBatchItemFailures"]
}
