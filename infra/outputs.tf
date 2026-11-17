output "webhook_api_endpoint" {
  description = "HTTP API Gateway POST URL for registering with Calendly Webhooks"
  value       = "${aws_apigatewayv2_api.webhook_api.api_endpoint}/webhooks/calendly"
}

output "sqs_processing_queue_url" {
  description = "URL of the SQS Processing Queue buffering incoming Calendly events"
  value       = aws_sqs_queue.processing.url
}

output "sqs_dlq_url" {
  description = "URL of the SQS Dead-Letter Queue for malformed payloads and Pydantic validation quarantines"
  value       = aws_sqs_queue.dlq.url
}

output "s3_bronze_bucket" {
  description = "S3 Bronze bucket storing raw webhooks and archived rolling campaign spend files"
  value       = aws_s3_bucket.bronze.bucket
}

output "s3_silver_bucket" {
  description = "S3 Silver bucket backing Apache Iceberg tables silver.calendly_events and silver.campaign_spend"
  value       = aws_s3_bucket.silver.bucket
}

output "s3_gold_bucket" {
  description = "S3 Gold bucket backing Apache Iceberg table gold.campaign_attribution_summary"
  value       = aws_s3_bucket.gold.bucket
}

output "s3_glue_scripts_bucket" {
  description = "S3 bucket storing packaged PySpark Glue jobs and Pydantic schemas"
  value       = aws_s3_bucket.glue_scripts.bucket
}

output "aws_deploy_role_arn" {
  description = "IAM Role ARN for GitHub Actions OIDC deployment (set as AWS_DEPLOY_ROLE_ARN secret)"
  value       = aws_iam_role.github_actions_deploy_role.arn
}

