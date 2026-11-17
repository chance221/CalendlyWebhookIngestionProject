variable "aws_region" {
  description = "AWS region to deploy the Calendly Lakehouse infrastructure"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Base name prefix for all Calendly Lakehouse resources"
  type        = string
  default     = "calendly-lakehouse"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "glue_schedule_expression" {
  description = "EventBridge cron/rate expression for the scheduled Glue batch pipeline"
  type        = string
  default     = "cron(0 2 * * ? *)"
}

variable "sqs_max_receive_count" {
  description = "Maximum receives before SQS redrives a message to the Dead-Letter Queue"
  type        = number
  default     = 3
}

variable "github_sha" {
  description = "Git commit SHA used to version S3 directories holding Lambda and Glue job code"
  type        = string
  default     = "local"
}

variable "github_repository" {
  description = "GitHub repository (owner/repo) allowed to assume the OIDC deployment role"
  type        = string
  default     = "chance221/CalendlyWebhookIngestionProject"
}


