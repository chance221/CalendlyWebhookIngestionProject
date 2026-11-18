# -----------------------------------------------------------------------------
# Upload Glue PySpark Scripts & Pre-Bundled Dependencies Zip to S3 Glue Bucket
# -----------------------------------------------------------------------------
resource "aws_s3_object" "glue_dependencies_bundle" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "glue_jobs_${var.github_sha}/glue_dependencies.zip"
  source = "${path.module}/../dist/glue_dependencies.zip"
  etag   = filemd5("${path.module}/../dist/glue_dependencies.zip")
}

resource "aws_s3_object" "schemas_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "glue_jobs_${var.github_sha}/schemas.py"
  source = "${path.module}/../src/glue_jobs/schemas.py"
  etag   = filemd5("${path.module}/../src/glue_jobs/schemas.py")
}

resource "aws_s3_object" "bronze_to_silver_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "glue_jobs_${var.github_sha}/bronze_to_silver.py"
  source = "${path.module}/../src/glue_jobs/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../src/glue_jobs/bronze_to_silver.py")
}

resource "aws_s3_object" "silver_to_gold_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "glue_jobs_${var.github_sha}/silver_to_gold.py"
  source = "${path.module}/../src/glue_jobs/silver_to_gold.py"
  etag   = filemd5("${path.module}/../src/glue_jobs/silver_to_gold.py")
}

# -----------------------------------------------------------------------------
# AWS Glue PySpark Jobs (Apache Iceberg 4.0) - Zero Runtime Pip Install
# -----------------------------------------------------------------------------
resource "aws_glue_job" "bronze_to_silver" {
  name              = "${var.project_name}-${var.environment}-bronze-to-silver"
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/${aws_s3_object.bronze_to_silver_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.glue_scripts.bucket}/${aws_s3_object.glue_dependencies_bundle.key}"
    "--BRONZE_BUCKET"                    = aws_s3_bucket.bronze.bucket
    "--SILVER_BUCKET"                    = aws_s3_bucket.silver.bucket
    "--SILVER_DATABASE"                  = aws_glue_catalog_database.silver.name
    "--DLQ_URL"                          = aws_sqs_queue.dlq.url
    "--target_date"                      = "yesterday"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }
}

resource "aws_glue_job" "silver_to_gold" {
  name              = "${var.project_name}-${var.environment}-silver-to-gold"
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/${aws_s3_object.silver_to_gold_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.glue_scripts.bucket}/${aws_s3_object.glue_dependencies_bundle.key}"
    "--GOLD_BUCKET"                      = aws_s3_bucket.gold.bucket
    "--SILVER_DATABASE"                  = aws_glue_catalog_database.silver.name
    "--GOLD_DATABASE"                    = aws_glue_catalog_database.gold.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }
}

# -----------------------------------------------------------------------------
# Amazon EventBridge Batch Schedule Triggering the Glue Pipeline
# -----------------------------------------------------------------------------
resource "aws_scheduler_schedule" "bronze_to_silver_schedule" {
  name       = "${var.project_name}-${var.environment}-bronze-to-silver-schedule"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = var.glue_schedule_expression

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:glue:startJobRun"
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn

    input = jsonencode({
      JobName = aws_glue_job.bronze_to_silver.name
    })
  }
}
