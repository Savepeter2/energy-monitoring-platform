# =============================================================================
# Module: Lambda
# Two functions:
#   1. batch_processor  – triggered by SQS; reads S3 raw files; batch-inserts
#   2. stream_processor – triggered by Kinesis; processes MQTT events
# Both run inside the private VPC subnet and pull DB credentials from
# Secrets Manager at cold-start (cached in memory for subsequent invocations).
# =============================================================================

variable "project"                  { type = string }
variable "environment"              { type = string }
variable "vpc_id"                   { type = string }
variable "private_subnet_ids"       { type = list(string) }

# The Lambda security group is now created in the ROOT module and passed in here.
# This breaks the circular dependency: root creates Lambda SG → passes it to
# both aurora module (as allowed_sg_ids) and lambda module (as lambda_sg_id).
variable "lambda_sg_id"             { type = string }
variable "batch_role_arn"           { type = string }
# variable "stream_role_arn"          { type = string }
variable "sqs_queue_arn"            { type = string }
# variable "kinesis_stream_arn"       { type = string }
variable "processed_bucket"         { type = string }
variable "aurora_secret_arn"        { type = string }
variable "aurora_cluster_endpoint"  { type = string }
variable "db_name"                  { type = string }
variable "batch_size"               { 
                                     type = number
                                     default = 100 
                                     }
variable "starting_position"        { 
                                    type = string 
                                    default = "TRIM_HORIZON" 
                                    }
variable "lambda_zip_path"          { type = string }

# Shared environment variables
locals {
  common_env = {
    ENVIRONMENT           = var.environment
    DB_SECRET_ARN         = var.aurora_secret_arn
    DB_HOST               = var.aurora_cluster_endpoint
    DB_NAME               = var.db_name
    PROCESSED_BUCKET      = var.processed_bucket
    LOG_LEVEL             = var.environment == "prod" ? "WARNING" : "DEBUG"
  }
}

# CloudWatch Log Groups (explicit so retention is managed by Terraform) 
resource "aws_cloudwatch_log_group" "batch" {
  name              = "/aws/lambda/${var.project}-${var.environment}-batch"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "stream" {
  name              = "/aws/lambda/${var.project}-${var.environment}-stream"
  retention_in_days = 30
}

# =============================================================================
# 1. Batch Processor Lambda (SQS trigger)
# =============================================================================
resource "aws_lambda_function" "batch_processor" {
  function_name = "${var.project}-${var.environment}-batch"
  description   = "Reads raw S3 meter files, validates, deduplicates, and batch-inserts to Aurora"
  runtime       = "python3.12"
  handler       = "batch_processor.handler"
  source_code_hash = filebase64sha256(var.lambda_zip_path)
  role          = var.batch_role_arn
  filename      = var.lambda_zip_path
  timeout       = 300   # 5 min – generous for large S3 files; SQS visibility matches
  memory_size   = 512   # 512 MB covers in-memory dedup sets for 100-record batches

  # Lambda SG is passed in from root — it was created there to break the
  # circular dependency between this module and the aurora module.
  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [var.lambda_sg_id]
  }

  environment {
    variables = local.common_env
  }

  # No reserved concurrency execution set because its a free-tier account 
  # and it needs a minimum of 10 unreserved at all times,
  # and 10 is the default limit for Lambda concurrent executions in a new account.
  reserved_concurrent_executions = -1 

  depends_on = [aws_cloudwatch_log_group.batch]
}

# Wire SQS queue → Lambda (batch, up to 100 messages per invocation)
resource "aws_lambda_event_source_mapping" "sqs_to_batch" {
  event_source_arn                   = var.sqs_queue_arn
  function_name                      = aws_lambda_function.batch_processor.arn
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = 10  # wait up to 10 s to collect a full batch

  # On partial batch failure: only retry failed records, not the whole batch
  function_response_types = ["ReportBatchItemFailures"]
}

# =============================================================================
# 2. Stream Processor Lambda (Kinesis trigger)
# =============================================================================
# resource "aws_lambda_function" "stream_processor" {
#   function_name = "${var.project}-${var.environment}-stream"
#   description   = "Consumes Kinesis meter events, deduplicates, and batch-inserts to Aurora"
#   runtime       = "python3.12"
#   handler       = "stream_processor.handler"
#   role          = var.stream_role_arn
#   filename      = var.lambda_zip_path
#   timeout       = 180
#   memory_size   = 256

#   vpc_config {
#     subnet_ids         = var.private_subnet_ids
#     security_group_ids = [var.lambda_sg_id]
#   }

#   environment {
#     variables = local.common_env
#   }

#   reserved_concurrent_executions = 20  # one per Kinesis shard * safety factor

#   depends_on = [aws_cloudwatch_log_group.stream]
# }

# # Wire Kinesis stream → Lambda
# resource "aws_lambda_event_source_mapping" "kinesis_to_stream" {
#   count            = var.kinesis_stream_arn != "" ? 1 : 0  # only create if ARN is provided
#   event_source_arn  = var.kinesis_stream_arn
#   function_name     = aws_lambda_function.stream_processor.arn
#   starting_position = var.starting_position
#   batch_size        = 100

#   # Bisect-on-error: splits a failing batch in half to isolate bad records
#   # rather than retrying the full batch indefinitely
#   bisect_batch_on_function_error = true

#   # Destination for records that fail after all retries
#   destination_config {
#     on_failure {
#       destination_arn = var.sqs_queue_arn  # re-use the DLQ concept via SQS
#     }
#   }
# }

# CloudWatch Alarms 
# Alert if either Lambda has errors in the last 5 minutes
resource "aws_cloudwatch_metric_alarm" "batch_errors" {
  alarm_name          = "${var.project}-${var.environment}-batch-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Batch Lambda has >5 errors in 5 min"
  dimensions = { FunctionName = aws_lambda_function.batch_processor.function_name }
}

# resource "aws_cloudwatch_metric_alarm" "stream_errors" {
#   alarm_name          = "${var.project}-${var.environment}-stream-errors"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "Errors"
#   namespace           = "AWS/Lambda"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 5
#   alarm_description   = "Stream Lambda has >5 errors in 5 min"
#   dimensions = { FunctionName = aws_lambda_function.stream_processor.function_name }
# }

# Outputs 
output "batch_function_name"  { value = aws_lambda_function.batch_processor.function_name }
# output "stream_function_name" { value = aws_lambda_function.stream_processor.function_name }
# lambda_sg_id is no longer output from here — it lives in root and is passed IN
