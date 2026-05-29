# =============================================================================
# Module: IAM
# Principle of least privilege: each Lambda function gets exactly the
# permissions it needs – no wildcard * actions, no * resources.
# =============================================================================

variable "project"              { type = string }
variable "environment"          { type = string }
variable "raw_bucket_arn"        { type = string }
variable "processed_bucket_arn"  { type = string }
variable "sqs_queue_arn"         { type = string }
variable "sqs_dlq_arn"           { type = string }
variable "aurora_secret_arn"     { type = string }

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Lambda assume-role trust policy (shared by both Lambda functions) 
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# =============================================================================
# Batch Lambda role – triggered by SQS, reads S3, writes Aurora & S3
# =============================================================================
resource "aws_iam_role" "lambda_batch" {
  name               = "${var.project}-${var.environment}-lambda-batch"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "lambda_batch_policy" {
  # SQS: receive and delete messages (needed to acknowledge processed records)
  statement {
    sid    = "SQSConsume"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [var.sqs_queue_arn]
  }

  # SQS DLQ: only send (Lambda writes failures to DLQ)
  statement {
    sid     = "SQSSendDLQ"
    effect  = "Allow"
    actions = ["sqs:SendMessage"]
    resources = [var.sqs_dlq_arn]
  }

  # S3 raw: read-only (Lambda reads the raw meter files)
  statement {
    sid     = "S3RawRead"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:HeadObject"]
    resources = ["${var.raw_bucket_arn}/raw/*"]
  }

  # S3 processed: write-only (Lambda writes Parquet output)
  statement {
    sid     = "S3ProcessedWrite"
    effect  = "Allow"
    actions = ["s3:PutObject"]
    resources = ["${var.processed_bucket_arn}/*"]
  }

  # Secrets Manager: read database credentials
  statement {
    sid     = "SecretsManagerRead"
    effect  = "Allow"
    actions = ["secretsmanager:GetSecretValue"]
    resources = [var.aurora_secret_arn]
  }

  # CloudWatch Logs: write Lambda execution logs
  statement {
    sid     = "CloudWatchLogs"
    effect  = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.project}-${var.environment}-batch:*"
    ]
  }

  # VPC: allow Lambda to create ENIs in private subnets
  statement {
    sid     = "VPCNetworking"
    effect  = "Allow"
    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface",
    ]
    resources = ["*"]  # EC2 VPC actions require * resource
  }
}

resource "aws_iam_policy" "lambda_batch" {
  name   = "${var.project}-${var.environment}-lambda-batch-policy"
  policy = data.aws_iam_policy_document.lambda_batch_policy.json
}

resource "aws_iam_role_policy_attachment" "lambda_batch" {
  role       = aws_iam_role.lambda_batch.name
  policy_arn = aws_iam_policy.lambda_batch.arn
}

output "lambda_batch_role_arn"  { value = aws_iam_role.lambda_batch.arn }
