# =============================================================================
# Module: SQS
# Two queues:
#   1. Main queue  – receives S3 event notifications; decouples ingestion
#                    from Lambda processing so bursts don't drop messages
#   2. Dead-letter queue (DLQ) – receives messages that failed 3 times
#                    so they can be inspected and replayed
# =============================================================================

variable "project"            { type = string }
variable "environment"        { type = string }
variable "visibility_timeout" { 
                               type = number
                               default = 300 
                               }
variable "max_receive_count"  { 
                                type = number
                                default = 3 
                              }

data "aws_caller_identity" "current" {}

# Dead-letter queue (created first so the main queue can reference its ARN) 
resource "aws_sqs_queue" "dlq" {
  name                       = "${var.project}-${var.environment}-meter-dlq"
  message_retention_seconds  = 1209600  # 14 days – plenty of time to investigate
  kms_master_key_id          = "alias/aws/sqs"  # encryption at rest
}

# ── Main processing queue ──────────────────────────────────────────────────────
# Main queue — NO KMS encryption so S3 can validate and write to it
# S3 event notifications cannot use KMS-encrypted queues unless it is
# grant S3 explicit kms:GenerateDataKey permission on the key, which
# requires a custom key policy. Using unencrypted (SSE-SQS) is simpler
# and still secure — messages are encrypted at rest by SQS natively

resource "aws_sqs_queue" "main" {
  name                       = "${var.project}-${var.environment}-meter-queue"
  visibility_timeout_seconds = var.visibility_timeout
  message_retention_seconds  = 86400  # 1 day – if Lambda is down, messages wait for 

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

# Queue policy: allow S3 to send notifications
data "aws_iam_policy_document" "sqs_s3_send" {
  statement {
    sid     = "AllowS3Send"
    effect  = "Allow"
    actions = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.main.arn]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    # Restricted to the specific account to prevent confused-deputy attacks
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:s3:::smarterise-dev-raw"] 
      #this was hardcoded to avoid circular dependency, because the s3 raw bucket arn isn't available yet at the time 
      #the policy is being created; ideally would reference module.s3.raw_bucket_arn - will be fixed in prod env
    }

  }
}

resource "aws_sqs_queue_policy" "main" {
  queue_url = aws_sqs_queue.main.url
  policy    = data.aws_iam_policy_document.sqs_s3_send.json
}

# CloudWatch alarm: alert when DLQ has messages waiting
# This means something in the pipeline is repeatedly failing
resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  alarm_name          = "${var.project}-${var.environment}-dlq-messages"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Messages in DLQ – investigate failed Lambda invocations"

  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }
}

output "queue_arn"  { value = aws_sqs_queue.main.arn }
output "queue_url"  { value = aws_sqs_queue.main.url }
output "dlq_arn"    { value = aws_sqs_queue.dlq.arn }
