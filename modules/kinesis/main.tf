# =============================================================================
# Module: Kinesis Data Stream
# Handles the MQTT/real-time streaming path from IoT Core.
# Shard count is parameterised so it can be scaled via a tfvars change
# without structural pipeline changes.
# =============================================================================

variable "project"          { type = string }
variable "environment"      { type = string }
variable "shard_count"      { 
                            type = number 
                            default = 2 
                            }
variable "retention_hours"  { 
                            type = number
                            default = 24 
                            }

# resource "aws_kinesis_stream" "meter_events" {
#   name             = "${var.project}-${var.environment}-meter-events"
#   shard_count      = var.shard_count
#   retention_period = var.retention_hours

#   # Enhanced fan-out + shard-level metrics helps to observe per-shard lag
#   shard_level_metrics = [
#     "IncomingBytes",
#     "IncomingRecords",
#     "OutgoingBytes",
#     "OutgoingRecords",
#     "WriteProvisionedThroughputExceeded",
#     "ReadProvisionedThroughputExceeded",
#     "IteratorAgeMilliseconds",
#   ]

#   # Encrypt records at rest using the AWS-managed Kinesis key
#   encryption_type = "KMS"
#   kms_key_id      = "alias/aws/kinesis"

#   stream_mode_details {
#     # ON_DEMAND automatically scales shards – switch from PROVISIONED if
#     # traffic is highly variable and hard to predict
#     stream_mode = "PROVISIONED"
#   }
# }

# CloudWatch alarm: alert if any shard's iterator age exceeds 60 s
# (means Lambda is falling behind – could indicate a processing bottleneck)
# resource "aws_cloudwatch_metric_alarm" "iterator_age" {
#   alarm_name          = "${var.project}-${var.environment}-kinesis-iterator-age"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 2
#   metric_name         = "GetRecords.IteratorAgeMilliseconds"
#   namespace           = "AWS/Kinesis"
#   period              = 60
#   statistic           = "Maximum"
#   threshold           = 60000  # 60 seconds
#   alarm_description   = "Kinesis consumer is falling behind – Lambda may need more concurrency"

#   dimensions = {
#     StreamName = aws_kinesis_stream.meter_events.name
#   }
# }

# output "stream_arn"  { value = aws_kinesis_stream.meter_events.arn }
# output "stream_name" { value = aws_kinesis_stream.meter_events.name }
