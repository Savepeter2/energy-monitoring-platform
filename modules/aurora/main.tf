# =============================================================================
# Module: Aurora Serverless v2 (PostgreSQL-compatible)
# Why Aurora Serverless v2?
#   - Auto-scales ACUs (Aurora Capacity Units) from 0.5 to 16 in seconds
#   - Writer instance handles all writes; read replica offloads dashboard queries
#   - Private subnet placement: no internet route, accessible only from Lambda
#   - Credentials stored in Secrets Manager (auto-rotated every 30 days)
# =============================================================================

variable "project"            { type = string }
variable "environment"        { type = string }
variable "vpc_id"             { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "allowed_sg_ids"     { type = list(string) }
variable "min_capacity"       { 
                                type = number
                                default = 0.5 
                              }
variable "max_capacity"       { 
                                type = number
                                default = 3 
                              }
variable "db_name"            { 
                                type = string 
                                default = "smarterise_iot" 
                              }

data "aws_region" "current" {}

# Security group: only accept Postgres port from Lambda 
resource "aws_security_group" "aurora" {
  name        = "${var.project}-${var.environment}-aurora"
  description = "Allow PostgreSQL only from Lambda security group"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = var.allowed_sg_ids
    description     = "PostgreSQL from Lambda"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Subnet group: Aurora must span 2+ AZs for automatic failover 
resource "aws_db_subnet_group" "aurora" {
  name       = "${var.project}-${var.environment}-aurora"
  subnet_ids = var.private_subnet_ids
  description = "Private subnets for Aurora cluster"
}

# Parameter group: tuned for IoT time-series workloads 
resource "aws_rds_cluster_parameter_group" "aurora" {
  name   = "${var.project}-${var.environment}-aurora-params"
  family = "aurora-postgresql15"

  # Enable pg_partman-compatible settings for partition pruning
  parameter {
    name  = "enable_partition_pruning"
    value = "on"
  }

  # Reduce idle connection overhead (critical with many Lambda invocations)
  parameter {
    name  = "idle_in_transaction_session_timeout"
    value = "30000"  # 30 seconds
  }

  parameter {
    name  = "statement_timeout"
    value = "60000"  # 60 seconds – prevents runaway dashboard queries
  }

  parameter {
  name  = "shared_preload_libraries"
  value = "pg_cron"
  apply_method = "pending-reboot"
}
}

# Random password for the master user 
resource "random_password" "db_master" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}:?"
}

# ── Secrets Manager: store credentials (auto-rotation configured separately) ──
resource "aws_secretsmanager_secret" "db_credentials" {
  name                    = "${var.project}/${var.environment}/aurora/master"
  recovery_window_in_days = 0  # allow immediate deletion during development; set to 7+ in prod
  description             = "Aurora master credentials – managed by Terraform"
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    username = "smarterise_admin"
    password = random_password.db_master.result
    host     = ""  # populated after cluster creation; use aws_secretsmanager_secret_rotation
    port     = 5432
    dbname   = var.db_name
  })
}

# Aurora Serverless v2 cluster 
resource "aws_rds_cluster" "aurora" {
  cluster_identifier      = "${var.project}-${var.environment}-aurora"
  engine                  = "aurora-postgresql"
  engine_mode             = "provisioned"   # Serverless v2 uses 'provisioned' mode
  engine_version          = "15.14"
  database_name           = var.db_name
  master_username         = "smarterise_admin"
  master_password         = random_password.db_master.result

  db_subnet_group_name            = aws_db_subnet_group.aurora.name
  vpc_security_group_ids          = [aws_security_group.aurora.id]
  db_cluster_parameter_group_name = aws_rds_cluster_parameter_group.aurora.name

  storage_encrypted = true  # encryption at rest using AWS-managed key

  serverlessv2_scaling_configuration {
    min_capacity = var.min_capacity
    max_capacity = var.max_capacity
    seconds_until_auto_pause = 300 
  }

  # Automatic backups: 1-day retention in the same region for dev environment
  backup_retention_period   = 1
  preferred_backup_window   = "02:00-03:00"  # 2–3 AM UTC
  skip_final_snapshot       = true
  final_snapshot_identifier = "${var.project}-${var.environment}-final-snapshot"

  # Enabled enhanced monitoring and performance insights
  enabled_cloudwatch_logs_exports = ["postgresql"]

  # Enabled RDS Data API – allows running SQL over HTTPS without a direct
  # VPC connection or psycopg2 driver. Required for the Query Editor in the
  # AWS Console and for any client using the @aws-sdk/rds-data SDK.
  # Only supported on Aurora Serverless v2 with engine_mode = "provisioned"
  enable_http_endpoint = true  
  deletion_protection = false  # acceptable for dev, should be set to true to prevent accidental cluster deletion in prod
}

# Writer instance 
resource "aws_rds_cluster_instance" "writer" {
  identifier          = "${var.project}-${var.environment}-writer"
  cluster_identifier  = aws_rds_cluster.aurora.id
  instance_class      = "db.serverless"
  engine              = aws_rds_cluster.aurora.engine
  engine_version      = aws_rds_cluster.aurora.engine_version

  performance_insights_enabled = true
  monitoring_interval   = 0  
}

# Read replica instance 
# Serves QuickSight and the web app so dashboard queries don't contend
# with Lambda writes on the writer instance.
resource "aws_rds_cluster_instance" "reader" {
  identifier          = "${var.project}-${var.environment}-reader"
  cluster_identifier  = aws_rds_cluster.aurora.id
  instance_class      = "db.serverless"
  engine              = aws_rds_cluster.aurora.engine
  engine_version      = aws_rds_cluster.aurora.engine_version

  performance_insights_enabled = true
  monitoring_interval          = 0 
}

# Outputs 
output "cluster_endpoint"        { 
                                   value = aws_rds_cluster.aurora.endpoint
                                   sensitive = true 
                                  }
output "reader_endpoint"         { 
                                    value = aws_rds_cluster.aurora.reader_endpoint
                                     sensitive = true 
                                  }
output "aurora_sg_id"            { value = aws_security_group.aurora.id }
output "db_secret_arn"           { value = aws_secretsmanager_secret.db_credentials.arn }
