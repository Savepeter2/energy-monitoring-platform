# =============================================================================
# Smarterise IoT Energy Platform – Root Terraform Configuration
# Wires all modules together. Each module is independently reusable.
# =============================================================================

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "Smarterise"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# =============================================================================
# 1. VPC – private subnets isolate Lambda and Aurora from the internet
# =============================================================================
module "vpc" {
  source = "./modules/vpc"

  project     = var.project
  environment = var.environment
  vpc_cidr    = var.vpc_cidr
  azs         = var.availability_zones
}

# =============================================================================
# 2. S3 – raw landing bucket (versioned) and processed/parquet bucket
# =============================================================================
module "s3" {
  source = "./modules/s3"

  project     = var.project
  environment = var.environment

  # Raw bucket lifecycle: move to IA after 30 days, Glacier after 90, expire after 365
  raw_ia_days      = 30
  raw_glacier_days = 90
  raw_expire_days  = 365
}

# =============================================================================
# 3. SQS – decouples S3 event notifications from Lambda processing
# =============================================================================
module "sqs" {
  source = "./modules/sqs"

  project     = var.project
  environment = var.environment

  # How long a message stays invisible while being processed (seconds)
  visibility_timeout = 300

  # Dead-letter queue receives messages after 3 failed attempts
  max_receive_count = 3
}

# S3 → SQS notification wired here at root level where both ARNs are resolved.
# This avoids passing sqs_queue_arn into the S3 module before SQS exists.
resource "aws_s3_bucket_notification" "raw" {
  bucket = module.s3.raw_bucket_id

  queue {
    queue_arn     = module.sqs.queue_arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
  }

  # S3 validates it can deliver to the queue before saving this config.
  # The queue policy (which grants S3 permission to send) must exist first.
  depends_on = [module.sqs]

}


# =============================================================================
# 4. Kinesis Data Stream – handles the MQTT streaming path
# =============================================================================
module "kinesis" {
  source = "./modules/kinesis"

  project        = var.project
  environment    = var.environment
  shard_count    = var.kinesis_shard_count  # start at 2; scale out as sites grow
  retention_hours = 24                       # 24 h replay window for recovery
}

# =============================================================================
# 5. IAM – least-privilege roles for every compute resource
# =============================================================================
module "iam" {
  source = "./modules/iam"

  project             = var.project
  environment         = var.environment
  raw_bucket_arn      = module.s3.raw_bucket_arn
  processed_bucket_arn = module.s3.processed_bucket_arn
  sqs_queue_arn       = module.sqs.queue_arn
  sqs_dlq_arn         = module.sqs.dlq_arn
  aurora_secret_arn   = module.aurora.db_secret_arn
}

# =============================================================================
# 5b. Lambda security group – created HERE in root, not inside either module.
#
# WHY: The original design had aurora module needing the Lambda SG (to allow
# inbound Postgres from Lambda), and the lambda module needing the Aurora SG
# (to allow outbound Postgres to Aurora). Terraform saw a cycle and refused
# to plan. The fix is to lift the Lambda SG out of both modules into root,
# so it exists before either module is evaluated. Root then passes the SG id
# into both modules as a plain input — no circular reference.
# =============================================================================
resource "aws_security_group" "lambda" {
  name        = "${var.project}-${var.environment}-lambda"
  description = "Lambda: outbound to Aurora port 5432 and HTTPS to AWS services"
  vpc_id      = module.vpc.vpc_id

  # Outbound to Aurora — references aurora SG by id AFTER aurora module runs.
  # This direction is fine: Lambda SG is created first (root), Aurora SG is
  # created second (aurora module), then this rule is added as a separate
  # aws_security_group_rule to avoid the cycle.
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS to AWS services via VPC endpoints"
  }

  # Port 5432 egress to Aurora is added AFTER aurora module creates its SG,
  # using a standalone rule below — this is the key cycle-breaking pattern.
}

# Add the Postgres egress rule separately, after aurora module has run.
# aws_security_group_rule has no output that aurora depends on, so there
# is no cycle — Terraform can resolve the order correctly.
resource "aws_security_group_rule" "lambda_to_aurora" {
  type                     = "egress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.lambda.id
  source_security_group_id = module.aurora.aurora_sg_id
  description              = "PostgreSQL outbound from Lambda to Aurora"
}

# =============================================================================
# 6. Aurora Serverless v2 – scales automatically; private subnet only
# =============================================================================
module "aurora" {
  source = "./modules/aurora"

  project            = var.project
  environment        = var.environment
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  # Pass the root-level Lambda SG — this is now just a plain string value,
  # no circular reference because aws_security_group.lambda is a root resource.
  allowed_sg_ids     = [aws_security_group.lambda.id]

  min_capacity = 0.5 
  max_capacity = 3

  db_name = var.db_name
}

# =============================================================================
# 7. ElastiCache (Redis) – caches dashboard query results to spare Aurora
# =============================================================================
# (See modules/elasticache for full resource definition)

# =============================================================================
# 8. Lambda – two functions: batch (S3/SQS) and stream (Kinesis)
# =============================================================================
module "lambda" {
  source = "./modules/lambda"

  project             = var.project
  environment         = var.environment
  vpc_id              = module.vpc.vpc_id
  private_subnet_ids  = module.vpc.private_subnet_ids
  # Pass the root-level SG in — lambda module no longer creates its own SG
  lambda_sg_id        = aws_security_group.lambda.id
  batch_role_arn      = module.iam.lambda_batch_role_arn
  # stream_role_arn     = module.iam.lambda_stream_role_arn
  sqs_queue_arn       = module.sqs.queue_arn
  # kinesis_stream_arn  = module.kinesis.stream_arn
  processed_bucket    = module.s3.processed_bucket_id
  aurora_secret_arn   = module.aurora.db_secret_arn
  aurora_cluster_endpoint = module.aurora.cluster_endpoint
  db_name             = var.db_name

  batch_size          = 100
  starting_position   = "TRIM_HORIZON"

  lambda_zip_path     = var.lambda_zip_path
}

# =============================================================================
# Outputs – useful for CI/CD and manual inspection
# =============================================================================
output "raw_bucket_name" {
  value = module.s3.raw_bucket_id
}

# output "kinesis_stream_name" {
#   value = module.kinesis.stream_name
# }

output "aurora_cluster_endpoint" {
  value     = module.aurora.cluster_endpoint
  sensitive = true
}

output "lambda_batch_function_name" {
  value = module.lambda.batch_function_name
}

# output "lambda_stream_function_name" {
#   value = module.lambda.stream_function_name
# }
