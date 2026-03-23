# =============================================================================
# Module: S3
# Creates two buckets:
#   1. raw/     – landing zone for FTP file drops (versioned + lifecycle)
#   2. processed/ – Parquet files written by Lambda after transformation
# Both buckets enforce encryption at rest and block all public access.
# =============================================================================

# Variables 
variable "project"         { type = string }
variable "environment"     { type = string }
variable "raw_ia_days"     { 
                           type = number
                           default = 30  
                           }
variable "raw_glacier_days"{ 
                          type = number
                          default = 90  
                          }

variable "raw_expire_days" { 
                            type = number
                            default = 365 
                           }

locals {
  raw_bucket_name       = "${var.project}-${var.environment}-raw"
  processed_bucket_name = "${var.project}-${var.environment}-processed"
}

# Raw Bucket 
resource "aws_s3_bucket" "raw" {
  bucket = local.raw_bucket_name

  force_destroy = true  # allows bucket to be deleted even if it contains objects (use with caution in production!)

  # Prevent accidental deletion of production data
  lifecycle {
    prevent_destroy = false # will be set to true in prod
  }
}

# Enable versioning – required for S3 Object Lock and audit trails
resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption with AWS-managed keys (upgrade to KMS for stricter compliance)
resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true  # reduces KMS API calls by ~99 % if you switch to KMS later
  }
}

# Block all public access – raw meter data must never be publicly readable
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle policy:
#   Day 0–29   → S3 Standard (hot, fast access for recent queries)
#   Day 30–89  → S3 Standard-IA (infrequently accessed, ~45 % cheaper)
#   Day 90–364 → S3 Glacier Instant Retrieval (~68 % cheaper than IA)
#   Day 365+   → Expire (delete) – adjust for your regulatory requirements


resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "raw-tier-transition"
    status = "Enabled"

    # Only apply to the raw/ prefix to leave other prefixes unaffected
    filter {
      prefix = "raw/"
    }

    transition {
      days          = var.raw_ia_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = var.raw_glacier_days
      storage_class = "GLACIER_IR"
    }

    expiration {
      days = var.raw_expire_days
    }

    # incomplete multipart uploads will be cleaned up after 7 days (to save storage costs)
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Processed Bucket 
resource "aws_s3_bucket" "processed" {
  bucket = local.processed_bucket_name
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Processed Parquet files: moved to IA after 60 days, keep indefinitely for analytics
resource "aws_s3_bucket_lifecycle_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    id     = "processed-tier-transition"
    status = "Enabled"
    filter { prefix = "" }

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
  }
}

output "raw_bucket_id"       { value = aws_s3_bucket.raw.id }
output "raw_bucket_arn"      { value = aws_s3_bucket.raw.arn }
output "processed_bucket_id" { value = aws_s3_bucket.processed.id }
output "processed_bucket_arn"{ value = aws_s3_bucket.processed.arn }
