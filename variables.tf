# Root variable declarations – override defaults in environments/dev/terraform.tfvars

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
  default     = "dev"
}

variable "project" {
  description = "Short project name used as a resource prefix"
  type        = string
  default     = "smarterise"
}

variable "vpc_cidr" {
  description = "CIDR block for the dedicated VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of AZs – at least 2 for high availability"
  type        = list(string)
  default     = ["eu-west-1a", "eu-west-1b"]
}

variable "kinesis_shard_count" {
  description = "Number of Kinesis shards (each handles 1 MB/s write, 2 MB/s read)"
  type        = number
  default     = 2
}

variable "db_name" {
  description = "Aurora database name"
  type        = string
  default     = "smarterise_iot"
}

variable "lambda_zip_path" {
  description = "Path to the zipped Lambda deployment package"
  type        = string
  default     = "lambda_package.zip"
}
