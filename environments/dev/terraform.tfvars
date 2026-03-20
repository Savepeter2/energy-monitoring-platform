# environments/dev/terraform.tfvars
# Override root defaults for the dev environment

aws_region          = "us-east-1"
environment         = "dev"
project             = "smarterise"
vpc_cidr            = "10.0.0.0/16"

availability_zones  = ["us-east-1a", "us-east-1b"]
kinesis_shard_count = 1       
db_name             = "smarterise_iot_dev"
lambda_zip_path     = "./lambda_package.zip" 

