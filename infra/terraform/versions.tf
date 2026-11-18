terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }

  # Remote state; create the bucket/table once, then `terraform init -backend-config=...`.
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "walmart-retail-lakehouse"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

provider "databricks" {
  host = var.databricks_host
}
