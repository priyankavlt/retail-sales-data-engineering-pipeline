variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "dev"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod"
  }
}

variable "name_prefix" {
  type    = string
  default = "walmart-retail"
}

variable "databricks_host" {
  type        = string
  description = "Workspace URL, e.g. https://dbc-xxxx.cloud.databricks.com"
}

variable "databricks_account_id" {
  type        = string
  description = "Databricks account ID (external ID for the Unity Catalog trust policy)"
  sensitive   = true
}

variable "databricks_uc_master_role_arn" {
  type        = string
  description = "Databricks-owned Unity Catalog master role allowed to assume our storage role"
  default     = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
}

variable "alert_email" {
  type    = string
  default = null
}

variable "raw_retention_days" {
  type        = number
  default     = 365
  description = "Days before landed raw files transition to Glacier Instant Retrieval"
}
