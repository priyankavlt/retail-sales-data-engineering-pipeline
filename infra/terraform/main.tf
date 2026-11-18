locals {
  prefix           = "${var.name_prefix}-${var.environment}"
  landing_bucket   = "${local.prefix}-landing-${data.aws_caller_identity.current.account_id}"
  lakehouse_bucket = "${local.prefix}-lakehouse-${data.aws_caller_identity.current.account_id}"
  catalog_name     = replace("${var.name_prefix}_${var.environment}", "-", "_")
}

data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------------------------
# Encryption
# ---------------------------------------------------------------------------------------------
resource "aws_kms_key" "lake" {
  description             = "${local.prefix} data lake encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "lake" {
  name          = "alias/${local.prefix}-lake"
  target_key_id = aws_kms_key.lake.key_id
}

# ---------------------------------------------------------------------------------------------
# Buckets: landing (source drop zone) and lakehouse (raw/bronze/silver/gold Delta tables)
# ---------------------------------------------------------------------------------------------
resource "aws_s3_bucket" "landing" {
  bucket = local.landing_bucket
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = local.lakehouse_bucket
}

locals {
  buckets = {
    landing   = aws_s3_bucket.landing
    lakehouse = aws_s3_bucket.lakehouse
  }
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = local.buckets
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = local.buckets
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = local.buckets
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

data "aws_iam_policy_document" "tls_only" {
  for_each = local.buckets
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = [each.value.arn, "${each.value.arn}/*"]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each   = local.buckets
  bucket     = each.value.id
  policy     = data.aws_iam_policy_document.tls_only[each.key].json
  depends_on = [aws_s3_bucket_public_access_block.this]
}

# Drop-zone files are copied to the raw zone on landing; keep them 30 days for replay.
resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    id     = "expire-incoming"
    status = "Enabled"
    filter {
      prefix = "incoming/"
    }
    expiration {
      days = 30
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# Raw files are immutable history: tier to Glacier IR, keep Delta noncurrent versions short.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    id     = "tier-raw"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = var.raw_retention_days
      storage_class = "GLACIER_IR"
    }
  }
  rule {
    id     = "expire-noncurrent"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

# ---------------------------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------------------------
resource "aws_sns_topic" "pipeline_alerts" {
  name              = "${local.prefix}-pipeline-alerts"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == null ? 0 : 1
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Databricks PAT / service-principal secret for Airflow and dbt. Value is set out of band.
resource "aws_secretsmanager_secret" "databricks_token" {
  name       = "${local.prefix}/databricks/token"
  kms_key_id = aws_kms_key.lake.arn
}
