# ---------------------------------------------------------------------------------------------
# Unity Catalog storage credential role (Databricks reads raw files, writes Delta tables)
# ---------------------------------------------------------------------------------------------
locals {
  uc_role_name = "${local.prefix}-unity-catalog"
  uc_role_arn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.uc_role_name}"
}

data "aws_iam_policy_document" "uc_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type = "AWS"
      # UC requires the role to be self-assuming in addition to trusting the Databricks master role.
      identifiers = [var.databricks_uc_master_role_arn, local.uc_role_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_account_id]
    }
  }
}

resource "aws_iam_role" "unity_catalog" {
  name               = local.uc_role_name
  assume_role_policy = data.aws_iam_policy_document.uc_trust.json
}

data "aws_iam_policy_document" "lakehouse_rw" {
  statement {
    sid       = "LakehouseReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn, "${aws_s3_bucket.lakehouse.arn}/*"]
  }
  statement {
    sid       = "LakeKms"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey*", "kms:DescribeKey"]
    resources = [aws_kms_key.lake.arn]
  }
  statement {
    sid       = "SelfAssume"
    actions   = ["sts:AssumeRole"]
    resources = [local.uc_role_arn]
  }
}

resource "aws_iam_role_policy" "unity_catalog" {
  name   = "lakehouse-access"
  role   = aws_iam_role.unity_catalog.id
  policy = data.aws_iam_policy_document.lakehouse_rw.json
}

# ---------------------------------------------------------------------------------------------
# Airflow (MWAA / EKS / EC2) - least privilege: sense + land files, publish alerts, read secret
# ---------------------------------------------------------------------------------------------
data "aws_iam_policy_document" "airflow" {
  statement {
    sid       = "ReadLanding"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.landing.arn, "${aws_s3_bucket.landing.arn}/*"]
  }
  statement {
    sid       = "WriteRawZone"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse.arn, "${aws_s3_bucket.lakehouse.arn}/raw/*"]
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey*"]
    resources = [aws_kms_key.lake.arn]
  }
  statement {
    sid       = "Alerts"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.pipeline_alerts.arn]
  }
  statement {
    sid       = "DatabricksSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.databricks_token.arn]
  }
}

resource "aws_iam_policy" "airflow" {
  name   = "${local.prefix}-airflow"
  policy = data.aws_iam_policy_document.airflow.json
}
