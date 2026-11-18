# Unity Catalog objects: storage credential -> external location -> catalog + medallion schemas.
resource "databricks_storage_credential" "lakehouse" {
  name = "${local.prefix}-lakehouse"
  aws_iam_role {
    role_arn = aws_iam_role.unity_catalog.arn
  }
  comment = "Managed by Terraform"
}

resource "databricks_external_location" "lakehouse" {
  name            = "${local.prefix}-lakehouse"
  url             = "s3://${aws_s3_bucket.lakehouse.id}/"
  credential_name = databricks_storage_credential.lakehouse.name
  comment         = "Raw zone + managed storage for the walmart catalog"
}

resource "databricks_catalog" "walmart" {
  name         = local.catalog_name
  storage_root = "s3://${aws_s3_bucket.lakehouse.id}/managed"
  comment      = "Walmart retail sales lakehouse"
  depends_on   = [databricks_external_location.lakehouse]
}

resource "databricks_schema" "layers" {
  for_each     = toset(["bronze", "silver", "gold", "staging", "intermediate", "reference", "snapshots", "ops"])
  catalog_name = databricks_catalog.walmart.name
  name         = each.key
}
