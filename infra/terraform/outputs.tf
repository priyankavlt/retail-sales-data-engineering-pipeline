output "landing_bucket" {
  value = aws_s3_bucket.landing.id
}

output "lakehouse_bucket" {
  value = aws_s3_bucket.lakehouse.id
}

output "incoming_uri" {
  value = "s3://${aws_s3_bucket.landing.id}/incoming"
}

output "lakehouse_uri" {
  value = "s3://${aws_s3_bucket.lakehouse.id}"
}

output "unity_catalog_role_arn" {
  value = aws_iam_role.unity_catalog.arn
}

output "airflow_policy_arn" {
  value = aws_iam_policy.airflow.arn
}

output "alerts_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}

output "catalog" {
  value = databricks_catalog.walmart.name
}
