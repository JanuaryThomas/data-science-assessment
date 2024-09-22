variable "key_name" {}
variable "region" {}
variable "app_name" {}
variable "environment" {}
variable "prefix" {}
variable "aws_account_id" {}



resource "aws_athena_workgroup" "intelecs_athena_query" {
  name          = "${var.app_name}-workgroup"
  state         = "ENABLED"
  force_destroy = true

  configuration {
    bytes_scanned_cutoff_per_query     = 5000000000000
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    engine_version {
      selected_engine_version = "AUTO"
    }

    result_configuration {
      acl_configuration {
        s3_acl_option = "BUCKET_OWNER_FULL_CONTROL"
      }
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
      output_location = "s3://${var.prefix}-${var.app_name}-bucket/athena-query-results"
    }
  }
}

resource "aws_glue_catalog_database" "intelecs_glue_database" {
  name        = "cultivo_database"
  description = "Cultivo Catalog"
}
