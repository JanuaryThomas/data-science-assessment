variable "region" {}
variable "app_name" {}
variable "environment" {}
variable "prefix" {}
variable "aws_account_id" {}

resource "aws_s3_bucket" "cultivo_datalake_s3_bucket" {
  bucket        = "${var.prefix}-${var.app_name}-bucket"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "cultivo_datalake_s3_bucket_public_block_config" {
  bucket = aws_s3_bucket.cultivo_datalake_s3_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "cultivo_datalake_s3_bucket_encryption" {
  bucket = aws_s3_bucket.cultivo_datalake_s3_bucket.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_metric" "datalake_s3_bucket_metric" {
  bucket = aws_s3_bucket.cultivo_datalake_s3_bucket.id
  name   = "EntireBucket"
}

resource "aws_s3_bucket_ownership_controls" "cultivo_datalake_s3_bucket_owner_control" {
  bucket = aws_s3_bucket.cultivo_datalake_s3_bucket.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "cultivo_datalake_s3_bucket_acl" {
  bucket     = aws_s3_bucket.cultivo_datalake_s3_bucket.id
  acl        = "private"
  depends_on = [aws_s3_bucket_ownership_controls.cultivo_datalake_s3_bucket_owner_control]
}

resource "aws_s3_bucket_versioning" "versioning_example" {
  bucket = aws_s3_bucket.cultivo_datalake_s3_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

output "cultivo_s3_bucket" {
  value = aws_s3_bucket.cultivo_datalake_s3_bucket.id
}

