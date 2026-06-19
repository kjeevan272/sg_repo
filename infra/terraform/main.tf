terraform {
  required_providers { aws = { source = "hashicorp/aws", version = "~> 5.0" } }
}

provider "aws" { region = var.region }
variable "region" { default = "eu-central-1" }
variable "bucket" { default = "softgames-payments" }

resource "aws_s3_bucket" "lake" {
  bucket = var.bucket
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "bronze-to-glacier"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

resource "aws_glue_catalog_database" "gold" {
  name = "softgames_gold"
}
