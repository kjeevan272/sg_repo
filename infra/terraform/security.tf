// KMS CMK for at-rest encryption of the lake + Delta log.
resource "aws_kms_key" "lake" {
  description             = "softgames payments lake CMK"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

// Secrets in SSM Parameter Store (free) instead of Secrets Manager.
resource "aws_ssm_parameter" "api_token" {
  name  = "/softgames/payment-provider/token"
  type  = "SecureString"
  value = "REPLACE_ME"
  key_id = aws_kms_key.lake.id
}

// Lake Formation column masking: price visible to finance only.
resource "aws_lakeformation_data_cells_filter" "mask_price" {
  table_data {
    database_name    = aws_glue_catalog_database.gold.name
    name             = "fact_payment_transaction"
    table_catalog_id = data.aws_caller_identity.me.account_id
    column_names     = ["transaction_id", "game_key", "date_key", "status"]
  }
}

data "aws_caller_identity" "me" {}

// VPC endpoints so Spark/Lambda reach S3 + SSM without public egress.
variable "vpc_id" { default = "" }
variable "route_table_ids" { default = [] }

resource "aws_vpc_endpoint" "s3" {
  count             = var.vpc_id == "" ? 0 : 1
  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = var.route_table_ids
}
