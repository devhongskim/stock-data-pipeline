resource "aws_s3_bucket" "stock_pipeline_bucket" {
  bucket = var.bucket_name

  tags = {
    Environment = "Production"
    Project     = "Stock-Market-ETL"
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_public_access_block" "block_public" {
  bucket = aws_s3_bucket.stock_pipeline_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
