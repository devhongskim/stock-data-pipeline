output "bucket_arn" {
  description = "ARN of the stock pipeline's S3 bucket"
  value       = aws_s3_bucket.stock_pipeline_bucket.arn
}

output "bucket_name" {
  description = "Name of the stock pipeline's S3 bucket"
  value       = aws_s3_bucket.stock_pipeline_bucket.bucket
}
