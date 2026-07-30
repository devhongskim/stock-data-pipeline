variable "bucket_name" {
  description = "Name of the existing S3 bucket used by the stock pipeline (Bronze/Silver/Gold layers + DuckDB warehouse backups)"
  type        = string
}
