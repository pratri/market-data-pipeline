# ---------------------------------------------------------------------------
# S3 bucket for raw data landing.
#
# Bucket names are globally unique across all AWS accounts, so we append a
# random suffix rather than hoping "market-data-pipeline" is free.
# ---------------------------------------------------------------------------

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.project_name}-raw-${random_id.bucket_suffix.hex}"
}

# Versioning keeps the previous object when something overwrites it.
# A bad pipeline run that clobbers good data is recoverable instead of fatal.
resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption at rest. SSE-S3 is free and requires no key
# management, unlike SSE-KMS which bills per request.
resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block all public access. Defaults changed in 2023 to block by default,
# but stating it explicitly means the intent survives a provider change.
resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Versioning means overwritten objects accumulate forever and keep billing.
# Expire noncurrent versions after 30 days, and clean up failed multipart
# uploads, which otherwise linger invisibly and cost money.
resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  # Explicit dependency: applying lifecycle rules before versioning is
  # enabled can fail, and Terraform can't infer the ordering itself.
  depends_on = [aws_s3_bucket_versioning.raw_data]

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
