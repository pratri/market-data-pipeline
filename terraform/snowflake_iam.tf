# ---------------------------------------------------------------------------
# IAM role that Snowflake assumes to read the S3 bucket.
#
# This is cross-account access: the role lives in YOUR account, but the
# principal allowed to assume it lives in SNOWFLAKE's account. Snowflake
# generates that principal when you create the storage integration, which
# is why this takes two passes:
#
#   pass 1: apply with the placeholder values below, create the Snowflake
#           integration pointing at this role's ARN, then run
#           DESC INTEGRATION to read back what Snowflake generated
#   pass 2: put those values in terraform.tfvars and apply again
#
# The external ID is the important part. Without it, any Snowflake account
# could assume your role, since they all share the same IAM principal.
# The external ID is unique to your integration and is what stops the
# "confused deputy" problem where someone else's Snowflake account reads
# your bucket.
# ---------------------------------------------------------------------------

variable "snowflake_iam_user_arn" {
  description = <<-EOT
    STORAGE_AWS_IAM_USER_ARN from `DESC INTEGRATION s3_int` in Snowflake.
    Placeholder on the first apply; fill in and re-apply after creating
    the integration.
  EOT
  type        = string
    default     = "arn:aws:iam::742031403615:root"
}

variable "snowflake_external_id" {
  description = <<-EOT
    STORAGE_AWS_EXTERNAL_ID from `DESC INTEGRATION s3_int` in Snowflake.
    Placeholder on the first apply.
  EOT
  type        = string
  default     = "placeholder_external_id"
}

data "aws_iam_policy_document" "snowflake_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [var.snowflake_iam_user_arn]
    }

    # This condition is the security boundary. Snowflake sends the
    # external ID when assuming the role; a different Snowflake account
    # doesn't know yours and so can't assume it.
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake" {
  name               = "${var.project_name}-snowflake-role"
  description        = "Assumed by Snowflake to read Parquet from the raw bucket"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume_role.json
}

# Read-only. Snowflake loads data out; it has no reason to write back.
# GetObjectVersion is needed because the bucket has versioning enabled.
data "aws_iam_policy_document" "snowflake_s3_read" {
  statement {
    sid    = "ReadObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]

    resources = ["${aws_s3_bucket.raw_data.arn}/*"]
  }

  # ListBucket is required for COPY INTO to enumerate files under a
  # prefix. Without it you get an empty load and no useful error.
  statement {
    sid    = "ListBucket"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]

    resources = [aws_s3_bucket.raw_data.arn]
  }
}

resource "aws_iam_role_policy" "snowflake_s3_read" {
  name   = "${var.project_name}-snowflake-s3-read"
  role   = aws_iam_role.snowflake.id
  policy = data.aws_iam_policy_document.snowflake_s3_read.json
}

output "snowflake_role_arn" {
  description = "Paste this into STORAGE_AWS_ROLE_ARN when creating the integration."
  value       = aws_iam_role.snowflake.arn
}
