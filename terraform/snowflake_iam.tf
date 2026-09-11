# Role Snowflake assumes to read the bucket.
#
# Takes two applies. First apply with the placeholders, create the storage
# integration in Snowflake, run DESC INTEGRATION, put the IAM user ARN and
# external ID into terraform.tfvars, then apply again. The external ID is
# what stops other Snowflake accounts from assuming this role.

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

    # external ID check
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

# read only. GetObjectVersion because versioning is on.
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

  # COPY INTO needs ListBucket to find files, otherwise it silently loads nothing
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
