# ---------------------------------------------------------------------------
# IAM role for the EC2 instance.
#
# The instance gets temporary, auto-rotating credentials via an instance
# profile instead of us baking an access key into the box. Nothing to leak,
# nothing to rotate manually.
#
# The policy is scoped to this one bucket. Compare to the AdministratorAccess
# user you're running Terraform with: that one is broad for convenience, this
# one is what the pipeline actually runs as.
# ---------------------------------------------------------------------------

# Trust policy: says EC2 is allowed to assume this role.
data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "${var.project_name}-airflow-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

# Permission policy: what the role may actually do.
data "aws_iam_policy_document" "s3_access" {
  # Bucket-level: needed to list objects and read the bucket's location.
  statement {
    sid    = "BucketLevelAccess"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]

    resources = [aws_s3_bucket.raw_data.arn]
  }

  # Object-level: read/write/delete inside the bucket.
  # Note the /* suffix. Bucket ARN and object ARN are different resources,
  # and mixing them up is the most common reason an S3 policy silently
  # fails to work.
  statement {
    sid    = "ObjectLevelAccess"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:GetObjectVersion",
    ]

    resources = ["${aws_s3_bucket.raw_data.arn}/*"]
  }
}

resource "aws_iam_role_policy" "s3_access" {
  name   = "${var.project_name}-s3-access"
  role   = aws_iam_role.airflow.id
  policy = data.aws_iam_policy_document.s3_access.json
}

# An instance profile is the wrapper that lets an EC2 instance wear a role.
# You cannot attach a role to an instance directly.
resource "aws_iam_instance_profile" "airflow" {
  name = "${var.project_name}-airflow-profile"
  role = aws_iam_role.airflow.name
}
