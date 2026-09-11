# Role for the EC2 instance. Temporary creds through the instance profile,
# so no access keys on the box. Scoped to the raw bucket only.

# EC2 can assume this role
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

# what the role can do
data "aws_iam_policy_document" "s3_access" {
  # bucket level
  statement {
    sid    = "BucketLevelAccess"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]

    resources = [aws_s3_bucket.raw_data.arn]
  }

  # object level, note the /* on the ARN
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

# EC2 can't use a role directly, it needs an instance profile
resource "aws_iam_instance_profile" "airflow" {
  name = "${var.project_name}-airflow-profile"
  role = aws_iam_role.airflow.name
}
