variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short name used to prefix resource names and tags."
  type        = string
  default     = "market-data-pipeline"
}

variable "my_ip_cidr" {
  description = <<-EOT
    Your public IP in CIDR notation, e.g. "1.2.3.4/32".
    SSH and the Airflow UI are restricted to this address only.
    Find yours with: curl.exe ifconfig.me
    If your ISP rotates your IP you'll lose access; update this and re-apply.
  EOT
  type        = string

  validation {
    condition     = can(cidrhost(var.my_ip_cidr, 0))
    error_message = "my_ip_cidr must be valid CIDR notation, e.g. 1.2.3.4/32."
  }
}

variable "ec2_instance_type" {
  description = <<-EOT
    Instance type for the Airflow host.
    t3.medium (4 GB RAM) is the minimum. On a t3.small the scheduler goes
    unhealthy on the first scheduled run.
  EOT
  type        = string
  default     = "t3.medium"
}

variable "ec2_key_name" {
  description = <<-EOT
    Name of an existing EC2 key pair in this region, used for SSH.
    Create one first:
      aws ec2 create-key-pair --key-name market-pipeline-key \
        --query "KeyMaterial" --output text > market-pipeline-key.pem
  EOT
  type        = string
}

variable "ec2_root_volume_gb" {
  description = "Root EBS volume size in GB. Airflow images are large."
  type        = number
  default     = 30
}
