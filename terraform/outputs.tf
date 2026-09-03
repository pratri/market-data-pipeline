output "s3_bucket_name" {
  description = "Raw data bucket. Set this as S3_BUCKET in your .env."
  value       = aws_s3_bucket.raw_data.bucket
}

output "s3_bucket_arn" {
  description = "Bucket ARN."
  value       = aws_s3_bucket.raw_data.arn
}

output "ec2_public_ip" {
  description = "Public IP of the Airflow host."
  value       = aws_instance.airflow.public_ip
}

output "ssh_command" {
  description = "Copy-paste SSH command."
  value       = "ssh -i ${var.ec2_key_name}.pem ubuntu@${aws_instance.airflow.public_ip}"
}

output "airflow_ui_url" {
  description = "Airflow UI, once it's running on the box."
  value       = "http://${aws_instance.airflow.public_ip}:8080"
}
