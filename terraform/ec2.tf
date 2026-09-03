# ---------------------------------------------------------------------------
# EC2 host for Airflow.
# ---------------------------------------------------------------------------

# Look up the current Ubuntu 22.04 AMI rather than hardcoding an ID.
# AMI IDs differ per region and change whenever Canonical publishes a new
# image, so a hardcoded one rots and breaks anyone else running this.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical's official AWS account ID

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "airflow" {
  name        = "${var.project_name}-airflow-sg"
  description = "SSH and Airflow UI, restricted to a single source IP"

  # SSH. Locked to your IP only. Port 22 open to 0.0.0.0/0 gets scanned
  # and brute-forced within hours of an instance coming up.
  ingress {
    description = "SSH from my IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }

  # Airflow web UI. Also locked down: the default install has weak auth
  # and exposing it publicly is a real compromise vector.
  ingress {
    description = "Airflow UI from my IP"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }

  # Unrestricted egress. The instance needs to reach Yahoo, SEC, Docker Hub,
  # Snowflake, and apt repositories.
  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-airflow-sg"
  }
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.ec2_instance_type
  key_name               = var.ec2_key_name
  vpc_security_group_ids = [aws_security_group.airflow.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow.name

  root_block_device {
    volume_size           = var.ec2_root_volume_gb
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  # Bootstrap script, runs once on first boot as root.
  # Installs Docker and Compose so the box is ready for Airflow.
  user_data = <<-EOF
    #!/bin/bash
    set -euxo pipefail

    apt-get update
    apt-get install -y ca-certificates curl gnupg git

    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
      | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg

    echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.gpg] \
      https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
      > /etc/apt/sources.list.d/docker.list

    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io \
      docker-buildx-plugin docker-compose-plugin

    # Let the default ubuntu user run docker without sudo.
    usermod -aG docker ubuntu

    systemctl enable docker
    systemctl start docker

    # t3.small has 2 GB RAM. Airflow's scheduler and webserver together can
    # spike past that and get OOM-killed. Swap absorbs the spikes.
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
  EOF

  # Changing user_data would otherwise destroy and recreate the instance.
  # Once Airflow is set up on the box you don't want that happening by
  # accident. Comment this out if you're still iterating on the bootstrap.
  lifecycle {
    ignore_changes = [user_data]
  }

  tags = {
    Name = "${var.project_name}-airflow"
  }
}
