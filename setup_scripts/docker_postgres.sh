#!/bin/bash

set -e  # Exit on error

USER_NAME=$(whoami)

echo "===== Updating system ====="
sudo apt-get update
sudo apt-get upgrade -y

echo "===== Installing prerequisites ====="
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

echo "===== Adding Docker’s official GPG key ====="
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/$(. /etc/os-release && echo "$ID")/gpg | \
    gpg --dearmor | sudo tee /etc/apt/keyrings/docker.gpg > /dev/null
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo "===== Adding Docker repository ====="
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/$(. /etc/os-release && echo "$ID") \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "===== Installing Docker Engine ====="
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo "===== Adding user '$USER_NAME' to docker group ====="
sudo usermod -aG docker $USER_NAME
echo "You'll need to log out and log back in for group changes to take effect."

echo "===== Verifying Docker installation ====="
docker --version || echo "Docker command may fail until you re-login."

echo "===== Starting Docker service ====="
sudo systemctl enable docker
sudo systemctl start docker

echo "===== Pulling PostgreSQL Docker image ====="
docker pull postgres:15

echo "===== Running PostgreSQL container ====="
docker run -d \
  --name postgres_container \
  --restart=always \
  -e POSTGRES_USER=jarvis \
  -e POSTGRES_PASSWORD=2650 \
  -e POSTGRES_DB=avengers \
  -p 5432:5432 \
  -v pgdata:/var/lib/postgresql/data \
  postgres:15

sudo apt install -y postgresql-client
echo "=========== Postgres Client installed ========="
echo "===== PostgreSQL is up and running! ====="
echo "Connect through command line using:"
echo "psql -h localhost -U jarvis -d avengers"