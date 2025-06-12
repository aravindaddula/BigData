#!/bin/bash

set -e

echo "🔄 Updating system packages..."
sudo apt update && sudo apt upgrade -y

echo "☕ Installing Java 11..."
sudo apt install -y openjdk-11-jdk

echo "✅ Verifying Java installation..."
java -version
javac -version

echo "🐍 Installing Python 3, pip, and venv..."
sudo apt install -y python3 python3-pip python3-venv

echo "🐙 Installing Git..."
sudo apt install -y git

echo "📝 Installing Visual Studio Code..."
# Install dependencies
sudo apt install -y software-properties-common apt-transport-https wget gpg

# Import Microsoft GPG key
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > packages.microsoft.gpg
sudo install -o root -g root -m 644 packages.microsoft.gpg /usr/share/keyrings/
rm packages.microsoft.gpg

# Add VS Code repository
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/packages.microsoft.gpg] \
https://packages.microsoft.com/repos/vscode stable main" | \
sudo tee /etc/apt/sources.list.d/vscode.list

# Install VS Code
sudo apt update
sudo apt install -y code

echo "🧪 Creating virtual environment named .venv..."
python3 -m venv .venv

echo "📦 Activating virtual environment and installing PySpark 3.5.5..."
source .venv/bin/activate
pip install --upgrade pip
pip install pyspark==3.5.5

echo "✅ Verifying installation inside virtual environment..."
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"

echo "🏁 All done!"
echo "✔ Python, Git, Java 11, VS Code, and PySpark 3.5.5 (in .venv) are installed."
echo "👉 To activate your environment later, run: source .venv/bin/activate"


#download deltalake jars

wegt https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar;
wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar;

echo "DeltaLake jars download done ...."


cp *.jar ~/.venv/lib/python3.12/site-packages/pyspark/jars

echo "jars copied into pyspark default folder..."