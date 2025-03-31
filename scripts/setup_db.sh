#!/bin/bash
# scripts/setup_db.sh
# PostgreSQL + Prefect Database Migration Script

set -eo pipefail

# Configuration
DB_NAME="prefect_prod"
DB_USER="prefect_user"
DB_PASSWORD="StrongPassword123!"  # Change this!
PYTHON_DRIVER="asyncpg"
PRECTECT_VERSION="2.13.0"

# Detect package manager
if command -v apt &> /dev/null; then
    PKG_MANAGER="apt"
elif command -v dnf &> /dev/null; then
    PKG_MANAGER="dnf"
elif command -v yum &> /dev/null; then
    PKG_MANAGER="yum"
else
    echo "Unsupported package manager"
    exit 1
fi

# Install PostgreSQL
echo "Installing PostgreSQL..."
case $PKG_MANAGER in
    "apt")
        sudo apt update
        sudo apt install -y postgresql postgresql-contrib libpq-dev
        ;;
    "dnf"|"yum")
        sudo $PKG_MANAGER install -y postgresql-server postgresql-contrib
        sudo postgresql-setup --initdb
        ;;
esac

# Start PostgreSQL
echo "Starting PostgreSQL service..."
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create Database and User
echo "Creating database and user..."
sudo -u postgres psql <<EOF
CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASSWORD}';
CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};
ALTER DATABASE ${DB_NAME} SET TIMEZONE TO 'UTC';
GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};
ALTER SYSTEM SET max_connections = 200;
EOF

# Configure Authentication
PG_HBA=$(sudo -u postgres psql -t -P format=unaligned -c "SHOW hba_file")
echo "Configuring pg_hba.conf at ${PG_HBA}..."
sudo sed -i "/# TYPE  DATABASE        USER            ADDRESS                 METHOD/a host    ${DB_NAME}    ${DB_USER}    127.0.0.1/32            md5" "$PG_HBA"

# Reload PostgreSQL
echo "Reloading PostgreSQL..."
sudo systemctl restart postgresql

# Install Python Requirements
echo "Installing Python dependencies..."
python -m pip install --upgrade pip
python -m pip install "prefect>=${PRECTECT_VERSION}" ${PYTHON_DRIVER}

# Configure Prefect
echo "Setting up Prefect configuration..."
prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql+${PYTHON_DRIVER}://${DB_USER}:${DB_PASSWORD}@localhost/${DB_NAME}"

# Migrate Database
echo "Migrating Prefect database..."
prefect server database reset -y

# Create Work Pool
echo "Creating work pool..."
prefect work-pool create --type process lightning-pool

echo "PostgreSQL migration complete!"
echo "Start worker with:"
echo "prefect worker start --pool lightning-pool --limit 4"