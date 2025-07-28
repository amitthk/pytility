#!/bin/bash

set -e

# Configuration
POSTGRES_DIR="/opt/apps/postgres"
AIRFLOW_DIR="/opt/apps/airflow"
PYTHON_DIR="/opt/apps/python"
VENV_DIR="/opt/apps/venv3"
PG_VERSION="16"
PYTHON_VERSION="3.11.9"
AIRFLOW_VERSION="2.8.1"
POSTGRES_USER="airflow"
POSTGRES_PASSWORD="airflow_secure_password"
POSTGRES_DB="airflow"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Detect OS
detect_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
        OS_VERSION=$VERSION_ID
        if [[ "$OS" == "ubuntu" ]]; then
            VERSION_CODENAME=${VERSION_CODENAME:-$(lsb_release -cs 2>/dev/null || echo "")}
        elif [[ "$OS" == "debian" ]]; then
            VERSION_CODENAME=${VERSION_CODENAME:-$(lsb_release -cs 2>/dev/null || echo "bookworm")}
        fi
    else
        error "Cannot detect operating system."
    fi
    log "Detected OS: $OS ${VERSION_CODENAME:+($VERSION_CODENAME)} ${OS_VERSION:+v$OS_VERSION}"
}

# Create directories and users
setup_directories() {
    log "Creating directories and users..."
    
    # Create directories
    sudo mkdir -p "$POSTGRES_DIR" "$AIRFLOW_DIR" "$PYTHON_DIR" "$VENV_DIR"
    sudo mkdir -p "$AIRFLOW_DIR/dags" "$AIRFLOW_DIR/logs" "$AIRFLOW_DIR/plugins"
    
    # Create postgres user if it doesn't exist
    if ! id "postgres" &>/dev/null; then
        sudo useradd -r -s /bin/bash -d "$POSTGRES_DIR" postgres
        log "Created postgres user"
    else
        log "postgres user already exists"
    fi
    
    # Create airflow user if it doesn't exist
    if ! id "airflow" &>/dev/null; then
        sudo useradd -r -s /bin/bash -d "$AIRFLOW_DIR" -G postgres airflow
        log "Created airflow user"
    else
        log "airflow user already exists"
        # Add airflow to postgres group if not already
        sudo usermod -a -G postgres airflow
    fi
    
    # Set ownership
    sudo chown -R postgres:postgres "$POSTGRES_DIR"
    sudo chown -R airflow:airflow "$AIRFLOW_DIR"
    sudo chown -R root:root "$PYTHON_DIR" "$VENV_DIR"
}

# Install system dependencies
install_dependencies() {
    log "Installing system dependencies..."
    
    if [[ "$OS" == "fedora" || "$OS" == "rhel" || "$OS" == "centos" || "$OS" == "ol" || "$OS" == "almalinux" || "$OS" == "rocky" ]]; then
        sudo dnf update -y
        sudo dnf install -y \
            gcc gcc-c++ make \
            openssl-devel libffi-devel \
            readline-devel sqlite-devel \
            bzip2-devel xz-devel \
            zlib-devel ncurses-devel \
            wget curl gnupg2 tar \
            python3-devel \
            systemd-devel \
            krb5-devel \
            postgresql-devel \
            redhat-rpm-config
            
    elif [[ "$OS" == "debian" || "$OS" == "ubuntu" ]]; then
        sudo apt-get update
        sudo apt-get install -y \
            build-essential \
            libssl-dev libffi-dev \
            libreadline-dev libsqlite3-dev \
            libbz2-dev liblzma-dev \
            zlib1g-dev libncurses5-dev \
            libsystemd-dev \
            wget curl gnupg2 tar \
            python3-dev \
            lsb-release \
            libkrb5-dev \
            libpq-dev \
            pkg-config
    else
        error "Unsupported OS: $OS"
    fi
    
    log "System dependencies installed successfully"
}

# Install PostgreSQL
install_postgresql() {
    log "Installing PostgreSQL $PG_VERSION..."
    
    if [[ "$OS" == "fedora" || "$OS" == "rhel" || "$OS" == "centos" || "$OS" == "ol" || "$OS" == "almalinux" || "$OS" == "rocky" ]]; then
        # Install PostgreSQL repo
        if [[ "$OS" == "fedora" ]]; then
            sudo dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/F-$(rpm -E %{fedora})-x86_64/pgdg-fedora-repo-latest.noarch.rpm
        else
            # For RHEL-based distributions
            RHEL_VERSION=$(echo $OS_VERSION | cut -d. -f1)
            sudo dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-${RHEL_VERSION}-x86_64/pgdg-redhat-repo-latest.noarch.rpm
        fi
        
        sudo dnf -qy module disable postgresql 2>/dev/null || true
        sudo dnf install -y postgresql${PG_VERSION}-server postgresql${PG_VERSION} postgresql${PG_VERSION}-devel
        
        PG_BIN="/usr/pgsql-${PG_VERSION}/bin"
        SERVICE_NAME="postgresql-${PG_VERSION}"
        
    elif [[ "$OS" == "debian" || "$OS" == "ubuntu" ]]; then
        # Add PostgreSQL APT repo
        curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /usr/share/keyrings/postgresql.gpg
        echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
        
        sudo apt-get update
        sudo apt-get install -y postgresql-${PG_VERSION} postgresql-client-${PG_VERSION} postgresql-server-dev-${PG_VERSION}
        
        PG_BIN="/usr/lib/postgresql/${PG_VERSION}/bin"
        SERVICE_NAME="postgresql"
    fi
    
    # Stop any existing PostgreSQL services
    sudo systemctl stop postgresql* 2>/dev/null || true
    
    # Initialize PostgreSQL in custom directory
    log "Initializing PostgreSQL database in $POSTGRES_DIR..."
    sudo -u postgres ${PG_BIN}/initdb -D "$POSTGRES_DIR" --encoding=UTF8 --locale=C
    
    # Configure PostgreSQL
    configure_postgresql
    
    # Setup systemd service for custom data directory
    setup_postgresql_service
}

# Configure PostgreSQL
configure_postgresql() {
    log "Configuring PostgreSQL..."
    
    # Update postgresql.conf
    sudo -u postgres sed -i "s/#listen_addresses = 'localhost'/listen_addresses = 'localhost'/" "$POSTGRES_DIR/postgresql.conf"
    sudo -u postgres sed -i "s/#port = 5432/port = 5432/" "$POSTGRES_DIR/postgresql.conf"
    
    # Update pg_hba.conf for local connections
    sudo -u postgres cp "$POSTGRES_DIR/pg_hba.conf" "$POSTGRES_DIR/pg_hba.conf.bak"
    sudo -u postgres tee "$POSTGRES_DIR/pg_hba.conf" > /dev/null <<EOF
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             postgres                                peer
local   all             all                                     md5
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5
EOF
}

# Setup PostgreSQL systemd service
setup_postgresql_service() {
    log "Configuring PostgreSQL service..."
    
    if [[ "$OS" == "fedora" || "$OS" == "rhel" || "$OS" == "centos" || "$OS" == "ol" || "$OS" == "almalinux" || "$OS" == "rocky" ]]; then
        sudo cp /usr/lib/systemd/system/postgresql-${PG_VERSION}.service /etc/systemd/system/
        sudo sed -i "s|^Environment=PGDATA=.*|Environment=PGDATA=$POSTGRES_DIR|" /etc/systemd/system/postgresql-${PG_VERSION}.service
        SERVICE_NAME="postgresql-${PG_VERSION}"
        
    elif [[ "$OS" == "debian" || "$OS" == "ubuntu" ]]; then
        # Stop and disable default service
        sudo systemctl stop postgresql 2>/dev/null || true
        sudo systemctl disable postgresql 2>/dev/null || true
        
        # Create custom service
        sudo tee /etc/systemd/system/postgresql-custom.service > /dev/null <<EOF
[Unit]
Description=PostgreSQL database server
Documentation=man:postgres(1)
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
User=postgres
ExecStart=${PG_BIN}/postgres -D ${POSTGRES_DIR}
ExecReload=/bin/kill -HUP \$MAINPID
KillMode=mixed
KillSignal=SIGINT
TimeoutSec=0

[Install]
WantedBy=multi-user.target
EOF
        SERVICE_NAME="postgresql-custom"
    fi
    
    sudo systemctl daemon-reload
    sudo systemctl enable $SERVICE_NAME
    sudo systemctl start $SERVICE_NAME
    
    # Wait for PostgreSQL to start
    log "Waiting for PostgreSQL to start..."
    sleep 10
    
    # Verify PostgreSQL is running
    if ! sudo systemctl is-active --quiet $SERVICE_NAME; then
        error "PostgreSQL failed to start. Check logs with: sudo journalctl -u $SERVICE_NAME"
    fi
    
    log "PostgreSQL is running successfully"
}

# Setup PostgreSQL database and user
setup_database() {
    log "Setting up Airflow database and user..."
    
    # Wait a bit more to ensure PostgreSQL is fully ready
    sleep 5
    
    # Create database and user
    sudo -u postgres ${PG_BIN}/psql -c "DROP DATABASE IF EXISTS $POSTGRES_DB;"
    sudo -u postgres ${PG_BIN}/psql -c "DROP ROLE IF EXISTS $POSTGRES_USER;"
    sudo -u postgres ${PG_BIN}/psql -c "CREATE ROLE $POSTGRES_USER WITH LOGIN PASSWORD '$POSTGRES_PASSWORD';"
    sudo -u postgres ${PG_BIN}/psql -c "CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;"
    sudo -u postgres ${PG_BIN}/psql -d $POSTGRES_DB -c "CREATE SCHEMA IF NOT EXISTS airflow AUTHORIZATION $POSTGRES_USER;"
    sudo -u postgres ${PG_BIN}/psql -c "ALTER ROLE $POSTGRES_USER SET search_path TO airflow,public;"
    
    log "Database and user created successfully"
}

# Install Python from source
install_python() {
    log "Installing Python $PYTHON_VERSION from source..."
    
    # Check if Python is already installed
    if [ -f "$PYTHON_DIR/bin/python3.11" ]; then
        log "Python $PYTHON_VERSION already installed at $PYTHON_DIR"
        return
    fi
    
    cd /tmp
    rm -rf Python-$PYTHON_VERSION*
    
    wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
    tar -xzf Python-$PYTHON_VERSION.tgz
    cd Python-$PYTHON_VERSION
    
    ./configure \
        --prefix=$PYTHON_DIR \
        --enable-optimizations \
        --with-ensurepip=install \
        --enable-shared \
        --with-system-ffi \
        --with-computed-gotos \
        --enable-loadable-sqlite-extensions
    
    make -j$(nproc)
    sudo make install
    
    # Update library path
    echo "$PYTHON_DIR/lib" | sudo tee /etc/ld.so.conf.d/python-custom.conf
    sudo ldconfig
    
    # Create symlinks for easier access
    sudo ln -sf $PYTHON_DIR/bin/python3.11 /usr/local/bin/python3.11
    sudo ln -sf $PYTHON_DIR/bin/pip3.11 /usr/local/bin/pip3.11
    
    cd /tmp
    rm -rf Python-$PYTHON_VERSION*
    
    log "Python $PYTHON_VERSION installed successfully"
}

# Setup Python virtual environment
setup_virtualenv() {
    log "Creating Python virtual environment..."
    
    # Remove existing venv if it exists
    if [ -d "$VENV_DIR" ]; then
        sudo rm -rf "$VENV_DIR"
    fi
    
    # Create virtual environment using the custom Python
    sudo $PYTHON_DIR/bin/python3.11 -m venv $VENV_DIR
    sudo chown -R airflow:airflow $VENV_DIR
    
    # Upgrade pip and install dependencies
    log "Installing Python packages..."
    sudo -u airflow bash -c "
        source $VENV_DIR/bin/activate
        pip install --upgrade pip setuptools wheel
        pip install psycopg2-binary asyncpg
        pip install 'apache-airflow==$AIRFLOW_VERSION' --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-3.11.txt'
    "
    
    log "Virtual environment created and packages installed"
}

# Generate keys for Airflow
generate_keys() {
    log "Generating Airflow keys..."
    
    FERNET_KEY=$(sudo -u airflow bash -c "source $VENV_DIR/bin/activate && python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'")
    SECRET_KEY=$(sudo -u airflow bash -c "source $VENV_DIR/bin/activate && python -c 'import secrets; print(secrets.token_urlsafe(32))'")
    
    log "Airflow keys generated successfully"
}

# Configure Airflow
configure_airflow() {
    log "Configuring Airflow..."
    
    # Create Airflow configuration directory
    sudo mkdir -p /etc/sysconfig
    
    # Create Airflow environment file
    sudo tee /etc/sysconfig/airflow > /dev/null <<EOF
# Airflow Configuration
AIRFLOW_HOME=$AIRFLOW_DIR
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:5432/$POSTGRES_DB
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=$FERNET_KEY
AIRFLOW__API__SECRET_KEY=$SECRET_KEY
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW_DIR/dags
AIRFLOW__LOGGING__BASE_LOG_FOLDER=$AIRFLOW_DIR/logs
AIRFLOW__CORE__PLUGINS_FOLDER=$AIRFLOW_DIR/plugins
AIRFLOW__API__EXPOSE_CONFIG=True
AIRFLOW__CORE__AUTH_MANAGER=airflow.auth.managers.simple.SimpleAuthManager
AIRFLOW__SIMPLE_AUTH_MANAGER__USERS=admin:admin
PATH=$VENV_DIR/bin:\$PATH
EOF

    # Set proper ownership
    sudo chown airflow:airflow /etc/sysconfig/airflow
    
    # Initialize Airflow database (using the new command with direct environment variable)
    log "Initializing Airflow database..."
    sudo -u airflow bash -c "
        export AIRFLOW_HOME=$AIRFLOW_DIR
        export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:5432/$POSTGRES_DB
        export AIRFLOW__CORE__EXECUTOR=LocalExecutor
        export AIRFLOW__CORE__FERNET_KEY=$FERNET_KEY
        export AIRFLOW__API__SECRET_KEY=$SECRET_KEY
        export AIRFLOW__CORE__LOAD_EXAMPLES=False
        export AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW_DIR/dags
        export AIRFLOW__LOGGING__BASE_LOG_FOLDER=$AIRFLOW_DIR/logs
        export AIRFLOW__CORE__PLUGINS_FOLDER=$AIRFLOW_DIR/plugins
        export PATH=$VENV_DIR/bin:\$PATH
        source $VENV_DIR/bin/activate
        airflow db migrate
    "
    
    # Configure SimpleAuthManager users
    log "Configuring Airflow SimpleAuthManager users..."
    
    # Create airflow.cfg if it doesn't exist and configure SimpleAuthManager
    sudo -u airflow bash -c "
        export AIRFLOW_HOME=$AIRFLOW_DIR
        export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:5432/$POSTGRES_DB
        export AIRFLOW__CORE__EXECUTOR=LocalExecutor
        export AIRFLOW__CORE__FERNET_KEY=$FERNET_KEY
        export AIRFLOW__API__SECRET_KEY=$SECRET_KEY
        export AIRFLOW__CORE__LOAD_EXAMPLES=False
        export AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW_DIR/dags
        export AIRFLOW__LOGGING__BASE_LOG_FOLDER=$AIRFLOW_DIR/logs
        export AIRFLOW__CORE__PLUGINS_FOLDER=$AIRFLOW_DIR/plugins
        export AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
        export AIRFLOW__SIMPLE_AUTH_MANAGER__USERS=admin:admin
        export PATH=$VENV_DIR/bin:\$PATH
        source $VENV_DIR/bin/activate
        airflow config get-value core auth_manager || true
    "
    
    log "SimpleAuthManager configured with default admin:admin user"
    
    log "Airflow configuration completed"
}

# Create systemd services
create_services() {
    log "Creating systemd services..."
    
    # Airflow Webserver service (now API server)
    sudo tee /etc/systemd/system/airflow-apiserver.service > /dev/null <<EOF
[Unit]
Description=Airflow API Server Daemon
After=network.target $SERVICE_NAME.service
Requires=$SERVICE_NAME.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User=airflow
Group=airflow
Type=simple
ExecStart=$VENV_DIR/bin/airflow api-server --host 0.0.0.0 --port 8080
Restart=on-failure
RestartSec=10s
WorkingDirectory=$AIRFLOW_DIR
KillMode=mixed
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
EOF

    # Airflow Scheduler service
    sudo tee /etc/systemd/system/airflow-scheduler.service > /dev/null <<EOF
[Unit]
Description=Airflow Scheduler Daemon
After=network.target $SERVICE_NAME.service
Requires=$SERVICE_NAME.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User=airflow
Group=airflow
Type=simple
ExecStart=$VENV_DIR/bin/airflow scheduler
Restart=on-failure
RestartSec=10s
WorkingDirectory=$AIRFLOW_DIR
KillMode=mixed
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
EOF

    # Airflow DAG Processor service
    sudo tee /etc/systemd/system/airflow-dag-processor.service > /dev/null <<EOF
[Unit]
Description=Airflow DAG Processor Daemon
After=network.target $SERVICE_NAME.service
Requires=$SERVICE_NAME.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User=airflow
Group=airflow
Type=simple
ExecStart=$VENV_DIR/bin/airflow dag-processor
Restart=on-failure
RestartSec=10s
WorkingDirectory=$AIRFLOW_DIR
KillMode=mixed
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload

    sudo systemctl daemon-reload
    sudo systemctl enable airflow-apiserver airflow-scheduler airflow-dag-processor
    
    log "Starting Airflow services..."
    sudo systemctl start airflow-apiserver
    sudo systemctl start airflow-scheduler
    sudo systemctl start airflow-dag-processor
    # Wait a moment for services to start
    sleep 10
    
    log "Systemd services created and started"
}

# Verify installation
verify_installation() {
    log "Verifying installation..."
    
    # Check if all services are running
    local services=("$SERVICE_NAME" "airflow-apiserver" "airflow-scheduler", "airflow-dag-processor")
    local failed_services=()
    
    for service in "${services[@]}"; do
        if ! sudo systemctl is-active --quiet $service; then
            failed_services+=($service)
        fi
    done
    
    if [ ${#failed_services[@]} -eq 0 ]; then
        log "All services are running successfully!"
    else
        warn "Some services failed to start: ${failed_services[*]}"
        for service in "${failed_services[@]}"; do
            log "Checking $service status:"
            sudo systemctl status $service --no-pager -l || true
        done
    fi
    
    # Test database connection
    log "Testing database connection..."
    sudo -u airflow bash -c "
        export AIRFLOW_HOME=$AIRFLOW_DIR
        export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:5432/$POSTGRES_DB
        export PATH=$VENV_DIR/bin:\$PATH
        source $VENV_DIR/bin/activate
        python -c 'from airflow import settings; print(\"Database connection: OK\")'
    " || warn "Database connection test failed"
}

# Cleanup function
cleanup() {
    log "Cleaning up temporary files..."
    rm -rf /tmp/Python-$PYTHON_VERSION*
}

# Main execution
main() {
    log "Starting Airflow installation..."
    
    # Check if running as root or with sudo
    if [[ $EUID -eq 0 ]]; then
        error "Please run this script as a regular user with sudo privileges, not as root"
    fi
    
    if ! sudo -n true 2>/dev/null; then
        error "This script requires sudo privileges"
    fi
    
    # Trap to ensure cleanup on exit
    trap cleanup EXIT
    
    detect_os
    setup_directories
    install_dependencies
    install_postgresql
    setup_database
    install_python
    setup_virtualenv
    generate_keys
    configure_airflow
    create_services
    verify_installation
    
    log "Installation completed successfully!"
    log ""
    log " Airflow API server is running on http://localhost:8080"
    log " Login credentials: admin with auto-generated password"
    log " The password can be found in the apiserver logs:"
    log "    sudo journalctl -u airflow-apiserver -f | grep 'Login with username: admin, password:'"
    log "    or check: sudo journalctl -u airflow-apiserver | grep password"
    log ""
    log " Service status:"
    sudo systemctl status $SERVICE_NAME --no-pager -l | head -3
    sudo systemctl status airflow-apiserver --no-pager -l | head -3
    sudo systemctl status airflow-scheduler --no-pager -l | head -3
    log ""
    log " To check logs:"
    log "  sudo journalctl -u $SERVICE_NAME -f"
    log "  sudo journalctl -u airflow-apiserver -f"
    log "  sudo journalctl -u airflow-scheduler -f"
    log ""
    log " Configuration files:"
    log "  Airflow home: $AIRFLOW_DIR"
    log "  Python: $PYTHON_DIR/bin/python3.11"
    log "  Virtual env: $VENV_DIR"
    log "  PostgreSQL data: $POSTGRES_DIR"
}

# Execute main function
main "$@"