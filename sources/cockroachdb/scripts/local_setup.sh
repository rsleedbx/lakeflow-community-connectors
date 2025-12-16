#!/bin/bash
################################################################################
# CockroachDB Local Testing Setup Script
#
# Description:
#   Automates the setup of a single-node CockroachDB cluster for local testing
#   with YCSB workload and sample tables.
#
# Usage:
#   ./local_setup.sh [command]
#
# Commands:
#   start       - Start CockroachDB cluster and load test data
#   stop        - Stop the CockroachDB cluster
#   status      - Check cluster status
#   workload    - Start YCSB workload generator
#   changefeed  - Test changefeed on usertable
#   reset       - Stop cluster and remove all data
#   help        - Show this help message
#
# Requirements:
#   - cockroach binary in PATH (install via: brew install cockroachdb/tap/cockroach)
#   - Or use Docker mode by setting: DOCKER_MODE=true
#
# Examples:
#   ./local_setup.sh start
#   ./local_setup.sh workload
#   DOCKER_MODE=true ./local_setup.sh start
#
################################################################################

set -e

# Configuration
COCKROACH_HOST="localhost"
COCKROACH_PORT="26257"
HTTP_PORT="8080"
DATA_DIR="$HOME/cockroach-data"
DOCKER_CONTAINER="cockroach-local"
DOCKER_VOLUME="cockroach-data"
DATABASE="ycsb"

# Check if running in Docker mode
DOCKER_MODE="${DOCKER_MODE:-false}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
check_prerequisites() {
    if [ "$DOCKER_MODE" = "true" ]; then
        if ! command_exists docker; then
            log_error "Docker is not installed. Install from: https://www.docker.com/get-started"
        fi
        log_info "Using Docker mode"
    else
        if ! command_exists cockroach; then
            log_error "cockroach binary not found. Install from: https://www.cockroachlabs.com/docs/stable/install-cockroachdb"
        fi
        log_info "Using local binary mode"
    fi
}

# Start CockroachDB cluster
start_cluster() {
    log_info "Starting CockroachDB single-node cluster..."
    
    if [ "$DOCKER_MODE" = "true" ]; then
        # Docker mode
        if docker ps -a --format '{{.Names}}' | grep -q "^${DOCKER_CONTAINER}$"; then
            log_warning "Container ${DOCKER_CONTAINER} already exists. Starting..."
            docker start ${DOCKER_CONTAINER}
        else
            log_info "Creating new Docker container..."
            docker run -d \
                --name=${DOCKER_CONTAINER} \
                -p ${COCKROACH_PORT}:26257 \
                -p ${HTTP_PORT}:8080 \
                -v ${DOCKER_VOLUME}:/cockroach/cockroach-data \
                cockroachdb/cockroach:latest \
                start-single-node --insecure
        fi
        
        # Wait for cluster to be ready
        log_info "Waiting for cluster to be ready..."
        sleep 5
        
        # Check status
        docker exec ${DOCKER_CONTAINER} ./cockroach node status --insecure
    else
        # Local binary mode
        if pgrep -f "cockroach start-single-node" > /dev/null; then
            log_warning "CockroachDB is already running"
            return
        fi
        
        mkdir -p ${DATA_DIR}
        
        cockroach start-single-node \
            --insecure \
            --store=${DATA_DIR} \
            --listen-addr=${COCKROACH_HOST}:${COCKROACH_PORT} \
            --http-addr=${COCKROACH_HOST}:${HTTP_PORT} \
            --background
        
        # Wait for cluster to be ready
        log_info "Waiting for cluster to be ready..."
        sleep 3
        
        # Check status
        cockroach node status --insecure --host=${COCKROACH_HOST}:${COCKROACH_PORT}
    fi
    
    log_success "CockroachDB cluster started successfully"
    log_info "Admin UI: http://${COCKROACH_HOST}:${HTTP_PORT}"
    log_info "SQL endpoint: ${COCKROACH_HOST}:${COCKROACH_PORT}"
}

# Initialize database and load test data
init_database() {
    log_info "Initializing database and loading test data..."
    
    # Create database
    log_info "Creating ${DATABASE} database..."
    exec_sql "CREATE DATABASE IF NOT EXISTS ${DATABASE};"
    
    # Enable rangefeeds (required for changefeeds)
    log_info "Enabling rangefeeds for CDC..."
    exec_sql "SET CLUSTER SETTING kv.rangefeed.enabled = true;"
    
    # Initialize YCSB workload (use --drop to recreate if exists)
    log_info "Loading YCSB workload (10,000 records)..."
    if [ "$DOCKER_MODE" = "true" ]; then
        docker exec ${DOCKER_CONTAINER} ./cockroach workload init ycsb \
            "postgresql://root@localhost:26257/${DATABASE}?sslmode=disable" \
            --insert-count=10000 \
            --drop
    else
        cockroach workload init ycsb \
            "postgresql://root@${COCKROACH_HOST}:${COCKROACH_PORT}/${DATABASE}?sslmode=disable" \
            --insert-count=10000 \
            --drop
    fi
    
    # Create additional test tables
    log_info "Creating additional test tables..."
    exec_sql "
        USE ${DATABASE};
        
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(50),
            user_id INT,
            data JSONB,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        
        CREATE TABLE IF NOT EXISTS temp_records (
            id SERIAL PRIMARY KEY,
            value VARCHAR(100),
            created_at TIMESTAMPTZ DEFAULT now()
        );
        
        -- Insert sample data
        INSERT INTO events (event_type, user_id, data)
        SELECT 'login', generate_series, '{\"ip\": \"192.168.1.1\"}'::JSONB
        FROM generate_series(1, 100)
        ON CONFLICT DO NOTHING;
        
        INSERT INTO temp_records (value)
        SELECT 'record_' || generate_series
        FROM generate_series(1, 50)
        ON CONFLICT DO NOTHING;
        
        -- Grant changefeed privileges
        GRANT SELECT, CHANGEFEED ON TABLE ycsb.usertable TO root;
        GRANT SELECT, CHANGEFEED ON TABLE events TO root;
        GRANT SELECT, CHANGEFEED ON TABLE temp_records TO root;
    "
    
    # Show statistics
    log_success "Database initialized successfully"
    log_info "Tables and row counts:"
    exec_sql "
        SELECT 
            'usertable' as table_name,
            COUNT(*) as row_count
        FROM ycsb.usertable
        UNION ALL
        SELECT 
            'events' as table_name,
            COUNT(*) as row_count
        FROM ycsb.events
        UNION ALL
        SELECT 
            'temp_records' as table_name,
            COUNT(*) as row_count
        FROM ycsb.temp_records
        ORDER BY table_name;
    "
}

# Execute SQL command
exec_sql() {
    if [ "$DOCKER_MODE" = "true" ]; then
        docker exec -i ${DOCKER_CONTAINER} ./cockroach sql --insecure -e "$1"
    else
        cockroach sql --insecure --host=${COCKROACH_HOST}:${COCKROACH_PORT} -e "$1"
    fi
}

# Stop cluster
stop_cluster() {
    log_info "Stopping CockroachDB cluster..."
    
    if [ "$DOCKER_MODE" = "true" ]; then
        docker stop ${DOCKER_CONTAINER} 2>/dev/null || true
        log_success "CockroachDB container stopped"
    else
        cockroach quit --insecure --host=${COCKROACH_HOST}:${COCKROACH_PORT} 2>/dev/null || true
        log_success "CockroachDB process stopped"
    fi
}

# Check cluster status
check_status() {
    log_info "Checking cluster status..."
    
    if [ "$DOCKER_MODE" = "true" ]; then
        if docker ps --format '{{.Names}}' | grep -q "^${DOCKER_CONTAINER}$"; then
            log_success "CockroachDB container is running"
            docker exec ${DOCKER_CONTAINER} ./cockroach node status --insecure
        else
            log_warning "CockroachDB container is not running"
            return 1
        fi
    else
        if pgrep -f "cockroach start-single-node" > /dev/null; then
            log_success "CockroachDB is running"
            cockroach node status --insecure --host=${COCKROACH_HOST}:${COCKROACH_PORT}
        else
            log_warning "CockroachDB is not running"
            return 1
        fi
    fi
}

# Run YCSB workload
run_workload() {
    log_info "Starting YCSB workload generator..."
    log_info "This will generate continuous INSERT and UPDATE operations for 10 minutes"
    
    if [ "$DOCKER_MODE" = "true" ]; then
        docker exec -d ${DOCKER_CONTAINER} ./cockroach workload run ycsb \
            "postgresql://root@localhost:26257/${DATABASE}?sslmode=disable" \
            --duration=10m \
            --concurrency=10 \
            --max-rate=100
    else
        nohup cockroach workload run ycsb \
            "postgresql://root@${COCKROACH_HOST}:${COCKROACH_PORT}/${DATABASE}?sslmode=disable" \
            --duration=10m \
            --concurrency=10 \
            --max-rate=100 \
            > /tmp/ycsb-workload.log 2>&1 &
        
        log_info "Workload log: /tmp/ycsb-workload.log"
    fi
    
    log_success "YCSB workload started"
    log_info "Monitor with: watch 'cockroach sql --insecure -e \"SELECT COUNT(*) FROM ycsb.usertable;\"'"
}

# Test changefeed
test_changefeed() {
    log_info "Testing changefeed on ycsb.usertable..."
    log_info "Press Ctrl+C to stop"
    log_info ""
    
    if [ "$DOCKER_MODE" = "true" ]; then
        docker exec -it ${DOCKER_CONTAINER} ./cockroach sql --insecure -e \
            "EXPERIMENTAL CHANGEFEED FOR ycsb.usertable WITH updated, resolved='5s';"
    else
        cockroach sql --insecure --host=${COCKROACH_HOST}:${COCKROACH_PORT} -e \
            "EXPERIMENTAL CHANGEFEED FOR ycsb.usertable WITH updated, resolved='5s';"
    fi
}

# Reset cluster (remove all data)
reset_cluster() {
    log_warning "This will stop the cluster and remove all data!"
    read -p "Are you sure? (yes/no): " confirm
    
    if [ "$confirm" != "yes" ]; then
        log_info "Reset cancelled"
        return
    fi
    
    stop_cluster
    
    if [ "$DOCKER_MODE" = "true" ]; then
        log_info "Removing Docker container and volume..."
        docker rm ${DOCKER_CONTAINER} 2>/dev/null || true
        docker volume rm ${DOCKER_VOLUME} 2>/dev/null || true
    else
        log_info "Removing data directory..."
        rm -rf ${DATA_DIR}
    fi
    
    log_success "Cluster reset complete"
}

# Show help
show_help() {
    cat << EOF
CockroachDB Local Testing Setup Script

Usage:
    $0 [command]

Commands:
    start       Start CockroachDB cluster and load test data
    stop        Stop the CockroachDB cluster
    status      Check cluster status
    workload    Start YCSB workload generator
    changefeed  Test changefeed on usertable
    reset       Stop cluster and remove all data
    help        Show this help message

Environment Variables:
    DOCKER_MODE   Set to 'true' to use Docker instead of local binary

Examples:
    $0 start
    $0 workload
    DOCKER_MODE=true $0 start
    
After starting the cluster:
    - Admin UI: http://localhost:8080
    - SQL endpoint: localhost:26257
    - Database: ycsb
    - Tables: usertable, events, temp_records

EOF
}

# Main script
main() {
    command="${1:-help}"
    
    case "$command" in
        start)
            check_prerequisites
            start_cluster
            init_database
            log_info ""
            log_success "Setup complete! CockroachDB is ready for testing."
            log_info "Next steps:"
            log_info "  1. Generate load: $0 workload"
            log_info "  2. Watch changes: $0 changefeed"
            log_info "  3. Connect: cockroach sql --insecure --host=localhost:26257"
            ;;
        stop)
            stop_cluster
            ;;
        status)
            check_status
            ;;
        workload)
            run_workload
            ;;
        changefeed)
            test_changefeed
            ;;
        reset)
            reset_cluster
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Unknown command: $command"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@"

