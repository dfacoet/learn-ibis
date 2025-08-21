# Database management commands

# Start the PostgreSQL database container
db-start:
    #!/usr/bin/env bash
    set -a
    source .env
    set +a
    docker run --rm --name learn-ibis-db \
      -e POSTGRES_DB=$DB_NAME \
      -e POSTGRES_USER=$DB_USER \
      -e POSTGRES_PASSWORD=$DB_PASSWORD \
      -e POSTGRES_HOST_AUTH_METHOD=trust \
      -p $DB_PORT:5432 \
      -d postgres:16-alpine

# Stop the database container
db-stop:
    docker stop learn-ibis-db || true

# Restart the database container
db-restart: db-stop db-start

# Show database container logs
db-logs:
    docker logs -f learn-ibis-db

# Show database container status
db-status:
    docker ps -a --filter name=learn-ibis-db

# Connect to the database using psql
db-connect:
    #!/usr/bin/env bash
    set -a
    source .env
    set +a
    docker exec -it learn-ibis-db psql -U $DB_USER -d $DB_NAME



