# TODO: use just?
export DB_CONNECTION_STRING="postgresql://username:password@localhost:5433/ibis"

docker run --rm --name learn-ibis-db \
  -e POSTGRES_DB=ibis \
  -e POSTGRES_USER=username \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -p 5433:5432 \
  -d postgres:16-alpine
