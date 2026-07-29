Write-Host "Running High-Throughput Load Tester..." -ForegroundColor Cyan

# We run a temporary golang container attached to the same network as the docker-compose stack.
# This allows the container to resolve 'timescaledb' and 'api-gateway' easily without requiring Go on the host machine.
docker run --rm -it `
  --network placement_project_sysd_default `
  -v "${PWD}:/app" `
  -w /app `
  -e DB_HOST=timescaledb `
  -e API_HOST=api-gateway `
  golang:alpine `
  sh -c "go mod tidy && go run cmd/loadtest/main.go"
