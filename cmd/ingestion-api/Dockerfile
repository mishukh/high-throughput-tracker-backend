FROM golang:alpine AS builder

WORKDIR /app

# Copy mod files and download dependencies
COPY . .
RUN go mod tidy
RUN go build -o /ingestion-api ./cmd/ingestion-api

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /ingestion-api .
EXPOSE 8080
CMD ["./ingestion-api"]
