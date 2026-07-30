FROM golang:alpine AS builder

WORKDIR /app

# Copy mod files and download dependencies
COPY . .
RUN go mod tidy
RUN go build -o /loadtest ./cmd/loadtest

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /loadtest .
CMD ["./loadtest"]
