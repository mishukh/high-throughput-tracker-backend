FROM golang:alpine AS builder

WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -o /stream-processor ./cmd/stream-processor

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /stream-processor .
CMD ["./stream-processor"]
