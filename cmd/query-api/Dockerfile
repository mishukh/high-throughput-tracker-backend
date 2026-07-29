FROM golang:alpine AS builder

WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -o /query-api ./cmd/query-api

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /query-api .
EXPOSE 8081
CMD ["./query-api"]
