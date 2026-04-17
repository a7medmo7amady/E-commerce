FROM golang:1.23 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o notification-service .

FROM debian:bookworm-slim
WORKDIR /root/
COPY --from=builder /app/notification-service .
CMD ["./notification-service"]
