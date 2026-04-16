
FROM golang:1.23 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o payment-service .

FROM debian:bookworm-slim
WORKDIR /root/
COPY --from=builder /app/payment-service .
CMD ["./payment-service"]

