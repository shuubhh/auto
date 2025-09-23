# ---- Build stage ----
FROM golang:1.23 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o server main.go
 
# ---- Runtime stage ----
FROM debian:bookworm-slim
 
# Install CA certificates so TLS works
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
 
WORKDIR /app
COPY --from=builder /app/server .
COPY static ./static
EXPOSE 8080
CMD ["./server"]