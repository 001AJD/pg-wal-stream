# Build stage
FROM golang:1.25.0-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o pg-wal-stream main.go

# Final stage
FROM alpine:latest

WORKDIR /app

# Add CA certificates
RUN apk --no-cache add ca-certificates

# Copy the binary from the builder stage
COPY --from=builder /app/pg-wal-stream .

# Create destination directory for localfile sink if needed
RUN mkdir -p /data/destination

# Set the entrypoint
ENTRYPOINT ["./pg-wal-stream"]
