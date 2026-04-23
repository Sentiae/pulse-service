# Build stage
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy local replace dependencies (from parent dir)
COPY platform-kit/ /platform-kit/
COPY pulse-service/ /pulse-service/

WORKDIR /pulse-service

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /server ./cmd/server

# Runtime stage
FROM gcr.io/distroless/static-debian12

WORKDIR /app

COPY --from=builder /server /app/server
COPY --from=builder /pulse-service/configs /app/configs

EXPOSE 8086 50086

LABEL org.opencontainers.image.source="https://github.com/sentiae/pulse-service"

USER nonroot:nonroot

ENTRYPOINT ["/app/server"]
