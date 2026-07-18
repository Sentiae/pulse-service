# Build stage
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy local replace dependencies (from parent dir)
COPY ops-service/ /ops-service/
COPY work-service/ /work-service/
COPY pulse-service/ /pulse-service/

WORKDIR /pulse-service

# Fetch platform-kit (and all other modules) through the Athens Go proxy.
# GOPROXY has no ",direct" fallback on purpose: Athens is the single source of
# truth, so a proxy miss must FAIL rather than silently fall back to direct git
# (which would re-expose the drift the substrate design forbids). GOPRIVATE and
# GONOPROXY are deliberately left UNSET so ALL modules — public and private
# sentiae alike — route through Athens by construction. GONOSUMDB scopes the
# public checksum-DB skip to the private module only (public modules still
# verify against sumdb); platform-kit itself is verified against the committed
# go.sum hashes, so no global GOSUMDB=off is needed.
ARG GOPROXY
ARG GOFLAGS
ENV GOPROXY=${GOPROXY} \
    GOFLAGS=${GOFLAGS} \
    GONOSUMDB=github.com/sentiae/* \
    GOTOOLCHAIN=auto

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /server ./cmd/server

# Runtime stage
FROM alpine:3.19

WORKDIR /app

COPY --from=builder /server /app/server
COPY --from=builder /pulse-service/configs /app/configs

EXPOSE 8086 50086

LABEL org.opencontainers.image.source="https://github.com/sentiae/pulse-service"


ENTRYPOINT ["/app/server"]
