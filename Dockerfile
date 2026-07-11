# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.5
ARG ALPINE_VERSION=3.24

# --- Stage 1: Builder ---
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder

WORKDIR /src

# Кешуємо модульний граф окремо від вихідного коду.
COPY go.mod ./
RUN go mod download

COPY *.go ./
COPY api/openapi.yaml ./api/openapi.yaml

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

# Збираємо статичний Linux-бінарник із build metadata.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildDate=${BUILD_DATE}" \
    -o /out/site-checker .

# --- Stage 2: Production Image ---
FROM alpine:${ALPINE_VERSION}

RUN apk --no-cache add ca-certificates

# Непривілейований користувач для runtime.
RUN addgroup -S app && adduser -S -D -H -u 10001 -G app app

WORKDIR /app
COPY --from=builder /out/site-checker /app/site-checker

USER 10001:10001
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=30s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1

ENTRYPOINT ["/app/site-checker"]
