# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.6
ARG ALPINE_VERSION=3.24
ARG GO_IMAGE_DIGEST=sha256:af8d6740070b8906d12eae1c3e3ea0957fb63f492051ea05e354c38ef9fe88df
ARG ALPINE_IMAGE_DIGEST=sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

# --- Stage 1: Builder ---
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION}@${GO_IMAGE_DIGEST} AS builder

WORKDIR /src

# Кешуємо модульний граф окремо від вихідного коду.
COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
COPY internal ./internal

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

# Збираємо статичний Linux-бінарник із build metadata.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildDate=${BUILD_DATE}" \
    -o /out/site-checker .

# --- Stage 2: Production Image ---
FROM alpine:${ALPINE_VERSION}@${ALPINE_IMAGE_DIGEST}

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
