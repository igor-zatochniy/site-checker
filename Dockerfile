# --- Stage 1: Builder ---
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Кешуємо залежності окремо
COPY go.mod ./
RUN go mod download

# Копіюємо вихідний код (тепер зміни в README не скидають кеш збирання)
COPY main.go ./

# Збираємо без налагоджувальної інформації та зі статичним компонуванням
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o site-checker main.go

# --- Stage 2: Final Production Image ---
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

# Створюємо непривілейованого користувача
RUN adduser -D -u 10001 appuser
WORKDIR /home/appuser

# Копіюємо артефакт
COPY --from=builder /app/site-checker .

# Переходимо до непривілейованого користувача
USER appuser

CMD ["./site-checker"]
