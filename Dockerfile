# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/github-stats-exporter ./cmd/github-stats-exporter

FROM alpine:3.21
RUN adduser -D -u 10001 appuser
USER appuser
WORKDIR /app

COPY --from=builder /out/github-stats-exporter /app/github-stats-exporter

EXPOSE 8080
ENTRYPOINT ["/app/github-stats-exporter"]
