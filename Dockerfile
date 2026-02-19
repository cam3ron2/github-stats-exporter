# syntax=docker/dockerfile:1

ARG OCI_IMAGE_AUTHORS="cam3ron2"
ARG OCI_IMAGE_URL="https://github.com/cam3ron2/github-stats-exporter"
ARG OCI_IMAGE_DOCUMENTATION="https://github.com/cam3ron2/github-stats-exporter/blob/main/README.md"
ARG OCI_IMAGE_SOURCE="https://github.com/cam3ron2/github-stats-exporter"
ARG OCI_IMAGE_REVISION="unknown"
ARG OCI_IMAGE_VERSION="dev"
ARG OCI_IMAGE_CREATED="1970-01-01T00:00:00Z"
ARG OCI_IMAGE_TITLE="github-stats-exporter"
ARG OCI_IMAGE_DESCRIPTION="GitHub activity metrics exporter in OpenMetrics format."

FROM golang:1.25-alpine AS builder
WORKDIR /src

COPY . ./
RUN go mod download

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/github-stats-exporter ./cmd/github-stats-exporter

FROM alpine:3.23
ARG OCI_IMAGE_AUTHORS
ARG OCI_IMAGE_URL
ARG OCI_IMAGE_DOCUMENTATION
ARG OCI_IMAGE_SOURCE
ARG OCI_IMAGE_REVISION
ARG OCI_IMAGE_VERSION
ARG OCI_IMAGE_CREATED
ARG OCI_IMAGE_TITLE
ARG OCI_IMAGE_DESCRIPTION

LABEL org.opencontainers.image.authors="${OCI_IMAGE_AUTHORS}" \
      org.opencontainers.image.url="${OCI_IMAGE_URL}" \
      org.opencontainers.image.documentation="${OCI_IMAGE_DOCUMENTATION}" \
      org.opencontainers.image.source="${OCI_IMAGE_SOURCE}" \
      org.opencontainers.image.revision="${OCI_IMAGE_REVISION}" \
      org.opencontainers.image.version="${OCI_IMAGE_VERSION}" \
      org.opencontainers.image.created="${OCI_IMAGE_CREATED}" \
      org.opencontainers.image.title="${OCI_IMAGE_TITLE}" \
      org.opencontainers.image.description="${OCI_IMAGE_DESCRIPTION}"

RUN adduser -D -u 10001 appuser
USER appuser
WORKDIR /app

COPY --from=builder /out/github-stats-exporter /app/github-stats-exporter

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:8080/readyz || exit 1
ENTRYPOINT ["/app/github-stats-exporter"]
