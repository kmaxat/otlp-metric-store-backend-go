# Build stage: a static Go binary.
FROM golang:1.26.10 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /otlp-metric-store ./cmd/otlp-metric-store

# Runtime stage: no shell, no package manager, runs as a non-root user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /otlp-metric-store /otlp-metric-store
EXPOSE 4317 9464
ENTRYPOINT ["/otlp-metric-store"]
