# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the Garuda Alertmanager -> Kafka bridge.
#   Build stage:   internal Ubuntu 22.04 + Go 1.22.11 image
#   Runtime stage: bare internal Ubuntu base (~80 MB final image)
#
# Dependencies are vendored locally (go mod vendor) and copied into the build.

# ---------- Build stage ----------
ARG BUILD_IMAGE=artifactory-ci.myntra.com/dockerhub/ubuntu_base/ubuntugolang1.22.11:latest
FROM ${BUILD_IMAGE} AS build

WORKDIR /src

# Copy module files and vendored dependencies.
COPY go.mod go.sum ./
COPY vendor/ vendor/

# Copy source code.
COPY cmd/ cmd/
COPY internal/ internal/

# Build the binary using vendored dependencies.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -mod=vendor \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/bridge ./cmd/bridge

# ---------- Runtime stage ----------
ARG RUNTIME_IMAGE=artifactory-ci.myntra.com/dockerhub/ubuntu_base/ubuntu:v1
FROM ${RUNTIME_IMAGE}

# Create non-root user (uid/gid 65532 matches distroless 'nonroot' convention).
RUN groupadd --system --gid 65532 nonroot \
 && useradd --system --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin nonroot

WORKDIR /
COPY --from=build /out/bridge /bridge

USER nonroot:nonroot

EXPOSE 8080 9090
ENTRYPOINT ["/bridge"]
