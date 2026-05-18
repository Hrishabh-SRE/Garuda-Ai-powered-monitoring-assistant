FROM paas.jfrog.io/dockerhub/ubuntu_base/ubuntu:v1 AS build
ENV DEBIAN_FRONTEND=noninteractive

# Go toolchain configuration
ENV GOLANG_VERSION=1.22.11
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:/usr/local/go/bin:$PATH

COPY go1.22.11.linux-amd64.tar.gz /tmp/golang.tar.gz
RUN tar -C /usr/local -xzf /tmp/golang.tar.gz \
    && rm /tmp/golang.tar.gz \
    && mkdir -p "${GOPATH}/src" "${GOPATH}/bin"
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
FROM paas.jfrog.io/dockerhub/ubuntu_base/ubuntu:v1
    
    # Create non-root user (uid/gid 65532 matches distroless 'nonroot' convention).
RUN groupadd --system --gid 65532 nonroot \
 && useradd --system --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin nonroot
    
WORKDIR /
COPY --from=build /out/bridge /bridge
    
USER nonroot:nonroot
    
EXPOSE 8080 9090
ENTRYPOINT ["/bridge"]
    
