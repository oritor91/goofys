# Builds goofys inside a manylinux2014 (CentOS 7 / glibc 2.17) container so the
# resulting binary runs on very old glibc-based Linux distros. goofys has no
# cgo dependencies, so we cross-compile for both amd64 and arm64 from the same
# x86_64 build container via GOARCH, rather than needing a second aarch64
# container + QEMU emulation.
FROM quay.io/pypa/manylinux2014_x86_64

ARG GOARCH=amd64
ARG GO_VERSION=1.25.0

RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tar.gz && \
    tar -C /usr/local -xzf /tmp/go.tar.gz && \
    rm /tmp/go.tar.gz

ENV PATH=/usr/local/go/bin:$PATH
ENV GOARCH=${GOARCH}
ENV GOOS=linux
ENV CGO_ENABLED=0

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=unknown
RUN go build -ldflags "-X main.Version=${VERSION}" -o /out/goofys-linux-${GOARCH} .
