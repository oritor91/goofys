# Builds goofys inside a manylinux2014 (CentOS 7 / glibc 2.17) container so the
# resulting binary runs on very old glibc-based Linux distros. goofys has no
# cgo dependencies, so we cross-compile for both amd64 and arm64 from the same
# x86_64 build container via GOARCH, rather than needing a second aarch64
# container + QEMU emulation.
FROM quay.io/pypa/manylinux2014_x86_64@sha256:b47e710c0b1a84f406436ae48a3ef81b0db2924ab6f61e60dcbf48c31b5abf23

ARG GOARCH=amd64
ARG GO_VERSION=1.25.0
ARG GO_SHA256=2852af0cb20a13139b3448992e69b868e50ed0f8a1e5940ee1de9e19a123b613

RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tar.gz && \
    echo "${GO_SHA256}  /tmp/go.tar.gz" | sha256sum -c - && \
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
