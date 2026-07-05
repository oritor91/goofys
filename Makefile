run-test: s3proxy.jar
	./test/run-tests.sh

s3proxy.jar:
	wget https://github.com/andrewgaul/s3proxy/releases/download/s3proxy-1.6.0/s3proxy -O s3proxy.jar

get-deps: s3proxy.jar
	go get -t ./...

build:
	go build -ldflags "-X main.Version=`git rev-parse HEAD`"

install:
	go install -ldflags "-X main.Version=`git rev-parse HEAD`"

# Cross-compiled builds inside a manylinux2014 (CentOS 7 / glibc 2.17) container,
# for maximum compatibility with older Linux distros. Two independent targets so
# they can run in parallel, e.g. `make -j2 build-manylinux-all`.
MANYLINUX_IMAGE := goofys-manylinux2014-builder
DIST_DIR := dist

.PHONY: build-manylinux-amd64 build-manylinux-arm64 build-manylinux-all

build-manylinux-amd64:
	mkdir -p $(DIST_DIR)
	docker build \
		-f docker/manylinux2014/Dockerfile \
		--build-arg GOARCH=amd64 \
		--build-arg VERSION=`git rev-parse HEAD` \
		-t $(MANYLINUX_IMAGE)-amd64 \
		.
	cid=`docker create $(MANYLINUX_IMAGE)-amd64` && \
		trap "docker rm -f $$cid >/dev/null" EXIT && \
		docker cp $$cid:/out/goofys-linux-amd64 $(DIST_DIR)/goofys-linux-amd64

build-manylinux-arm64:
	mkdir -p $(DIST_DIR)
	docker build \
		-f docker/manylinux2014/Dockerfile \
		--build-arg GOARCH=arm64 \
		--build-arg VERSION=`git rev-parse HEAD` \
		-t $(MANYLINUX_IMAGE)-arm64 \
		.
	cid=`docker create $(MANYLINUX_IMAGE)-arm64` && \
		trap "docker rm -f $$cid >/dev/null" EXIT && \
		docker cp $$cid:/out/goofys-linux-arm64 $(DIST_DIR)/goofys-linux-arm64

build-manylinux-all: build-manylinux-amd64 build-manylinux-arm64
