FROM ubuntu:16.04

RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates \
  g++ \
  gcc \
  git \
  libc6-dev \
  make \
  wget \
  patch \
  && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
  url="https://dl.google.com/go/go1.10.3.linux-amd64.tar.gz"; \
  wget -O go.tgz "$url"; \
  tar -C /usr/local -xzf go.tgz; \
  rm go.tgz; \
  export PATH="/usr/local/go/bin:$PATH"; \
  go version

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
