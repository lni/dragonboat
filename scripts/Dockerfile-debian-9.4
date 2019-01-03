FROM debian:testing

RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates \
  g++ \
  gcc \
  g++-6 \
  gcc-6 \
  git \
  libc6-dev \
  make \
  wget \
  librocksdb-dev \
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
RUN go get -u google.golang.org/grpc

RUN set -eux; \
  url="https://github.com/facebook/rocksdb/archive/v5.13.2.tar.gz"; \
  wget -O v5.13.2.tgz "$url"; \
  mkdir /rocksdb; \
  tar -C /rocksdb -xzf v5.13.2.tgz; \
  rm v5.13.2.tgz; \
  CXX=g++-6 make -C /rocksdb/rocksdb-5.13.2 -j 16 shared_lib; \
  INSTALL_PATH=/usr/local make -C /rocksdb/rocksdb-5.13.2 install-shared; \
  touch /etc/ld.so.conf.d/usr_local_lib.conf; \
  echo '/usr/local/lib' >> /etc/ld.so.conf.d/usr_local_lib.conf; \
  ldconfig; \
  rm -rf /rocksdb

CMD cd /go/src/github.com/lni/dragonboat; make && make test
