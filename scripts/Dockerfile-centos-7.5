FROM centos:latest

RUN yum install -y \
  git \
  golang \
  gflags-devel \
  snappy-devel \
  zlib-devel \
  bzip2-devel \
  gcc-c++ \
  libstdc++-devel \
  wget \
  make \
  which \
  perl

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
RUN go get -u google.golang.org/grpc

RUN set -eux; \
  url="https://github.com/facebook/rocksdb/archive/v5.13.2.tar.gz"; \
  wget -O v5.13.2.tgz "$url"; \
  mkdir /rocksdb; \
  tar -C /rocksdb -xzf v5.13.2.tgz; \
  rm v5.13.2.tgz; \
  make -C /rocksdb/rocksdb-5.13.2 -j 16 shared_lib; \
  INSTALL_PATH=/usr/local make -C /rocksdb/rocksdb-5.13.2 install-shared; \
  touch /etc/ld.so.conf.d/usr_local_lib.conf; \
  echo '/usr/local/lib' >> /etc/ld.so.conf.d/usr_local_lib.conf; \
  ldconfig; \
  rm -rf /rocksdb

CMD cd /go/src/github.com/lni/dragonboat; make && make test
