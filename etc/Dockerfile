FROM ubuntu:16.04

RUN apt-get update -y
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:maarten-fonville/protobuf -y
RUN apt-get update -y
RUN apt-get install libprotobuf-dev libprotoc-dev protobuf-compiler -y
RUN add-apt-repository ppa:webupd8team/java
RUN apt-get update -y
RUN echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections
RUN apt-get install -y oracle-java8-installer
RUN apt-get update -y
RUN apt-get upgrade -y
RUN apt-get dist-upgrade -y
RUN apt-get install build-essential software-properties-common -y
RUN add-apt-repository ppa:ubuntu-toolchain-r/test -y
RUN apt-get update -y
RUN apt-get install gcc-7 g++-7 -y
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 60 --slave /usr/bin/g++ g++ /usr/bin/g++-7
RUN update-alternatives --config gcc

RUN mkdir /build
WORKDIR /build

# to build docker image run: docker build -t proteus/proteus-java-build:latest -f etc/Dockerfile .