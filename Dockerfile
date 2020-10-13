FROM lganzzzo/alpine-cmake:latest

RUN apk update

RUN apk add postgresql-dev

WORKDIR /module

COPY ./cmake cmake
COPY ./src src
COPY ./test test
COPY ./utility utility
COPY ./CMakeLists.txt CMakeLists.txt

WORKDIR /module/utility

RUN ./install-oatpp-modules.sh

WORKDIR /module/build

ARG PG_HOST=postgres
RUN cmake ..
RUN make

ENTRYPOINT ["./test/module-tests"]
