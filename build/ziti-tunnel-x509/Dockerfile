ARG ZITI_VERSION=2.0.0
ARG ZITI_REVISION=9bf62f39ec3ba1a25d2faeea01b707410d73428d

FROM golang:1.26-alpine AS build
ARG ZITI_VERSION
ARG ZITI_REVISION
WORKDIR /src
RUN apk add --no-cache git
RUN git clone --depth 1 --branch "v${ZITI_VERSION}" https://github.com/openziti/ziti.git .
RUN set -eu; \
    needle='pfxlog.Logger().WithError(err).Fatal("could not create ziti sdk context")'; \
    grep -q "${needle}" ziti/tunnel/root.go; \
    sed -i "/${needle}/a\\\n\\trootPrivateContext.(*ziti.ContextImpl).CtrlClt.SetUseOidc(false)" ziti/tunnel/root.go; \
    grep -q 'SetUseOidc(false)' ziti/tunnel/root.go
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags "-s -w -X github.com/openziti/ziti/v2/common/version.Version=v${ZITI_VERSION} -X github.com/openziti/ziti/v2/common/version.Revision=${ZITI_REVISION}" -o /out/ziti ./ziti

FROM openziti/ziti-tunnel:${ZITI_VERSION}
COPY --from=build /out/ziti /usr/local/bin/ziti
