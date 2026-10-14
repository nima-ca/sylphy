FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
    -o /out/sylphy-server ./cmd/sylphy-server
# The data directory, created here because the final image has no shell.
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sylphy-server /sylphy-server
# Owned by the nonroot user (uid 65532), so a fresh named volume mounted at
# /data is writable. Persistence stays off unless SYLPHY_PERSISTENCE_MODE is set:
#   docker run -v sylphy-data:/data -e SYLPHY_PERSISTENCE_MODE=aof ...
COPY --from=build --chown=65532:65532 /out/data /data
ENV SYLPHY_ADDR=0.0.0.0:6380
ENV SYLPHY_DIR=/data
VOLUME /data
EXPOSE 6380
USER nonroot:nonroot
ENTRYPOINT ["/sylphy-server"]