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

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sylphy-server /sylphy-server
ENV SYLPHY_ADDR=0.0.0.0:6380
EXPOSE 6380
USER nonroot:nonroot
ENTRYPOINT ["/sylphy-server"]