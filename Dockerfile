FROM golang:alpine as builder

RUN apk add --no-cache git

WORKDIR /build

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
      -ldflags='-w -s -extldflags "-static"' -a \
      -o main .

WORKDIR /dist

RUN cp /build/main .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /dist/main /

EXPOSE 8080
ENTRYPOINT [ "/main" ]