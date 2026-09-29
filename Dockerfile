# Build stage
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/broker ./cmd/broker

# Runtime stage: static binary, no shell, non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/broker /broker
EXPOSE 8080
VOLUME /data
ENTRYPOINT ["/broker"]
CMD ["-addr", ":8080", "-data", "/data"]
