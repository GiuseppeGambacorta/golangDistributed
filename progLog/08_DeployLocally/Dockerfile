FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/agent ./progLog/08_DeployLocally/cmd/agent && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/getservers ./progLog/08_DeployLocally/cmd/getservers && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/proglog ./progLog/08_DeployLocally/cmd/proglog

FROM alpine:3.22
RUN addgroup -S -g 1000 log && adduser -S -D -H -u 1000 -G log log && \
    mkdir -p /var/lib/log && chown -R log:log /var/lib/log
COPY --from=build /out/agent /usr/local/bin/agent
COPY --from=build /out/getservers /usr/local/bin/getservers
COPY --from=build /out/proglog /usr/local/bin/proglog
USER 1000:1000
EXPOSE 8400 7400 8080
VOLUME ["/var/lib/log"]
ENTRYPOINT ["/usr/local/bin/agent"]
