FROM golang:1.18-alpine as build

WORKDIR /app

COPY  go.mod ./
COPY  go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o app/upload-move-files


FROM golang:1.18-alpine as service

COPY --from=build app/ app/

WORKDIR app

CMD [ "app/upload-move-files" ]


