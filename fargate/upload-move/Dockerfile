FROM golang:1.18-alpine

WORKDIR /app

#COPY  go.mod ./
#COPY  go.sum ./
#RUN go mod download

#COPY *.go ./

COPY app/upload-move-files ./app/

CMD [ "app/upload-move-files" ]
