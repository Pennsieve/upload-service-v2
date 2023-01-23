FROM golang:1.18-alpine

#COPY  go.mod ./
#COPY  go.sum ./
#RUN go mod download

#COPY *.go ./

#COPY app/upload-move-files ./app/

WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -v -o /usr/local/bin/app .

#COPY --chown=pennsieve:pennsieve publish/ ./publish/
#
#ENV PATH "/home/pennsieve/.local/bin:$PATH"

CMD [ "app" ]