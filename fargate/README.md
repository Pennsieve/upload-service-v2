# The Upload-Move Task 
The upload-move taks is part of the Upload-V2 Service. It is a long-running task
that moves files to their final storage bucket. We implement this in Fargate because 
we can guarantee that this task can be completed within the 15 lambda timelimit.


## local docker
```bash
env GOOS=linux GOARCH=amd64 go build -o app/upload-move-files
```

## Build, deploy, and run:
This migration is run on a jenkins-executor (prod/dev). To run:

1. Build the Docker container

```bash
docker buildx build --platform linux/amd64 -t pennsieve/upload_move_files .
```

2. Push the Docker container to the Pennsieve DockerHub account

```bash
docker push pennsieve/upload_move_files 
```

3. Run the Migration/(dev/prod)-migrations/(dev/prod)-package-name-migration task in Pennsieve Jenkins.

The executor has IAM access to the RDS Proxy so no RDS password needs to be provided.


