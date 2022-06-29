[
  {
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group":"/aws/fargate/${environment_name}-${service_name}-${tier}-${aws_region_shortname}",
        "awslogs-region": "${aws_region}",
        "awslogs-stream-prefix": "fargate"
      }
    },
    "environment": [
      { "name" : "ENVIRONMENT", "value": "${environment_name}" },
      { "name" : "MANIFEST_TABLE", "value": "${manifest_table_name}" },
      { "name" : "FILES_TABLE", "value": "${manifest_files_table_name}" },
      { "name" : "UPLOAD_BUCKET", "value": "${upload_bucket}" },
      { "name" : "STORAGE_BUCKET", "value": "${storage_bucket}" }
    ],
    "name": "${tier}",
    "image": "${image_url}:${image_tag}",
    "cpu": ${container_cpu},
    "memory": ${container_memory},
    "essential": true,
    "repositoryCredentials": {
      "credentialsParameter": "${docker_hub_credentials}"
    }
  }
]
