#!groovy

ansiColor('xterm') {
  node('executor') {
    checkout scm

    def authorName  = sh(returnStdout: true, script: 'git --no-pager show --format="%an" --no-patch')
    def isMain    = env.BRANCH_NAME == "main"
    def serviceName = env.JOB_NAME.tokenize("/")[1]

    def commitHash  = sh(returnStdout: true, script: 'git rev-parse HEAD | cut -c-7').trim()
    def version    = "${env.BUILD_NUMBER}-${commitHash}"

    try {
      stage("Run Tests") {
        sh "make test-ci"
      }

      if(isMain) {
        stage ('Build and Push') {
          sh "VERSION=${version} make publish"
        }

        stage("Deploy") {
          build job: "service-deploy/pennsieve-non-prod/us-east-1/dev-vpc-use1/dev/${serviceName}",
          parameters: [
            string(name: 'IMAGE_TAG', value: version),
            string(name: 'TERRAFORM_ACTION', value: 'apply')
          ]
        }
      }
    } catch (e) {
      slackSend(color: '#b20000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL}) by ${authorName}")
      throw e
    }

    slackSend(color: '#006600', message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL}) by ${authorName}")
  }
}