pipeline {
    agent { docker 'goldenbadger/cerberus-ci' }
    stages {
        stage('build') {
            steps {
                sh 'cargo test --all'
            }
        }
    }
}
