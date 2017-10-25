pipeline {
    agent { 
        docker {
            image 'goldenbadger/cerberus-ci'
            args '-v $HOME/.cargo:/root/.cargo'
        }
    }
    stages {
        stage('build') {
            steps {
                sh 'cargo test --all'
            }
        }
    }
}
