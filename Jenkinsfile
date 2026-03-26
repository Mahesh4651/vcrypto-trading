pipeline {
    agent any

    environment {
        COMPOSE_PROJECT_NAME = 'springcrypto'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                    docker compose down || true
                    docker compose up -d --build
                '''
            }
        }

        stage('Status') {
            steps {
                sh 'docker compose ps'
            }
        }
    }
}
