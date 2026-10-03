// Jenkinsfile -- kafka-principal-engineer-lab CI
//
// Runs each lab's own test suite: "the script for each lab" is that
// lab's own Gradle wrapper (`./gradlew test`), a self-contained Gradle
// project per this repo's one-project-per-lab convention. Most labs'
// tests spin up their own ephemeral Kafka cluster via Testcontainers, so
// a plain `./gradlew test` on a Docker-capable agent is the whole CI
// step for them -- no manual docker-compose bring-up needed. Two labs
// (15, 17) are a deliberate exception: their tests run against an
// already-running platform/*/docker-compose.yml environment, so those
// two branches bring that environment up first and tear it down after,
// same as you'd do by hand.
//
// See docs/ci/JENKINS_CI.md for the full breakdown of which lab falls
// into which category and why, plus how to wire this file into an
// actual Jenkins job.
//
// Generic assumptions this Jenkinsfile makes (adjust to your Jenkins):
//   - Agents labeled `docker` have a working Docker daemon/socket --
//     Testcontainers needs this for 16 of the 18 labs below.
//   - JDK 21 and bash are on PATH on those agents.
//   - Outbound internet access to Maven Central (Gradle dependency
//     resolution) and Docker Hub/ghcr.io (Testcontainers images, the
//     Kafka/Debezium/Postgres images platform/*/docker-compose.yml
//     pulls).
//
// lab-01 has no Gradle project (a CLI-only walkthrough) and is not part
// of this pipeline -- there is nothing to automate there.

def selfContainedLabs = [
    'lab-02-native-java-producer-consumer',
    'lab-03-partitioning-ordering',
    'lab-04-consumer-groups-rebalancing',
    'lab-05-offset-management-delivery-semantics',
    'lab-06-replication-isr-broker-failure',
    'lab-07-kraft-controller-quorum-failure',
    'lab-08-transactions-exactly-once',
    'lab-09-schema-evolution-governance',
    'lab-12-retry-dlq-idempotency',
    'lab-13-kafka-streams',
    'lab-14-spring-kafka',
    'lab-16-performance-capacity',
    'lab-18-failure-engineering',
    'lab-19-multi-cluster-dr',
]

// These two need platform/kafka-connect/plugins/ populated first (a
// one-time download of the Debezium Postgres connector + connect-file
// plugin) -- see platform/kafka-connect/fetch-plugins.sh's own header
// comment for why this can't just be a jar dependency.
def connectPluginLabs = [
    'lab-10-kafka-connect-cdc',
    'lab-11-transactional-outbox',
]

def runGradleTest(String lab) {
    dir(lab) {
        sh 'chmod +x gradlew'
        try {
            sh './gradlew test --no-daemon'
        } finally {
            junit testResults: 'build/test-results/test/*.xml', allowEmptyResults: true
        }
    }
}

// platform-k8s/ CD validation -- proves each lab's Kubernetes manifests
// still deploy cleanly on a real k3d cluster, separate from (and much
// heavier than) the Testcontainers-based lab test suites above. Every
// environment here is fully self-contained (its own initContainer fetches
// whatever it needs -- TLS certs, Connect plugins -- no manual pre-step),
// per platform-k8s/README.md.
//
// Deliberately sequential, one environment at a time (deploy, validate
// readiness via that environment's own setup.sh, wipe), never all 9
// running concurrently on the one shared k3d node -- this repo's own
// hard-learned practice after running too many heavy JVM environments
// at once caused real Docker/Rancher Desktop instability.
def standaloneK8sEnvironments = [
    'kafka',
    'kraft-quorum',
    'kafka-security',
    'multi-cluster-dr',
]

// These four all need platform-k8s/kafka-cluster/ applied first (their own
// setup.sh scripts check for and refuse to proceed without the
// 'kafka-cluster' namespace already existing).
def kafkaClusterDependentK8sEnvironments = [
    'schema-registry',
    'kafka-connect',
    'observability',
    'kafka-ui',
]

// Deploys one platform-k8s/<env> environment, lets its own setup.sh be the
// readiness check (kubectl apply + wait_for_pods_ready, per
// platform-k8s/_lib/common.sh), then always wipes it (namespace + PVCs)
// before moving on -- regardless of whether setup.sh succeeded, so one
// bad environment never leaves stray state for the next one. Returns true
// on success, false on failure, rather than throwing, so the caller can
// keep validating every other environment and report all failures together
// at the end instead of stopping at the first one.
def deployValidateWipe(String env) {
    def ok = true
    try {
        sh "bash platform-k8s/${env}/setup.sh"
    } catch (e) {
        ok = false
        echo "FAILED: platform-k8s/${env}/setup.sh did not reach ready state: ${e}"
    } finally {
        sh "bash platform-k8s/${env}/cleanup.sh --wipe || true"
    }
    return ok
}

pipeline {
    agent none

    options {
        timestamps()
        timeout(time: 90, unit: 'MINUTES')
        disableConcurrentBuilds()
    }

    parameters {
        // Off by default: this stage deploys 9 environments sequentially onto
        // a real k3d cluster (real JVM Kafka brokers each time) and can take
        // 30-45+ minutes, versus a few minutes for the Testcontainers-based
        // lab test suites above. Flip the default to true, or wire a nightly
        // cron trigger with this parameter pre-set, once you've seen it pass
        // at least once against your actual Jenkins agent.
        booleanParam(
            name: 'RUN_K8S_DEPLOY_VALIDATION',
            defaultValue: false,
            description: 'Also deploy every platform-k8s/ environment to a k3d cluster and validate readiness (slow; needs k3d+kubectl on the agent).'
        )
    }

    stages {
        stage('Fetch Kafka Connect plugins') {
            agent { label 'docker' }
            steps {
                sh 'bash platform/kafka-connect/fetch-plugins.sh'
                stash name: 'connect-plugins', includes: 'platform/kafka-connect/plugins/**'
            }
        }

        stage('Lab test suites') {
            steps {
                script {
                    def branches = [:]

                    selfContainedLabs.each { lab ->
                        branches[lab] = {
                            node('docker') {
                                checkout scm
                                runGradleTest(lab)
                            }
                        }
                    }

                    connectPluginLabs.each { lab ->
                        branches[lab] = {
                            node('docker') {
                                checkout scm
                                unstash 'connect-plugins'
                                runGradleTest(lab)
                            }
                        }
                    }

                    // Not self-contained via Testcontainers -- deliberately, same
                    // reason lab-17 documents ("Why not Testcontainers here"):
                    // observability needs a real, persistent JMX-exporting broker
                    // fleet plus Prometheus actually scraping it over time.
                    branches['lab-15-observability'] = {
                        node('docker') {
                            checkout scm
                            try {
                                sh '''
                                    cd platform/kafka-cluster && docker compose up -d
                                    cd ../observability && docker compose up -d
                                '''
                                sh 'sleep 30' // brokers + exporter + first Prometheus scrape need to warm up
                                runGradleTest('lab-15-observability')
                            } finally {
                                sh '''
                                    cd platform/observability && docker compose down -v || true
                                    cd ../kafka-cluster && docker compose down -v || true
                                '''
                            }
                        }
                    }

                    // Not self-contained either -- a real self-signed CA, a
                    // CA-signed broker cert, and SCRAM users bootstrapped at
                    // storage-format time, per lab-17's own README.
                    branches['lab-17-security'] = {
                        node('docker') {
                            checkout scm
                            try {
                                sh '''
                                    cd platform/kafka-security
                                    bash certs/generate-certs.sh
                                    docker compose up -d
                                '''
                                sh 'sleep 20' // broker + SCRAM/ACL bootstrap needs to finish
                                runGradleTest('lab-17-security')
                            } finally {
                                sh 'cd platform/kafka-security && docker compose down -v || true'
                            }
                        }
                    }

                    parallel branches
                }
            }
        }

        stage('k3d deploy validation (platform-k8s/)') {
            when { expression { return params.RUN_K8S_DEPLOY_VALIDATION } }
            agent { label 'docker' }
            steps {
                script {
                    sh 'bash platform-k8s/bootstrap-cluster.sh'

                    def failed = []

                    standaloneK8sEnvironments.each { env ->
                        if (!deployValidateWipe(env)) {
                            failed << env
                        }
                    }

                    try {
                        sh 'bash platform-k8s/kafka-cluster/setup.sh'

                        kafkaClusterDependentK8sEnvironments.each { env ->
                            if (!deployValidateWipe(env)) {
                                failed << env
                            }
                        }
                    } catch (e) {
                        failed << 'kafka-cluster'
                        echo "FAILED: platform-k8s/kafka-cluster/setup.sh did not reach ready state: ${e}"
                    } finally {
                        sh 'bash platform-k8s/kafka-cluster/cleanup.sh --wipe || true'
                    }

                    if (failed) {
                        error "platform-k8s/ environments that failed to deploy cleanly: ${failed.join(', ')}"
                    }
                }
            }
            post {
                // Ephemeral per CI run -- destroy the whole shared k3d cluster
                // rather than leaving it (and its port bindings) on the agent
                // for the next build, unlike the persistent-by-design cluster
                // platform-k8s/README.md describes for local use.
                always {
                    sh 'bash platform-k8s/destroy-cluster.sh || true'
                }
            }
        }
    }
}
