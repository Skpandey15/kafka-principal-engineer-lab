# A real Jenkins controller for running this repository's Jenkinsfile locally -- the same pipeline
# a team would run, on the same Jenkins software, just on one machine.
#
# It is also the build agent (its built-in node carries the `docker` label the Jenkinsfile asks for),
# so it needs the Docker CLI to talk to the host's Docker daemon: Testcontainers and the
# `docker run` steps start SIBLING containers on the host, not nested ones.
FROM jenkins/jenkins:lts-jdk21

USER root
ARG DOCKER_CLI_VERSION=27.5.1
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates unzip \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz" \
    | tar -xz --strip-components=1 -C /usr/local/bin docker/docker

COPY plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt

COPY casc.yaml /usr/share/jenkins/ref/casc.yaml
# No setup wizard (configuration is code), and a heap small enough to coexist with Kafka and MongoDB
# containers on a laptop. The Git plugin refuses a checkout from a local directory unless told that is
# intended -- which it is here: the job reads the repository that run.sh mounts at /repo (read-only).
ENV CASC_JENKINS_CONFIG=/usr/share/jenkins/ref/casc.yaml \
    JAVA_OPTS="-Djenkins.install.runSetupWizard=false -Dhudson.plugins.git.GitSCM.ALLOW_LOCAL_CHECKOUT=true -Xms128m -Xmx512m"

USER jenkins
