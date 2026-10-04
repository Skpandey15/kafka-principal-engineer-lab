#!/usr/bin/env bash
# Builds and starts a real Jenkins controller that runs THIS repository's Jenkinsfile:
#
#   ci/jenkins/run.sh                  # start (or restart) it, http://localhost:8082
#   ci/jenkins/trigger.sh LABS         # run the pipeline, e.g. the two lab-02b services, and wait
#   ci/jenkins/stop.sh                 # stop it (keeps its home directory; --wipe deletes that too)
#
# Needs Docker and git. The pipeline is checked out from your WORKING COPY's current branch, so commit
# what you want tested first.
#
# Two details that are easy to get wrong when Jenkins itself runs in a container:
#  * Jenkins starts sibling containers on the HOST's Docker daemon. Anything it asks the daemon to
#    bind-mount (the workspace) must exist at the same path on the host -- so Jenkins' home is mounted
#    at an identical path on both sides instead of the usual /var/jenkins_home.
#  * The Docker socket belongs to a group; the container user must be in it.
set -euo pipefail
cd "$(dirname "$0")"

REPO="$(cd ../.. && pwd)"
HOME_DIR="${JENKINS_HOME_DIR:-$HOME/kafkalab-jenkins-home}"
BRANCH="$(git -C "${REPO}" rev-parse --abbrev-ref HEAD)"
NAME=kafkalab-jenkins
PORT="${JENKINS_PORT:-8082}"

[[ -S /var/run/docker.sock ]] || { echo "No Docker socket at /var/run/docker.sock." >&2; exit 1; }
if [[ -n "$(git -C "${REPO}" status --porcelain)" ]]; then
  echo "NOTE: your working copy has uncommitted changes. Jenkins checks out the COMMITTED branch '${BRANCH}', so they will not be tested." >&2
fi

mkdir -p "${HOME_DIR}"
echo "Building the Jenkins image (first time downloads the plugins)..."
docker build -q -t kafkalab/jenkins:local .

docker rm -f "${NAME}" >/dev/null 2>&1 || true
docker run -d --name "${NAME}" \
  -p "127.0.0.1:${PORT}:8080" \
  --group-add "$(stat -c %g /var/run/docker.sock)" \
  -e JENKINS_HOME="${HOME_DIR}" \
  -e LAB_BRANCH="${BRANCH}" \
  -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0='*' \
  -v "${HOME_DIR}:${HOME_DIR}" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "${REPO}:/repo:ro" \
  kafkalab/jenkins:local >/dev/null

echo -n "Waiting for Jenkins"
for _ in $(seq 1 90); do
  if curl -fsS -o /dev/null "http://localhost:${PORT}/login" 2>/dev/null || curl -fsS -o /dev/null "http://localhost:${PORT}/api/json" 2>/dev/null; then
    echo " - up: http://localhost:${PORT}  (job 'kafka-lab', branch '${BRANCH}')"
    exit 0
  fi
  echo -n "."; sleep 3
done
echo; echo "Jenkins did not come up; see: docker logs ${NAME}" >&2
exit 1
