#!/usr/bin/env bash
# Stops the local Jenkins. Its home (jobs, build history, the Gradle cache) is kept; --wipe deletes it.
set -euo pipefail
HOME_DIR="${JENKINS_HOME_DIR:-$HOME/kafkalab-jenkins-home}"
docker rm -f kafkalab-jenkins >/dev/null 2>&1 || true
echo "Jenkins stopped."
if [[ "${1:-}" == "--wipe" ]]; then
  # The home was written by the container's user; remove it through a container to avoid ownership problems.
  docker run --rm -v "${HOME_DIR}:/h" alpine sh -c 'rm -rf /h/* /h/.[!.]* 2>/dev/null || true'
  rmdir "${HOME_DIR}" 2>/dev/null || true
  echo "Jenkins home deleted."
fi
