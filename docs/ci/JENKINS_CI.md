# Jenkins CI for this repository

This repo has no CI/CD service wired up yet. The [`Jenkinsfile`](../../Jenkinsfile)
at the repo root is a starting point: it automates the one real "script"
every lab already has -- its own Gradle wrapper (`./gradlew test`) -- by
running each lab's test suite as its own Jenkins pipeline branch, in
parallel.

## Why this is realistic to automate at all

Most labs' test suites don't need any manual environment brought up
first. They use [Testcontainers](https://testcontainers.com/) (or, for
lab-14, Spring's `@EmbeddedKafka`) to spin up a real, ephemeral Kafka
cluster -- brokers, and for some labs Postgres or a second Kafka cluster
-- inside the test JVM's own lifecycle, then tear it down automatically.
That's exactly what a CI agent needs: give it Docker and a JDK, and
`./gradlew test` is the whole job.

## The three categories

| Category | Labs | What the pipeline does |
|---|---|---|
| Self-contained (Testcontainers/embedded) | lab-02, lab-02b (two independent Gradle projects: `producer/` and `consumer/`), lab-03 through lab-09, lab-12, lab-13, lab-14, lab-16, lab-18, lab-19 (16 Gradle projects) | `./gradlew test` on a Docker-capable agent. Nothing else. Jenkins runs `./gradlew check` (equal to `test` for most labs; for lab-02b it also runs the Docker-backed `integrationTest` suite, which is **always enforced when `JENKINS_URL`/`CI` is set** and only skippable on a developer machine with no Docker). lab-02b also gets a separate `lab-02b-frontend-build` branch (`npm ci && npm run build` in a throwaway `node:22-alpine` container) because its React UI is not covered by Gradle. |
| Needs `platform/kafka-connect/plugins/` pre-populated | lab-10, lab-11 | Run `platform/kafka-connect/fetch-plugins.sh` once (downloads the pinned Debezium Postgres connector + connect-file plugin), then `./gradlew test`. The Jenkinsfile does this once and `stash`/`unstash`es the result into both branches rather than downloading it twice. |
| Needs an already-running `platform/*/docker-compose.yml` environment | lab-15, lab-17 | `docker compose up -d` the environment the test suite expects (kafka-cluster + observability for lab-15; kafka-security, with certs generated first, for lab-17), `./gradlew test`, then `docker compose down -v` in a `finally` block so the environment never lingers on the agent. |
| Not automated | lab-01 | A CLI-only walkthrough with no Gradle project -- nothing to run. |

lab-15 and lab-17 are deliberate exceptions, not oversights -- both labs'
own READMEs explain why Testcontainers wasn't used for them (lab-15
needs a real JMX-exporting broker fleet that Prometheus scrapes over
time; lab-17 needs a real self-signed CA and SCRAM users bootstrapped at
storage-format time). The Jenkinsfile respects that instead of forcing
them into the same pattern as the other 16.

## `platform-k8s/` (k3d) deploy validation -- opt-in

A separate stage, `k3d deploy validation (platform-k8s/)`, deploys every
`platform-k8s/*` environment (including `event-console`, which builds its own images and so needs JDK 21 + Docker on the agent) to a real k3d cluster and treats that
environment's own `setup.sh` (which already does `kubectl apply` + wait
for real pod readiness, per
[`platform-k8s/_lib/common.sh`](../../platform-k8s/_lib/common.sh)) as
the pass/fail check. Unlike the Testcontainers-based lab suites, this is
a genuine CD-style check -- it doesn't run any Gradle tests, it proves
the Kubernetes manifests themselves still deploy and reach Ready.

It's gated behind a `RUN_K8S_DEPLOY_VALIDATION` build parameter,
**default `false`**. Reasons it's opt-in rather than on by default:

- It deploys 9 environments -- real JVM Kafka brokers each time -- onto
  one shared k3d cluster, sequentially, one at a time (never all 9
  concurrently). That's this repo's own hard-learned practice: running
  too many heavy JVM environments simultaneously is what caused real
  Docker/Rancher Desktop instability during local development of this
  repo's Kubernetes environments.
- It can take 30-45+ minutes end to end, versus a few minutes for the
  Testcontainers-based lab suites -- too slow to make every push/PR wait
  on.
- It needs `k3d` and `kubectl` installed on the agent, on top of Docker
  and JDK 21.

Every environment is deployed, validated, then wiped (`cleanup.sh
--wipe`, deleting its namespace and PersistentVolumeClaims) before the
next one starts -- so a failure in one environment never leaves stray
state for the next. `kafka-cluster` is the one exception: it's deployed
once, kept up while its four dependents (`schema-registry`,
`kafka-connect`, `observability`, `kafka-ui` -- each of which requires it,
per their own `setup.sh` prerequisite checks) are deployed, validated,
and wiped one at a time, then wiped itself last. The whole k3d cluster is
destroyed (`destroy-cluster.sh`) in the stage's `post { always { ... } }`
block, since a CI agent shouldn't accumulate a persistent cluster across
runs the way `platform-k8s/README.md` assumes for local, ongoing use.

To actually run it: tick `RUN_K8S_DEPLOY_VALIDATION` on a "Build with
Parameters" run, or flip its `defaultValue` to `true` once you've seen it
pass at least once against your real agent, or wire a nightly `cron`
trigger that pre-sets the parameter (e.g. via a
`parameters([booleanParam(...)])` override in a separate scheduled job,
or the `triggers { cron(...) }` + `parameters` combination your Jenkins
version supports).

## Wiring this into an actual Jenkins instance

The `Jenkinsfile` makes generic assumptions since this repo has no
access to your Jenkins controller:

- **Agent label `docker`**: every `node('docker') { ... }` block expects
  an agent with that label and a working Docker daemon/socket. If your
  agents use a different label, do a find/replace on `'docker'` in the
  Jenkinsfile.
- **JDK 21 and bash on `PATH`**: the pipeline calls `./gradlew` directly
  rather than through a configured Jenkins JDK tool, to keep it portable.
  If your agents don't have JDK 21 pre-baked, either build one into your
  agent image or add a `tools { jdk 'jdk21' }` block once you've
  configured that JDK installation in Jenkins.
- **Outbound internet access**: Maven Central (Gradle dependencies) and
  Docker Hub/ghcr.io (Testcontainers images, plus the Kafka/Debezium/
  Postgres images `platform/*/docker-compose.yml` pulls).

**Recommended job type: Multibranch Pipeline.** Point it at this repo;
Jenkins will discover the `Jenkinsfile` automatically on every branch and
PR, which matches this repo's existing convention of a PR per work
package. A plain Pipeline job pointed at a fixed branch works too, driven
by a GitHub webhook or polling, if you don't want per-branch/per-PR runs.

## What this Jenkinsfile deliberately does not do

- It does not deploy or validate `platform-k8s/` on every build -- that's
  the opt-in `RUN_K8S_DEPLOY_VALIDATION` stage described above, not part
  of the default fast path. `platform/` stays the primary, documented way
  to run each lab; `platform-k8s/` is an optional, secondary way, per
  `platform-k8s/README.md`.
- It does not run lab experiments/demo apps (`runProducer`,
  `runSecurityDemo`, etc.). Those are interactive, manual-exploration
  tools by design, not automated checks -- the *tests* are the automated
  evidence; the demo apps are for a person to watch happen.

## Running the Jenkinsfile on a real Jenkins, on your own machine

Everything above describes what the Jenkinsfile *should* do. [`ci/jenkins/`](../../ci/jenkins) lets you
find out: it builds and starts a real Jenkins controller (LTS, in a container), configured entirely as
code, whose single job runs **this repository's `Jenkinsfile`** from your working copy's committed
branch.

```bash
ci/jenkins/run.sh                                   # build + start: http://localhost:8082
ci/jenkins/trigger.sh "lab-02b-spring-boot-event-console/producer,lab-02b-spring-boot-event-console/consumer,lab-02b-spring-boot-event-console/frontend,event-console-alert-rules"
ci/jenkins/stop.sh [--wipe]                         # --wipe also deletes its home (jobs, Gradle cache)
```

`trigger.sh` takes the new **`LABS`** pipeline parameter: a comma-separated list of lab paths (the names at
the top of the Jenkinsfile, plus `lab-02b-spring-boot-event-console/frontend` and `event-console-alert-rules`).
Empty means everything, which needs a far bigger machine than a laptop, so the script refuses to do that
unless you pass `--all`. The controller has **one executor**: each Gradle + Testcontainers build starts a Kafka
and a MongoDB container, and two side by side did not fit in 8 GB.

### What the first real run of the pipeline found

Nothing in this file had ever been executed by Jenkins before. Running it found problems that reading it
could not:

| Found by running it | Fix |
|---|---|
| The plugin is `timestamper`, not `timestamps` (the step is called `timestamps`) | `ci/jenkins/plugins.txt` |
| JCasC rejected a `crumbIssuer` attribute this Jenkins version does not have | removed; the default CSRF protection stays on |
| The Git plugin refuses a checkout from a local directory | `-Dhudson.plugins.git.GitSCM.ALLOW_LOCAL_CHECKOUT=true`, because mounting the repo at `/repo` is the point |
| `docker run -v "$PWD/…"` inside a Jenkins *container* names a path on the **host**, which does not exist there | Jenkins' home is mounted at the **same path** on both sides |
| The edge test (`frontend/test-nginx.sh`) called `127.0.0.1:<published port>`, which from inside the CI container is the wrong machine | it now makes every request from a client container on the test network, and copies files in instead of bind-mounting |
| One `promtool` expectation was wrong (an `absent()` alert keeps its `job` label) — a real test failure, the first time those tests were ever run | corrected |
| The Gradle projects ran `./gradlew check` and the `junit` step collected their results — which is the part of the Jenkinsfile that had also contained the `dir('lab-NN')` path bug fixed earlier | confirmed working |

### The result

On the final commit, with `LABS` set to the lab-02b producer, consumer, frontend and the alert rules:

```text
Result:                SUCCESS in 6 min 7 s   (4 parallel branches, run one at a time on 1 executor)
producer:              ./gradlew check -> BUILD SUCCESSFUL (unit + 14 Testcontainers integration tests)
consumer:              ./gradlew check -> BUILD SUCCESSFUL (unit + 31 integration tests incl. the real TTL monitor)
frontend:              npm ci && npm run build; then test-nginx.sh -> "edge: all checks passed"
alert rules:           promtool check: 9 rules; promtool test: passed
JUnit recorded by Jenkins:   144 tests, 0 failed, 0 skipped (45 of them integration tests against real Kafka + MongoDB)
```

### What this does **not** show

- Only these four branches ran. The other 16 lab suites, the Connect-plugin fetch, the lab-15/17 compose
  branches and the `RUN_K8S_DEPLOY_VALIDATION` stage were not run — they need more machine than this one.
- The controller and its only agent are the same container with no authentication (it listens on
  `127.0.0.1` only). It proves the *pipeline* works on real Jenkins; it is not a model of a production
  Jenkins with a fleet of agents, credentials and a multibranch job.
