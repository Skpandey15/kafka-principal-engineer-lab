# Kafka Observability & Troubleshooting (WP-16)

## Prerequisites

- WP-04 (partitioning) -- hot-partition diagnosis builds directly on
  partition-key mechanics.
- WP-05 (consumer groups) -- consumer lag is fundamentally a
  consumer-group concept.
- `PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`'s "Consumer lag explosion"
  row -- the one failure-matrix row this WP owns.

## 1. A deliberate departure from this repository's Testcontainers-first testing convention

Every prior lab tests against an ephemeral, Testcontainers-managed
cluster spun up fresh per test class. This WP's tests run against the
ALREADY-RUNNING, persistent `platform/kafka-cluster/` +
`platform/observability/` environments instead -- a deliberate choice,
not an oversight: this WP's entire subject is real, standing
infrastructure (a JMX exporter attached to a real broker process, a
real Prometheus scraping it over real HTTP, a real Grafana dashboard
reading from Prometheus). Spinning up Prometheus and Grafana fresh
inside Testcontainers for every test run would add real complexity to
prove something no differently than exercising the actual, persistent
stack an operator would actually run and look at.

## 2. Two completely different metric sources, on purpose

`platform/observability/prometheus.yml` scrapes TWO separate exporters,
not one:

- **The three brokers' own JMX exporter agents** (`platform/kafka-cluster/docker-compose.yml`,
  confirmed real: `curl localhost:7071/metrics` returns 6,300+ real
  `kafka_*` metrics) -- BROKER-side signals: throughput
  (`kafka_server_brokertopicmetrics_bytesin_total`), replication health
  (`kafka_server_replicamanager_underreplicatedpartitions`), request
  handler saturation
  (`kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent`).
- **`kafka-exporter`** (`danielqsj/kafka-exporter`, a real, separately
  governed community project) -- CONSUMER-GROUP lag
  (`kafka_consumergroup_lag`), computed the exact same way
  `kafka-consumer-groups.sh --describe` computes it: querying the Admin
  API directly (committed offset vs. log-end offset per partition), NOT
  read from any broker-side JMX MBean.

This split is not arbitrary -- **there is no broker-side JMX metric for
consumer lag.** The broker tracks committed offsets and log-end
offsets; it has no concept of "how far behind is consumer group X,"
because that's a property of a SPECIFIC consumer group's progress, not
of the broker's own state. Something external has to compute it, which
is exactly what `kafka-exporter` (and `kafka-consumer-groups.sh`) do.
`LagInspector` in this lab's own code computes the SAME thing a third
way, directly via the Admin API, and
`nativeAdminApiLagMatchesThePrometheusLagMetric` proves all three
methods agree: 6 unconsumed records, computed natively, match exactly
what Prometheus reports after scraping kafka-exporter's own independent
computation.

## 3. A real finding: `KAFKA_OPTS` breaks the container's own healthcheck

Attaching the JMX exporter via `KAFKA_OPTS="-javaagent:..."` (the
confirmed-real mechanism -- `kafka-run-class.sh` passes `KAFKA_OPTS`
straight into every java invocation it makes) broke ALL THREE brokers'
healthchecks outright: `kafka-broker-api-versions.sh` (the healthcheck
command) is ALSO built on `kafka-run-class.sh`, and ALSO inherits the
SAME container-wide `KAFKA_OPTS` -- so it ALSO tries to bind the
exporter's HTTP metrics port, which the already-running broker process
already holds. Real, confirmed evidence:
`java.net.BindException: Address in use`, exit code 1, healthcheck
`unhealthy` on all three brokers. The fix: `KAFKA_OPTS=` clears the
variable for JUST the healthcheck's own invocation
(`platform/kafka-cluster/docker-compose.yml`) -- a real, generalizable
lesson: a container-wide environment variable meant for ONE long-running
process silently applies to EVERY OTHER process later run inside that
same container, including tooling you don't think of as "the app."

## 4. Consumer lag, natively and via the pipeline

`nativeAdminApiLagMatchesThePrometheusLagMetric` produces 10 records to
a single-partition topic, commits exactly offset 4 (leaving 6
unconsumed), and confirms TWO independent computations agree: the
native `LagInspector` (Admin API, computed directly inside the test)
and the real Prometheus/kafka-exporter pipeline (an HTTP query against
Prometheus's own `/api/v1/query` endpoint). The Prometheus-side number
takes real, observable extra latency to appear -- kafka-exporter polls
the Admin API on its own interval, then Prometheus scrapes
kafka-exporter on ITS OWN interval (lowered to 5s in this lab's
`prometheus.yml`, deliberately, for fast feedback -- Section 9) -- a
concrete, hands-on demonstration of monitoring-pipeline latency, not
just an abstract warning.

## 5. Failure-matrix row: consumer lag explosion, no backpressure

`sustainedConsumerLagGrowsUnboundedWithNoProducerBackpressure` is this
WP's one dedicated failure-matrix row, made real: a consumer group
commits a starting position and then NEVER actually consumes. 20
records produced, then 40 more -- every single one of the 60 sends
succeeds (verified via `.get()` on each future, never throwing), and
the native lag computation grows in lockstep: 20, then 60. Real,
direct evidence of exactly what the failure matrix says: "Kafka has no
built-in backpressure toward producers based on consumer lag --
production continues regardless of how far behind consumers fall, up
to retention limits."

## 6. Failure-matrix row's stated consequence: retention-exceeded data loss

`lagExceedingRetentionCausesRealDataLossForTheLaggingGroup` is the
"up to retention limits" half of the SAME failure-matrix row, made
real: a topic with `retention.ms=3000`/`segment.ms=3000` (a lab-speed
override -- Section 9), 30 records produced, a consumer group that
commits offset 0 WITHOUT ever consuming (modeling a group that fell
behind from the very start). The test waits for the broker to actually
delete the expired segment (confirmed via a real, moving earliest
offset from the Admin API), then confirms the group's committed
position no longer exists: a fresh subscribe resets past the gap
(`auto.offset.reset=earliest` finds the new, later earliest offset, not
position 0) and the first record actually received has an offset well
past 0. A real, verified finding building this test: **retention
deletes an entire rolled SEGMENT at once, not record-by-record** -- a
manual verification run (produce 30, wait, check offsets) showed
earliest and latest offset BOTH already at 30 within seconds of the
segment rolling and expiring, not a gradual trickle. The test
accordingly produces a few FRESH records after confirming deletion, so
there's still real data for the consumer to land on past the gap,
rather than asserting against a topic that may have been emptied
entirely.

## 7. A hot partition, confirmed two ways

`aHotPartitionShowsRealUnevenPerPartitionThroughput` sends every one of
30 records to the SAME partition (key `0` explicitly, WP-04's own
partition-key mechanics), then confirms the skew BOTH natively (Admin
API end-offsets: partition 0 at 30, partitions 1-2 at 0) AND through
the real observability pipeline (`kafka_topic_partition_current_offset`,
scraped from kafka-exporter) -- the exact signal a real Grafana panel
(this lab's own `kafka-overview.json` dashboard, panel 5) would show an
operator diagnosing this.

## 8. Broker-side throughput, confirmed against real JMX

`brokerJmxMetricsReflectRealProducedThroughput` produces 50 real
records and confirms `kafka_server_brokertopicmetrics_bytesin_total`
(a genuine broker-side JMX metric, scraped by the JMX exporter, not
kafka-exporter) actually increases -- closing the loop from "I produced
data" to "the metric a Grafana dashboard would show moved," the same
connection Section 2's split-exporter design makes explicit.

## 9. Lab-speed overrides, and why they're never production settings

Three deliberate speed overrides exist ONLY for fast, bounded test
feedback in this local, single-user learning environment -- never
appropriate in production:

| Override | Real default | Why lowered here |
|---|---|---|
| Prometheus `scrape_interval` | 1 minute (Prometheus's own default) | 5s, so this lab's own tests don't wait up to a full minute per assertion |
| `KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS` | 5 minutes (300000ms) | 2000ms, so Section 6's segment-deletion experiment completes within a bounded test timeout, not a multi-minute one |
| Grafana anonymous admin access | Real authentication | Appropriate only for this local environment -- see the README's "Production considerations" |

A real production deployment would use each of these at (or near) its
real default -- frequent retention checks and sub-minute scrapes both
cost real broker/Prometheus CPU and I/O at scale, for no benefit beyond
faster feedback in a lab.

## 10. The operational diagnostic sequence (curriculum-map row 23)

Given a symptom, the sequence this WP's own experiments demonstrate,
in order:

1. **Is it lag, or throughput, or replication health?** Check the
   Grafana dashboard's relevant panel first -- lag (panel 1),
   bytes-in-per-topic (panel 2), under-replicated partitions (panel 3).
2. **If lag**: is it ONE partition or spread evenly (Section 7's hot-partition
   signature) or growing on every partition of one group (Section 5's
   signature)? `kafka_consumergroup_lag`'s own `partition` label
   distinguishes these immediately.
3. **If throughput looks skewed**: cross-reference
   `kafka_topic_partition_current_offset` per partition (Section 7) --
   a single hot key is the most common real cause, traceable back to
   WP-04's own partitioning mechanics.
4. **If a broker looks saturated**: `kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent`
   falling toward 0 is the broker-side signal (Section 8) -- not
   something kafka-exporter or any consumer-side tool can see, since
   it's about the BROKER's own capacity, not any one group's progress.
5. **Only once the SIGNAL is identified** does root-causing draw on the
   relevant earlier WP -- partitioning (WP-04) for skew, consumer-group
   mechanics (WP-05) for lag, replication (WP-07) for under-replication.
   Observability tells you WHAT is wrong; the earlier WPs' own
   mechanisms tell you WHY.

## 11. Lab limitations

- No JMX-based consumer-side metrics (`records-lag-max` and similar,
  exposed by the CONSUMER client's own JMX MBeans) -- this lab uses
  `kafka-exporter`'s Admin-API-based approach exclusively for lag,
  which needs no code changes to the consuming application at all and
  is the more common real-world pattern; the consumer-client-JMX
  alternative is named here, not built.
- No alerting (Prometheus Alertmanager) -- this WP builds the metrics
  pipeline and dashboard, not the alert-on-threshold layer on top of
  it.
- The Grafana dashboard (`kafka-overview.json`) is intentionally small
  -- 5 panels covering exactly what this lab's own experiments produce
  real evidence for, not an exhaustive production dashboard covering
  every JMX metric the exporter exposes.
- Grafana's `/api/search` endpoint did not reflect the provisioned
  dashboard in testing (Grafana 13.x's newer unified-storage search
  indexing appears to lag behind provisioning) even though
  `/api/dashboards/uid/...` and the actual UI both served it correctly
  -- noted as an observed tooling quirk, not investigated further since
  it didn't affect the dashboard's actual availability.

## 12. Principal Engineer questions

**1. Why does `kafka-exporter` need to query the Admin API on its OWN
polling interval, rather than Prometheus just asking it fresh on every
scrape?** It could -- but querying the Admin API for every consumer
group's offsets on every single Prometheus scrape (every 5s in this
lab, every 15-30s in a more realistic deployment) adds real load to the
brokers' own coordinator machinery for every scrape, whether or not
anything changed. Decoupling the two lets `kafka-exporter` poll at a
pace appropriate for how fast consumer group state actually changes,
independent of how often Prometheus wants a fresh number.

**2. Section 3's finding says a container-wide `KAFKA_OPTS` broke the
healthcheck -- would the SAME problem affect a real production
deployment's monitoring, not just this lab's Docker healthcheck?**
Yes, precisely the same class of bug: any OTHER ad-hoc Kafka CLI tool
run on a production broker host (an operator running
`kafka-topics.sh --describe` for a quick check, for instance) would
ALSO try to bind the exporter's port and fail, for the identical
reason. Real production JMX exporter deployments typically pin the
exporter's HTTP port explicitly and either accept this limitation for
ad-hoc CLI use or exclude `KAFKA_OPTS` from CLI-tool wrapper scripts
the same way this lab's healthcheck now does.

**3. Why does the retention-exceeded experiment (Section 6) need a
SINGLE partition, while the hot-partition experiment (Section 7)
deliberately uses three?** Section 6 needs the segment-deletion timing
to be unambiguous -- multiple partitions would each roll and expire on
their own independent schedules, complicating "wait until the data is
gone" into "wait until ALL partitions' data is gone," for no added
insight. Section 7's entire POINT is the contrast BETWEEN partitions,
which requires more than one to exist.

**4. Section 4 shows two independently-computed lag numbers (native
Admin API vs. the Prometheus pipeline) agreeing -- what would it mean,
diagnostically, if they DIDN'T agree in a real system?** A real,
useful signal: if the native, live Admin-API number and Prometheus's
number diverge PERSISTENTLY (not just by the expected scrape-latency
window), it usually means kafka-exporter itself is stuck, crashed, or
scraping a stale/cached view -- the monitoring PIPELINE has a problem,
distinct from the actual Kafka-side lag it's supposed to be reporting.
Cross-checking against the native computation (exactly what this test
does) is a real, useful way to validate your OWN observability tooling,
not just the system it observes.

**5. Why is the JMX exporter attached as a Java AGENT
(`-javaagent:...`) instead of running as a separate process that
connects to the broker's JMX port remotely?** Attaching in-process
avoids exposing a plain, unauthenticated JMX remote port at all (this
repository's brokers don't set `JMX_PORT`, deliberately) -- the agent
reads the SAME JVM's MBeans directly and serves Prometheus-format
metrics over its OWN dedicated HTTP port, with no separate JMX network
protocol involved. Fewer network-exposed protocols, same data.

**6. Section 9's overrides are all explicitly test-speed-only -- how
would you verify, in a REAL deployment, that you haven't accidentally
left one of these lab-speed values in production config?** Exactly the
kind of thing a documented, versioned configuration review process
should catch (comparing deployed broker config against your own
documented production baseline) -- this is also precisely why this
lab's own `docker-compose.yml` comments explicitly flag each override
as "never appropriate in production" inline, at the point of
configuration, not just in a separate doc a reviewer might not read.

**7. Why does this WP's diagnostic sequence (Section 10) put "is it
lag, throughput, or replication health" FIRST, before looking at any
specific partition or broker?** Because the FOLLOW-UP question
(which partition? which broker? which consumer group?) depends
entirely on which category the symptom falls into -- lag points you
toward consumer-group/partition-key questions (WP-04/WP-05);
replication health points you toward broker/ISR questions (WP-07).
Skipping straight to "let me look at partition 3" without first
knowing WHICH signal is abnormal risks investigating the wrong layer
entirely.
