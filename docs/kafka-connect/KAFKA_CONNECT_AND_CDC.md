# Kafka Connect and Change Data Capture

## Scope note

This document covers WP-11: Kafka Connect fundamentals (the worker/task/
offset model, source and sink connectors) followed by change data capture
with Debezium. It deliberately stops before:

- The transactional outbox pattern — WP-12 (built directly on this WP's
  CDC pipeline, but not implemented here)
- Retry/DLQ architecture — WP-13
- Kafka Streams — WP-14
- Spring Kafka — WP-15
- A full observability platform — WP-16
- Full Kafka security — WP-18

Kafka Connect fundamentals are deliberately sequenced BEFORE
Debezium-specific CDC within this one work package — CDC is "just" a
source connector, and understanding what a source connector, a task, and
an offset actually are first makes Debezium's own behavior far less
mysterious.

## 1. Kafka Connect is part of Apache Kafka, not a vendor product

Unlike Confluent Schema Registry (WP-10, a Confluent-ecosystem concept
layered around Kafka), **Kafka Connect's framework — the worker, the
REST API, the task model, the offset storage — ships inside Apache
Kafka's own distribution.** Real, verified evidence: `connect-distributed.sh`,
`connect-standalone.sh`, and `connect-plugin-path.sh` all exist in
`/opt/kafka/bin/` of the same `apache/kafka:4.3.1` image every prior lab
already uses — no separate vendor image was needed to run a real Connect
worker.

**Debezium, by contrast, IS a separately-governed project** (Commonhaus
Foundation since December 2024, per `docs/references/REFERENCE_REPOSITORIES.md`)
— a connector PLUGIN that runs inside Kafka Connect's framework, not part
of Kafka itself. This document names that boundary every time it matters.

## 2. Environment

`platform/kafka-connect/` — a new, separate compose file — adds:

- **PostgreSQL** (`postgres:17.6`, pinned), configured with
  `wal_level=logical` (required for logical decoding at all — Postgres's
  default `replica` WAL level does not retain what's needed) and raised
  `max_wal_senders`/`max_replication_slots` (see Section 14 for the real,
  reproducible reason this needed raising).
- **A Kafka Connect distributed-mode worker**, `apache/kafka:4.3.1` (the
  SAME image as the broker, per Section 1), with its default entrypoint
  overridden to run `connect-distributed.sh` directly against a real
  worker properties file, instead of the image's normal broker-launch
  script.

Both are attached to the EXISTING `kafka-cluster_default` network as an
external network — reusing WP-07's 3-broker cluster unmodified, exactly
like WP-10's Schema Registry did.

**Plugin installation is a separate, explicit, documented step**
(`platform/kafka-connect/fetch-plugins.sh`) — not baked into a custom
Docker image, keeping this repository's established "official image +
configuration" pattern intact. It fetches two real, pinned artifacts:

- `connect-file:4.3.1` (the `FileStreamSourceConnector`/`FileStreamSinkConnector`
  used for Phase 1) — see Section 4 for why this is NOT optional despite
  physically shipping inside the same image.
- `debezium-connector-postgres:3.6.3.Final` (Debezium's own
  self-contained "-plugin" bundle from Maven Central, connector + every
  dependency it needs, verified reachable via a real HTTP request before
  being pinned).

## 3. Registry architecture: worker, task, REST API

```text
Kafka Connect Worker (a JVM process, distributed mode)
      │
      ├── Connector instance (e.g. one PostgresConnector)
      │        │
      │        └── Task(s) (the actual work — 1..N per connector)
      │
      └── REST API (:8083) — the ONLY client interface
```

**There is no dedicated "Kafka Connect Java client" library**, unlike
the broker (`kafka-clients`) or Schema Registry
(`kafka-schema-registry-client`, WP-10). The REST API — `POST /connectors`,
`GET /connectors/{name}/status`, `DELETE /connectors/{name}`,
`GET /connector-plugins` — IS the client interface, by design. This lab's
`ConnectRestClient` is a small, direct wrapper around the JDK's own
`java.net.http.HttpClient`, not a missing convenience.

A **connector** is configuration plus lifecycle management; the actual
work happens in one or more **tasks** it spawns (`tasks.max` bounds this).
Distributed mode means multiple WORKERS can share connectors/tasks among
themselves via a `group.id` — this lab runs a single worker, but the
config is genuinely distributed-mode's real config, the same choice a
production deployment makes for HA even starting from one worker.

## 4. Source and sink connectors — real, with pure Apache Kafka Connect

```text
file (source)
    ↓
FileStreamSourceConnector
    ↓
Kafka topic
    ↓
FileStreamSinkConnector
    ↓
file (sink)
```

Real, captured evidence (`FileStreamDemoApp`, matching the automated
`fileStreamSourceAndSinkConnectorsMoveDataThroughKafkaEndToEnd` test):
a real file's content moved through Kafka and back out to a second real
file, with zero hand-written glue code — only two connector CONFIGS,
POSTed to the REST API.

**A real, unexpected finding building this lab.** `connect-file-4.3.1.jar`
physically exists in `/opt/kafka/libs/` of the pinned image — but
inspecting the ACTUAL running worker process's Java command line
(`/proc/1/cmdline` inside the container) showed it is **not** on
`connect-distributed.sh`'s classpath:

```
-cp ... connect-api-4.3.1.jar:connect-basic-auth-extension-4.3.1.jar:
        connect-json-4.3.1.jar:connect-mirror-4.3.1.jar:
        connect-mirror-client-4.3.1.jar:connect-runtime-4.3.1.jar:
        connect-transforms-4.3.1.jar: ...
```

`connect-file` is conspicuously absent. The very first attempt to
register `FileStreamSourceConnector` failed with a real
`Failed to find any class that implements Connector` error, listing only
Debezium (already on `plugin.path`) and the internal Mirror connectors
(on the classpath). The fix: `connect-file` has to be added to
`plugin.path` explicitly, exactly like any external plugin — confirmed
by the automated `fileStreamAndDebeziumPluginsAreBothDiscoveredOnTheWorker`
test, which asserts both connectors appear in `GET /connector-plugins`
only once fetched that way.

## 5. The offset model — real, and genuinely surprising

```text
Connect record:
key   = (connector name, connector-defined "source partition")
value = whatever the connector's OWN SourceTask implementation says a "position" is
```

Real, captured evidence from `connect-offsets` for `FileStreamSourceConnector`:

```
["file-source-orders",{"filename":"/data/source-orders.txt"}]	{"position":92}
["file-source-orders",{"filename":"/data/source-orders.txt"}]	{"position":138}
```

The "offset" is a **byte position in a file** — Kafka Connect's offset
storage is a generic key-value mechanism; what a "position" MEANS is
entirely up to each connector. For Debezium's PostgreSQL connector, the
equivalent is a **WAL LSN** (log sequence number) instead — a completely
different kind of "position" (Section 9).

**A real, consequential finding: Kafka Connect's offset storage OUTLIVES
a connector's own delete/recreate lifecycle.** Deleting a connector via
the REST API does NOT clear its stored offsets. Building this lab's OWN
demo app, re-running it with a SHORTER file than a previous run (same
connector name, same filename) silently sent **zero** records — the
task's stored position (from the earlier, longer file) was already past
the new, shorter file's end. Verified deterministically by the automated
`deletingAConnectorDoesNotClearItsStoredOffsets` test: register, produce
one line, delete the connector, re-register under the SAME name and
file, and confirm the re-registered task's stored offset is STILL the
old, non-zero position — not reset to zero. This is exactly why every
connector name and topic in this lab's own runnable demo apps includes a
fresh, unique suffix per run — not cosmetic, a genuine correctness
requirement for a repeatable demo, and a direct preview of why a real
Debezium deployment's `slot.name` needs the identical consideration at
the PostgreSQL replication-slot level (Section 9).

## 6. Setting up PostgreSQL for logical replication

```sql
-- wal_level=logical (broker-level, not table-level -- set via the
-- container command, not SQL)
CREATE PUBLICATION dbz_publication FOR TABLE orders;
ALTER TABLE orders REPLICA IDENTITY FULL;
```

`plugin.name=pgoutput` — PostgreSQL's OWN built-in logical-decoding output
plugin, available since Postgres 10, **not** an extra decoder Debezium
needs installed separately. Older Debezium/Postgres material that still
references `wal2json` or `decoderbufs` predates `pgoutput` becoming the
default, recommended choice.

`publication.autocreate.mode=disabled` because this lab creates the
publication explicitly in `init-postgres.sql` — a deliberate choice,
documenting a real production consideration: a connector's configured
database user often lacks the privilege to create publications itself.

**`REPLICA IDENTITY FULL`, and why it matters concretely.** Without it,
an `UPDATE`/`DELETE`'s WAL record includes only the PRIMARY KEY's old
value, not every column's — Debezium's `before` field would be null for
every column except the key. Real, verified via the automated
`updateProducesAnEventWithFullBeforeAndAfterDueToReplicaIdentityFull`
test: `before.amount` and `after.amount` are BOTH fully populated real
values (`10.00` → `20.00`), which requires `REPLICA IDENTITY FULL`
specifically. The tradeoff, real and worth stating: `FULL` costs more WAL
volume per change than the default (key-columns-only) — worth it here
specifically so this lab's before/after evidence is real, not a
documented limitation.

## 7. Registering the Debezium connector — real

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "connect-postgres",
  "topic.prefix": "cdc",
  "table.include.list": "public.orders",
  "publication.name": "dbz_publication",
  "publication.autocreate.mode": "disabled",
  "slot.name": "debezium_orders_slot",
  "plugin.name": "pgoutput"
}
```

Real, captured: `POST /connectors` → the task reaches `RUNNING`, and a
new topic, `cdc.public.orders`, is created automatically the moment the
first real change event needs to be written to it (not eagerly at
connector-registration time — verified: registering against an EMPTY
table created no topic at all until the first real `INSERT`).

## 8. The Debezium envelope — real, complete, decoded correctly

Real, captured (`INSERT INTO orders VALUES ('O-CDC-1', 'C-501', 42.50)`),
decoded via Kafka Connect's own `JsonConverter`/`Struct` API — not
hand-parsed JSON, which would have to reimplement the `Decimal`
logical-type decoding itself:

```
op=c (create/insert) | snapshot=false | lsn=26622856
before=null
after={order_id=O-CDC-1, customer_id=C-501, amount=42.50}
```

Real, captured (`UPDATE ... SET amount=99.99` then `DELETE`):

```
op=u (update) | before={..., amount=42.50} | after={..., amount=99.99}
op=d (delete) | before={..., amount=99.99} | after=null
(tombstone: a null-value record with the same key, immediately after the delete)
```

`op` values, real and verified against this exact pinned Debezium
version: `c` (create), `u` (update), `d` (delete), `r` (read — a
snapshot-phase record, Section 9). The **tombstone** (a record with the
delete's key but a `null` value) exists specifically so Kafka's own
log-compaction mechanism can eventually remove the deleted row's history
entirely — a real, standard Kafka pattern (compacted topics use a `null`
value as "delete this key"), not a Debezium-specific invention.

`source.lsn` is the WAL position this specific change was captured at —
this connector's own "offset" concept (Section 5), a genuinely different
kind of position than a file byte offset.

## 9. Snapshot vs. streaming — real, with the actual markers

```text
Connector starts
    ↓
snapshot.mode=initial (the default): existing rows captured first, marked snapshot=first/true/last
    ↓
then: streaming, snapshot=false, for every change from that point forward
```

Real, captured (two pre-existing rows, `snapshot.mode=initial`):

```
order_id=O-PRE-1 | snapshot=first
order_id=O-PRE-2 | snapshot=last
```

Verified deterministically by the automated
`preExistingRowsAreCapturedAsASnapshotWithFirstAndLastMarkers` test.
`snapshot.mode=no_data` (used by every OTHER test in this lab's suite,
deliberately) skips the data-snapshot phase entirely, capturing only the
table's schema at start and then streaming forward — the right choice
whenever a scenario only cares about NEW changes, not existing rows
(and, as Section 15 explains, a real necessity once the `orders` table
is shared across many independent experiments).

## 10. Kafka Connect task failure — real, and NOT what this lab assumed going in

`PRINCIPAL_ENGINEER_FAILURE_MATRIX.md` anticipated: *"Kafka Connect does
not automatically retry a permanently failed task past its configured
retry policy."* This is real GENERIC Connect behavior. **Debezium's own
PostgreSQL connector, however, has additional internal resilience layered
on top of that generic behavior** — real, captured evidence, stopping the
Postgres container while the connector was running:

```
Caused by: java.net.UnknownHostException: connect-postgres
[...] Awaiting end of restart backoff period after a retriable error
```

The connector's REST status stayed `RUNNING` throughout — the failure
never surfaced as a Connect-level task `FAILED` state at all, because
Debezium's own retry/backoff logic caught and retried the connection loss
BEFORE it could propagate up to Connect's task state machine. This is
genuinely different from a connector without this kind of built-in
resilience, and it directly informed a real automated test-suite
requirement (Section 15) rather than being a theoretical caveat.

## 11. Source database unavailable during CDC — real, exact LSN evidence

Real, captured — Postgres restarted after the outage above:

```
Starting replication stream from LSN LSN{0/1963E68} with automaticFlush=false (mode=CONNECTOR)
Streaming requested from LSN LSN{0/1963E68}, received LSN LSN{0/1963E68} identified as already processed
```

Debezium resumed from the **exact** LSN position it had last committed —
correctly identifying that position as already-processed (no duplicate
delivery) — and then captured a genuinely new insert made after
reconnection. Real, verified end state: 6 total events for 3 real
operations spanning the outage (insert before, insert during the outage
window queued at the DB level, insert after reconnection) — zero
duplicates, zero loss.

**The real limitation `PRINCIPAL_ENGINEER_FAILURE_MATRIX.md` itself
already names, worth restating precisely**: this resume-cleanly guarantee
depends on the WAL/replication-slot's retention not having been exceeded
during the outage. PostgreSQL retains WAL for an inactive replication
slot indefinitely by default (a real operational risk in its own right —
disk exhaustion on the source database, not something this lab
independently re-verified with a long-enough outage to trigger it) —
past `max_slot_wal_keep_size` (if configured) or actual disk exhaustion,
resuming cleanly is no longer possible and a fresh snapshot becomes
necessary.

## 12. Comparing this to a polling-based source connector (conceptual)

This lab did not build a JDBC polling source connector to compare
against directly (avoiding unnecessary scope — see Section 16), but the
architectural contrast is worth being precise about, since it's a common
real interview question:

| | Polling JDBC source connector | Debezium (log-based CDC) |
|---|---|---|
| Detects a change | Re-queries the table on an interval, diffing against a tracked column (e.g., an `updated_at` or auto-increment ID) | Reads the database's own commit log (WAL) directly |
| Sees DELETEs | Only if the schema has a soft-delete column to query for — a hard `DELETE` is invisible | Sees every real `DELETE`, natively |
| Load on the source database | A real, recurring query load, proportional to poll frequency | Effectively none — WAL streaming is what the database already does for its own replication |
| Latency | Bounded by the poll interval | Near-real-time, bounded by replication lag |
| Ordering across tables | Not guaranteed by the connector | Preserves the database's own commit order within its capture scope |

## 13. Bridge to WP-12

Named here, not implemented — the transactional outbox pattern (WP-12)
is built DIRECTLY on top of this WP's CDC pipeline: instead of a service
writing to its own business table and separately, unreliably, trying to
also produce a Kafka message (the dual-write problem, WP-09's own
transactions doc already names this precisely), it writes an "outbox"
row in the SAME database transaction as its business change, and lets
THIS WP's CDC mechanism — Debezium reading the WAL — reliably turn that
outbox row into a Kafka message, with no application-level dual write at
all. This WP's job was proving the CDC mechanism itself works, for real;
WP-12's job is applying it to that specific pattern.

## 14. Unexpected behavior discovered building this lab

Several real, verified findings, each corrected in place rather than
assumed away:

1. **`connect-file` is not on the default classpath** (Section 4) — a
   real registration failure, fixed by adding it to `plugin.path`.
2. **Kafka Connect offset storage outlives connector delete/recreate**
   (Section 5) — a real, silent zero-records bug this lab's own demo app
   hit, fixed with unique connector names/files per run, and turned into
   its own deterministic automated test.
3. **The `apache/kafka:4.3.1` image does not bundle `curl`** — the
   platform's Docker healthcheck for the Connect worker (`curl -sf
   http://localhost:8083/connectors`) failed even though the REST API
   itself was already answering real requests from the host. Confirmed
   via `docker exec connect-worker which curl` returning nothing. Fixed
   with a dependency-free `bash /dev/tcp` probe instead — unlike
   `confluentinc/cp-schema-registry` (WP-10), which does bundle `curl`.
4. **The PostgreSQL JDBC driver sends the CLIENT JVM's default timezone
   to the server on connect** — on a host whose JVM default was the old
   zoneinfo alias `Asia/Calcutta` (pre-1996 name for `Asia/Kolkata`),
   the container's own tzdata rejected it outright:
   `FATAL: invalid value for parameter "TimeZone": "Asia/Calcutta"`.
   Fixed with `TimeZone.setDefault(TimeZone.getTimeZone("UTC"))` before
   opening any connection — the driver derives the session TimeZone from
   the JVM default, not from a connection `Properties` key (an earlier
   attempt using a `"timezone"` property was silently ignored).
5. **Testcontainers 2.x renamed module artifact IDs and removed a
   generic type.** `org.testcontainers:postgresql` (the 1.x artifact ID)
   does not exist past `1.21.4`; the correct 2.x artifact is
   `org.testcontainers:testcontainers-postgresql` (matching the
   `testcontainers-kafka` renaming already known from WP-08/09/10's own
   test helpers). `PostgreSQLContainer` is ALSO no longer generic in
   2.x — `PostgreSQLContainer<?>` is a real compile error
   ("type PostgreSQLContainer does not take parameters"); it is
   `PostgreSQLContainer` (Testcontainers 2.0 dropped the self-type
   builder-generic pattern across the library).
6. **A real test-isolation bug this lab's own suite hit and fixed**:
   because the `orders` table is shared across every test (Postgres has
   no per-connector table scoping), a test using the DEFAULT snapshot
   mode captured EARLIER tests' leftover rows as `"r"` (read) events
   before its own fresh insert's `"c"` event ever appeared
   (`expected: <c> but was: <r>`). Fixed by using
   `snapshot.mode=no_data` for every test that only cares about
   streaming changes it makes itself.
7. **Each registered Debezium connector holds one open WAL sender for
   as long as it stays registered.** This test suite deliberately never
   deletes a connector between tests (so each test's own real evidence
   stays inspectable) — by the 5th connector, the manual environment's
   `max_wal_senders=4` was really, reproducibly exhausted:
   `FATAL: number of requested standby connections exceeds
   "max_wal_senders" (currently 4)`. Raised to 20 in both the manual
   environment and the test cluster.

## 15. Automated tests

`./gradlew test` — 9 tests, against a real Kafka + PostgreSQL + Kafka
Connect worker cluster (`KafkaConnectCluster`, Testcontainers, running
BOTH the `connect-file` plugin and the real Debezium PostgreSQL connector
plugin):

1. `fileStreamAndDebeziumPluginsAreBothDiscoveredOnTheWorker`
2. `fileStreamSourceAndSinkConnectorsMoveDataThroughKafkaEndToEnd`
3. `connectSourceOffsetIsARealFilePositionNotAKafkaOffset`
4. `debeziumConnectorReachesRunningState`
5. `insertProducesACreateEventWithNullBeforeAndPopulatedAfter`
6. `updateProducesAnEventWithFullBeforeAndAfterDueToReplicaIdentityFull`
7. `deleteProducesAnEventThenATombstone`
8. `preExistingRowsAreCapturedAsASnapshotWithFirstAndLastMarkers`
9. `deletingAConnectorDoesNotClearItsStoredOffsets`

Every assertion uses a bounded condition-polling loop rather than a
fixed sleep as the synchronization mechanism, per this repository's
established test convention. The two failure-matrix scenarios
(Sections 10-11) were verified manually, real and captured, but are NOT
part of the automated suite — stopping/restarting a container mid-test
for a DETERMINISTIC assertion is possible in principle, but this lab
followed `docs/references/REFERENCE_REPOSITORIES.md`'s own established
testing-progression guidance (Docker Compose experiments for
cluster/infrastructure-level failure scenarios, Testcontainers for
application-level behavior) and kept that specific experiment manual and
reproducible instead, consistent with how WP-07/WP-08's own broker- and
controller-failure experiments were handled.

## 16. Failure matrix

| Scenario | Result | Status |
|---|---|---|
| FileStream source → Kafka → FileStream sink, end to end | Real data moved through Kafka with zero hand-written glue | **Experimentally verified** |
| `connect-file` on the default classpath | FALSE — must be added to `plugin.path` explicitly | **Experimentally verified** |
| Source connector offset survives connector delete/recreate | TRUE — stored offset outlives the connector's own lifecycle | **Experimentally verified** |
| Insert → CDC event | `op=c`, `before=null`, `after=` the real row | **Experimentally verified** |
| Update → CDC event, `REPLICA IDENTITY FULL` | `op=u`, `before`/`after` BOTH fully populated | **Experimentally verified** |
| Delete → CDC event + tombstone | `op=d`, `after=null`, followed by a null-value tombstone | **Experimentally verified** |
| Pre-existing rows at connector start | Captured as a snapshot, `snapshot=first`/`last` markers | **Experimentally verified** |
| Kafka Connect task failure, generic | No automatic retry past the configured policy | Architectural reasoning, per `PRINCIPAL_ENGINEER_FAILURE_MATRIX.md` — not independently re-demonstrated with a connector lacking Debezium's own resilience |
| Debezium connector + source DB connection loss | Treated as internally retriable; task never surfaces as Connect-level FAILED; resumes from the exact last LSN once reachable | **Experimentally verified** |
| WAL/replication-slot retention exceeded during a long outage | Clean resume becomes impossible; a fresh snapshot is required | Architectural reasoning — not independently re-demonstrated (would require a deliberately long outage or artificially constrained WAL retention) |
| Polling JDBC source vs. log-based CDC comparison | CDC sees deletes natively, adds no source-DB query load, lower latency | Architectural reasoning (Section 12) — no polling connector was built to compare against directly |

## 17. Principal Engineer questions

**1. What does Kafka Connect's worker/task/offset model actually
separate?**
The WORKER is the JVM process (or a cluster of them, in distributed
mode) that hosts connectors and manages their lifecycle via the REST
API. A CONNECTOR is configuration plus lifecycle management; it spawns
one or more TASKS, which do the actual reading/writing. The OFFSET is
whatever position concept the connector itself defines — durably stored
by the worker, independent of what that position actually means
(Section 5).

**2. Why does a connector need `plugin.path` at all if its JAR already
ships inside the Kafka distribution?**
Because shipping inside the distribution's `libs/` directory does not
automatically put a JAR on the running worker's classpath — real,
verified: `connect-file-4.3.1.jar` exists in the image but was absent
from the actual running process's `-cp` argument (Section 4). Connect's
plugin isolation mechanism scans `plugin.path` directories (and the
worker's own base classpath, which is a curated, smaller set) — a JAR
sitting unused in `libs/` is not automatically part of either.

**3. What is a Connect "offset," precisely?**
A durable, connector-defined key-value record: the key identifies WHICH
source partition (e.g., a specific file, or a specific database), and
the value is whatever position concept that connector uses — a byte
offset for `FileStreamSourceConnector`, a WAL LSN for Debezium (Sections
5, 9). It is NOT a Kafka offset, even though it happens to be stored
IN Kafka.

**4. Does deleting a connector reset its progress?**
No — real, verified: a re-registered connector, same name and same
source partition identity, resumed from its OLD stored offset, not zero
(Section 5). This is a durability FEATURE for genuine restarts, and a
real gotcha for anyone expecting "delete means fresh start."

**5. What problem does CDC solve that a polling connector doesn't?**
Native visibility into every real change, including hard `DELETE`s a
polling connector's query would never see without a soft-delete column;
near-zero added query load on the source database (it reads the
database's OWN commit log instead of re-querying); and lower latency,
bounded by replication lag rather than a poll interval (Section 12).

**6. Why does `REPLICA IDENTITY FULL` matter for CDC updates
specifically?**
Without it, an `UPDATE`'s WAL record only carries the primary key's OLD
value, not every column's — Debezium's `before` struct would be null for
every non-key column. Real, verified: with `FULL` set, a real update's
`before.amount` and `after.amount` were both fully populated
(Section 6, 8).

**7. What is a Debezium tombstone, and why does it exist?**
A record with the deleted row's key but a `null` value, emitted
immediately after a delete event — a standard KAFKA pattern (not
Debezium-specific): compacted topics treat a `null` value as "this key
should eventually be removed entirely" (Section 8).

**8. What's the real difference between `snapshot.mode=initial` and
`no_data`?**
`initial` (the default) captures every EXISTING row as a real change
event (`snapshot=first`/`.../last`) before streaming new changes;
`no_data` captures only the table's SCHEMA at start and skips existing
rows entirely, streaming only from that point forward. Real, concrete
consequence discovered building this lab: using `initial` against a
table other tests had already left rows in produced a real test-isolation
bug (Section 15).

**9. What happens to a Debezium connector when its source database
becomes unreachable?**
Real, verified: this pinned Debezium version treats a connection loss as
an internally RETRIABLE error, with its own backoff/restart loop — the
Connect-level task state never surfaces as `FAILED` at all during the
outage. This is Debezium's OWN resilience, layered on top of, and
distinct from, Kafka Connect's generic "a permanently failed task is not
automatically retried" behavior (Section 10).

**10. Does Debezium always resume cleanly after a source database
outage?**
Only if the WAL/replication slot's retention held for the outage's
duration. Real, verified for a real (bounded) outage: Debezium resumed
from the exact last-committed LSN, correctly recognized it as
already-processed, and captured new changes cleanly (Section 11). Past
retention limits, resuming is no longer possible and a fresh snapshot
becomes necessary — a real operational risk this lab documents rather
than independently reproduces (doing so deterministically would require
either a very long outage or deliberately constrained WAL retention).

**11. Is Kafka Connect's REST API the only way to manage connectors?**
Yes — there is no separate "Kafka Connect Java client" library the way
there is for the broker or Schema Registry. The REST API is the
management interface, by design (Section 3); this lab's own
`ConnectRestClient` is a thin, direct wrapper around it, not standing in
for a missing official client.

**12. How does this WP connect to the transactional outbox pattern
(WP-12)?**
The outbox pattern's entire premise is: write a business change and an
"outbox" row in ONE database transaction, then let CDC (THIS WP's
mechanism) reliably turn that outbox row into a Kafka message — with no
application-level dual write. This WP proves the CDC mechanism works;
WP-12 is specifically about applying it to that pattern (Section 13).
