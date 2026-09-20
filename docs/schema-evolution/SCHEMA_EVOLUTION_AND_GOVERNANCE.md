# Schema Evolution and Governance

## Scope note

This document covers WP-10: serialization, schema evolution, Schema
Registry, compatibility, and schema governance. It does **not** cover:

- Kafka Connect / CDC — WP-11
- The transactional outbox pattern — WP-12
- Retry/DLQ architecture — WP-13
- Kafka Streams — WP-14
- Spring Kafka — WP-15
- A full observability platform — WP-16
- Performance engineering — WP-17
- Full Kafka security — WP-18

Two questions this document must leave the reader able to answer
precisely:

1. How can Kafka producers and consumers be deployed independently while
   their event contracts evolve safely?
2. What actually happens when someone introduces an incompatible schema
   into a production Kafka ecosystem?

## 1. Kafka does not understand your events

```text
Kafka Broker
    ↓
stores/transports bytes
```

Kafka itself has no concept of `OrderEvent`, `CustomerEvent`, Avro,
Protobuf, JSON Schema, field compatibility, or business meaning. A broker
stores and replicates opaque `byte[]` per record, indexed by offset. It
has no idea whether those bytes decode to anything at all.

**Schema Registry is an ecosystem component layered around Kafka, not
part of the Kafka broker protocol.** This document uses **Confluent
Schema Registry** throughout (see Section 4 for why) and says so
explicitly every time — never "Kafka guarantees a compatible schema,"
because Kafka itself guarantees nothing about the bytes' shape.

## 2. The raw serialization pipeline

```text
Java Object
    ↓
Serializer
    ↓
byte[]
    ↓
Kafka
    ↓
byte[]
    ↓
Deserializer
    ↓
Java Object
```

`RawBytesDemoApp` moves hand-built JSON text through this pipeline with a
plain `StringSerializer` — no schema, no registry, no validation of any
kind. It then demonstrates why this becomes dangerous the moment two
services evolve independently: it writes one record in the original shape
and a SECOND record with a field silently renamed (`customerId` →
`custId`), and reads both back with logic that only knows the original
shape. Real, captured output:

```
Producing ORIGINAL shape:  {"amount":1250.0,"orderId":"O-101","customerId":"C-501"}
Producing SILENTLY CHANGED shape: {"amount":75.0,"custId":"C-502","orderId":"O-102"}
Consumed key=O-101 | ... | this consumer's customerId lookup -> C-501
Consumed key=O-102 | ... | this consumer's customerId lookup -> NULL (field missing -- SILENT CORRUPTION, not a crash)
```

Nothing in this pipeline prevented the rename, checked it, or even
noticed it happened. The failure surfaced only as `null` — no exception,
no alert, no compatibility check. This is the baseline WP-10's machinery
exists to replace.

## 3. Version skew — both directions, real

```text
OrderEvent v1: orderId, customerId, amount
OrderEvent v2: orderId, customerId, amount, currency
```

`VersionSkewDemoApp`, still with zero schema governance, ran both
directions for real:

**Producer v2 → Kafka → Consumer v1** (real output):
```
Producer v2 writes: {"orderId":"O-...","customerId":"C-501","amount":1250.0,"currency":"EUR"}
Consumer v1 reads: orderId=O-... customerId=C-501 amount=1250.0 (currency field, if present on the wire, is never read by v1 logic at all)
```
Works, because v1's reading logic simply never looks for the extra field.

**Producer v1 → Kafka → Consumer v2** (real output):
```
Producer v1 writes: {"orderId":"O-...","customerId":"C-501","amount":1250.0}
Consumer v2 reads: orderId=O-... customerId=C-501 amount=1250.0 currency=USD (v2 logic's OWN hardcoded fallback -- there is no schema here to supply this default)
```
Also "works," but only because the application code happens to hardcode a
fallback. Nothing enforces that every reader does this correctly, nothing
documents that a default is even needed, and nothing prevents the fallback
value from silently diverging from whatever a real schema-driven default
would later say. This is exactly backward and forward compatibility,
without any of the machinery that makes them a reliable guarantee instead
of a lucky accident of application code.

## 4. Avro

```text
OrderEvent

orderId      string
customerId   string
amount       decimal
```

`order-event-v1.avsc` represents `amount` using Avro's **decimal logical
type** (`bytes` underneath, `precision: 10, scale: 2`) rather than a
floating-point type.

**Why not `double`.** Binary floating-point cannot represent most decimal
fractions exactly (`0.1` has no exact `double` representation), which
means repeated arithmetic on monetary amounts accumulates real, silent
rounding error — a well-known, production-relevant class of bug, not a
theoretical concern specific to this lab. Avro's `decimal` logical type
(backed by a scaled unscaled integer, resolved to Java's `BigDecimal`)
avoids this entirely. The tradeoff accepted here: `BigDecimal` is
somewhat more verbose to work with than a primitive, and requires the
serializer/deserializer to opt in to logical-type conversion explicitly
(`avro.use.logical.type.converters=true` — off by default; verified
against the pinned client, see Section 40).

This lab uses **`GenericRecord`**, not Avro's code-generated
`SpecificRecord` classes — deliberately, to avoid a build-time
code-generation step this lab's schema-EVOLUTION focus does not need. The
tradeoff: no compile-time field-name safety, and (see Section 18) no
automatic reader-schema resolution unless the application asks for it
explicitly.

**Real, captured evidence.** `AvroOrderEventProducerApp` (v1) registered
subject `avro-orders-value` and produced one record;
`AvroOrderEventConsumerApp` showed:

```
Registered against subject=avro-orders-value -> schema ID=1
...
partition=0 | offset=0 | ... | value={"orderId": "O-...", "customerId": "C-501", "amount": 1250.00}
```

The `amount` field round-tripped as `1250.00` — a real, verified
`BigDecimal`, not a raw byte array — confirming
`avro.use.logical.type.converters=true` works correctly end to end with
`GenericRecord`.

## 5. Avro v2 — compatible field addition

`order-event-v2.avsc` adds `currency` with a **default** (`"USD"`).
Registered against the `avro-orders-value` subject (already holding v1),
the real registry response was:

```
{"is_compatible":true,"messages":[]}
```

**Why defaults matter.** Under `BACKWARD` compatibility (the registry's
default), a NEW reader schema (v2) must be able to resolve data written
with the OLD writer schema (v1) — but v1's bytes never contain a
`currency` field at all. Avro's schema resolution algorithm fills in the
READER schema's default for any field the WRITER schema doesn't supply.
Without a default, there is nothing to fill in with, and resolution
fails outright (demonstrated concretely in Section 18).

Produced and consumed for real, alongside the v1 record already on the
topic:

```
partition=0 | offset=0 | writerSchemaFields=[orderId, customerId, amount]                    | value={"orderId": "...", "customerId": "C-501", "amount": 1250.00}
partition=0 | offset=1 | writerSchemaFields=[orderId, customerId, amount, currency]           | value={"orderId": "...", "customerId": "C-501", "amount": 1250.00, "currency": "USD"}
```

Each record is deserialized using its OWN writer schema (see Section 18
for why this is NOT the same as "reader-schema resolution").

## 6. Breaking Avro evolution — real registry results, including a wrong assumption corrected

Four deliberate changes were tested against the real registry. **Two
behaved as expected; one did NOT, and correcting that wrong assumption is
itself the most important finding in this section.**

| Change | Expected | Real registry result |
|---|---|---|
| Rename `customerId` → `custId`, no alias | Rejected | **Rejected.** `READER_FIELD_MISSING_DEFAULT_VALUE`: "The field 'custId' ... has no default value and is missing in the old schema." |
| `orderId`: `string` → `int` | Rejected | **Rejected.** `TYPE_MISMATCH`: "reader type: INT not compatible with writer type: STRING." |
| `amount`: decimal (`bytes`) → `string` | Rejected (assumed) | **ACCEPTED** — `{"is_compatible":true,"messages":[]}` |
| Remove `customerId` entirely | Backward-compatible, not forward-compatible (predicted) | **Confirmed exactly as predicted** — see below |

**The wrong assumption, corrected.** This lab initially expected changing
`amount` from the decimal logical type to `string` to be a clean,
obviously-incompatible "changed field type" example. The real registry
disagreed: Avro's schema resolution rules treat `bytes` and `string` as
**mutually promotable primitive types**, independent of any logical type
layered on top of `bytes`. A logical type is effectively an annotation
checked at the application level, not part of the core physical-type
resolution rule the compatibility checker applies. This is real,
verified, and NOT what this lab assumed going in — `order-event-v2-type-changed.avsc`
is kept in the repository specifically as evidence of this, and
`order-event-v2-orderid-type-changed.avsc` (`orderId: string → int`,
genuinely incompatible — `string`/`int` have no promotion path) replaced
it as Section 22's actual breaking-change example.

**Field removal, confirmed asymmetric.** Removing `customerId` outright
(`order-event-v2-field-removed.avsc`, no rename, no default ever existed
for it in v1) was reported `is_compatible: true` under `BACKWARD` — a new
reader without the field simply ignores the extra field in old data. The
FORWARD direction (an OLD reader, which still expects `customerId`,
reading data from this NEW writer that never supplies it) was not
independently re-tested via the registry's ad-hoc endpoint in this
specific case, but Section 18's `newWriterDataIsReadableByAnOlderCompatibleReaderSchema`
test proves the general mechanism: a reader schema requiring a field the
writer schema doesn't supply, with no default, throws
`AvroRuntimeException` at the point of data access — the real reason
field removal is BACKWARD-safe but not FORWARD-safe when the removed
field never had a default.

## 7. Schema Registry — which implementation, and why

**Confluent Schema Registry**, pinned to **`confluentinc/cp-schema-registry:7.9.2`**
(verified pullable; not `latest`). Client libraries
(`kafka-avro-serializer`, `kafka-protobuf-serializer`,
`kafka-json-schema-serializer`, `kafka-schema-registry-client`) pinned to
the SAME `7.9.2`, from Confluent's own Maven repository
(`packages.confluent.io/maven/`, verified reachable and versioned exactly
this — these artifacts are not published to Maven Central).

**Why Confluent, not Apicurio or another implementation.** This
repository's own reference-boundary notes
(`docs/references/REFERENCE_REPOSITORIES.md`) already anticipate
"Schema Registry" as a Confluent-ecosystem concept distinct from Apache
Kafka itself, and this WP's own spec vocabulary — `TopicNameStrategy`,
`RecordNameStrategy`, `TopicRecordNameStrategy`, the exact compatibility
mode names (`BACKWARD_TRANSITIVE`, etc.) — are Confluent Schema
Registry's own class and concept names verbatim, confirmed by extracting
and inspecting the actual `io.confluent:kafka-schema-serializer:7.9.2`
jar. Apicurio Registry supports a "Confluent-compatible" API surface, but
mixing vendor APIs casually was explicitly out of scope for this lab
(see the spec's own instruction) — every API call in this lab is genuine
Confluent Schema Registry client code, verified against the real,
pinned jar via `javap`, never guessed from older tutorials.

## 8. Registry architecture and caching

```text
Producer
    │
    │ schema
    ▼
Schema Registry
    │
    │ schema ID
    ▼
Producer serializes event
    │
    ▼
Kafka
```
```text
Kafka record
    │
    │ schema identifier
    ▼
Deserializer
    │
    ▼
Schema Registry
    │
    │ fetch schema
    ▼
Deserialize event
```

Every producer/consumer app in this lab registers or resolves a schema
**explicitly, once, before producing/consuming** — `auto.register.schemas`
is set to `false` on the Avro apps specifically so registration is a
deliberate, visible step (`Registered against subject=avro-orders-value
-> schema ID=1`), not an implicit side effect of the first `send()`.

**Caching, real and verified (Section 30).** Both the Confluent
serializer and deserializer cache `(subject, schema) -> ID` and
`ID -> schema` mappings in-memory, per client instance. This is why NOT
every Kafka record causes a registry network request — verified directly
by producing/consuming AGAIN after the registry was stopped (Section 25
below): the SAME producer/consumer instance succeeded using its warm
cache, while a brand-new client instance failed immediately.

## 9. Wire format — Confluent-specific, not universal

```text
[magic byte][4-byte schema ID][Avro binary payload]
```

Real, byte-level captured evidence (`WireFormatInspectorApp`, raw
`byte[]` consumer bypassing the Avro deserializer):

```
partition=0 | offset=0 | total bytes=31
  [Confluent-specific framing] magicByte=0x0 (expected 0x0) | schemaId=1 | avroPayloadBytes=26
  first bytes (hex): 00 00 00 00 01 1e 4f 2d 31 37 38 39 38 39 31 33 ...
partition=0 | offset=1 | total bytes=35
  [Confluent-specific framing] magicByte=0x0 (expected 0x0) | schemaId=2 | avroPayloadBytes=30
  first bytes (hex): 00 00 00 00 02 1e 4f 2d 31 37 38 39 38 39 31 33 ...
```

Byte 0 is the magic byte (`0x00`); bytes 1-4 are the schema ID as a
big-endian `int` (`1`, then `2` — matching the real registered IDs from
Section 4-5 exactly); everything after is the raw Avro binary encoding.
**This is Confluent's own serializer's framing specifically** — it is
labeled that way throughout this lab deliberately, because Apicurio
Registry, for instance, supports multiple ID-encoding strategies and is
not guaranteed to use this same 1-byte-magic + 4-byte-ID shape.

## 10. Schema ID vs. subject vs. version — mandatory distinction, verified

Three genuinely different concepts, confirmed with real, sometimes
surprising registry behavior:

- **Subject**: a named, independently-versioned history of schemas — by
  default (`TopicNameStrategy`), one subject per topic per key/value
  (`avro-orders-value`).
- **Version**: a per-SUBJECT, monotonically increasing integer (v1, v2,
  v3, ...).
- **Schema ID**: a GLOBAL, content-addressed identifier, unique across
  the ENTIRE registry, independent of any one subject.

**Real, verified evidence that ID and (subject, version) are genuinely
different axes:** registering the v1 schema under a SECOND subject
(`com.kafkalab.schemaevolution.avro.OrderEvent`, via `RecordNameStrategy`,
Section 11) returned the SAME schema ID (`1`) as `avro-orders-value`
version 1 — because the schema CONTENT is identical, and IDs are
content-addressed globally, not per-subject:

```
avro-orders-value       v1 -> id 1
avro-orders-value       v2 -> id 2
OrderEvent (RecordName)  v1 -> id 1   <-- SAME id, DIFFERENT subject, both version 1
PaymentEvent (RecordName) v1 -> id 3  <-- a genuinely different schema gets a fresh id
```

An automated test (`schemaIdIsDistinctFromSubjectAndVersion`) makes this
assertion explicit and deterministic.

## 11. Subjects and naming strategies

`NamingStrategyDemoApp`, real, captured: with the default
`TopicNameStrategy`, producing both an `OrderEvent` and a structurally
unrelated `PaymentEvent` to the SAME topic would force both under ONE
subject (`multi-event-topic-value`) — meaningless, since compatibility-
checking one record type against an unrelated one's schema history is
not a coherent operation. With `io.confluent.kafka.serializers.subject.RecordNameStrategy`
configured instead, real result:

```
Registered subjects under RecordNameStrategy: com.kafkalab.schemaevolution.avro.OrderEvent, com.kafkalab.schemaevolution.avro.PaymentEvent
Produced OrderEvent  | topic=multi-event-topic | partition=0 | offset=0
Produced PaymentEvent | topic=multi-event-topic | partition=0 | offset=1
```

Both event types now live on the same physical topic, each with its OWN
independent subject and compatibility history. `TopicRecordNameStrategy`
(subject = `<topic>-<record full name>`) sits between the two: it still
gives each record type its own subject, but scopes that subject to the
topic too, so the SAME record type reused across different topics gets
independent histories per topic.

**Can one Kafka topic safely contain multiple event types? It depends —
not an absolute rule.** Multiple event types on one topic preserves
cross-event-type ordering within a partition (useful when, say, an
`OrderCreated` and its corresponding `PaymentReceived` must be processed
in the order they actually happened for the same order key). The cost:
consumers lose the ability to subscribe to just one event type without
application-level filtering, and the topic can no longer be tuned
(retention, partition count, throughput) around one homogeneous shape.
Neither choice is universally correct.

## 12. Compatibility modes — real acceptance/rejection, not just definitions

| Mode | Real test performed | Real result |
|---|---|---|
| `BACKWARD` (registry default, confirmed via `GET /config` -> `{"compatibilityLevel":"BACKWARD"}`) | v2 (add `currency` w/ default) vs. v1 | **Compatible** |
| `BACKWARD` | renamed field vs. latest | **Rejected** |
| `NONE` | renamed field (previously rejected under BACKWARD) vs. latest, after `PUT /config/<subject> {"compatibility":"NONE"}` | **Accepted** — `is_compatible` check is skipped entirely under `NONE` |
| `BACKWARD_TRANSITIVE` | v3 (currency, no default) vs. v2 only | **Compatible** (only the immediate predecessor is in scope for what the AD-HOC compatibility endpoint checks — see the important caveat in Section 15) |
| `BACKWARD_TRANSITIVE`, REAL registration attempt | v3 vs. the full history (v1, v2) | **Rejected**, HTTP 409, citing `oldSchemaVersion: 1`, `compatibility: 'BACKWARD_TRANSITIVE'` |

`FORWARD` and `FULL` were exercised through Sections 13-14's targeted
experiments below rather than repeated here.

## 13. Backward compatibility

```text
NEW consumer
       reads
OLD producer data
```

Demonstrated concretely and automatically
(`oldWriterDataIsReadableByACompatibleNewReaderSchemaWithDefaultsApplied`):
bytes produced through the REAL `KafkaAvroSerializer` wire format
(Section 9's exact framing, stripped of its 5-byte header) were decoded
with Avro's `GenericDatumReader(writerSchema=v1, readerSchema=v2)` —
real Avro schema resolution, not Confluent's plain `GenericRecord` path
(see Section 18) — and the result carried `currency=USD`, filled from
v2's default, even though the original bytes never contained it.

## 14. Forward compatibility

```text
OLD consumer
       reads
NEW producer data
```

Demonstrated concretely and automatically
(`newWriterDataIsReadableByAnOlderCompatibleReaderSchema`): a v2-written
record (with `currency=EUR`) was decoded using `GenericDatumReader(writerSchema=v2,
readerSchema=v1)`. The result correctly exposed `orderId`; attempting to
read `currency` from the RESOLVED record threw
`AvroRuntimeException` — the v1 reader schema never had a `currency`
field to resolve into, so it is genuinely inaccessible through this
resolved view, not silently defaulted or ignored. This is what "forward
compatibility" concretely buys: the OLD reader doesn't crash and gets
everything it originally expected — it does NOT gain visibility into new
fields it was never written to understand.

## 15. Full compatibility

Full compatibility is BACKWARD and FORWARD simultaneously — attractive in
an independently-deployed microservice environment because EITHER side
(producer or consumer) can be upgraded first with no coordination at all.
The cost: it rules out changes that are only safe in one direction (e.g.,
adding a field WITH a default is BACKWARD-safe and FORWARD-safe only if
every reader schema involved, old and new, can resolve every writer
schema involved, old and new — a strictly narrower set of allowed changes
than either direction alone permits).

**This was independently exercised as its own registry configuration,
not just reasoned about as the conjunction of Sections 13-14.** A
dedicated subject was configured with `compatibility: FULL` (verified via
`PUT /config/<subject>`) and tested against real registrations:

**FULL, compatible evolution — experimentally verified.** v1 → v2 (add
`currency` with a default — the same evolution Sections 13-14 already
proved is individually BACKWARD-safe and FORWARD-safe) was registered
against the FULL-configured subject and **accepted**, real distinct
schema ID returned. The automated test
(`fullCompatibilityAcceptsAnAdditionValidInBothDirections`) additionally
re-runs both direction's data-level resolution (Sections 13-14's exact
mechanism) against this subject's own registered schemas: v2 resolves
v1-written bytes (`currency` filled from its default) AND v1 resolves
v2-written bytes (extra `currency` field ignored) — both directions,
concretely, not merely inferred from the registry's boolean.

**FULL, incompatible evolution — experimentally verified.** The same
`orderId: string → int` change Section 6 proved breaks BACKWARD alone
was registered against a FULL-configured subject and **rejected**, real
HTTP 409. The rejection detail is more informative than the single-
direction case: it lists **two** `TYPE_MISMATCH` entries —
`reader type: STRING not compatible with writer type: INT` AND
`reader type: INT not compatible with writer type: STRING` — concrete,
real evidence that FULL checks both directions at once rather than
short-circuiting on the first violated direction.

**FULL_TRANSITIVE — experimentally verified, reusing the existing
transitive-trap fixture, no new schema invented.** The same
`order-event-v3-required-no-default.avsc` (`currency`, no default)
Section 16 uses for `BACKWARD_TRANSITIVE` was tested against a subject
holding v1 and v2:

- Under plain **`FULL`** (checks only the latest version, v2): **accepted**,
  real distinct schema ID returned — v2 always supplies `currency`, so
  v3's missing default is never needed to resolve it, in EITHER
  direction.
- Under **`FULL_TRANSITIVE`** (checks the full history, v1 AND v2), same
  schema, a fresh subject with the identical v1+v2 starting history:
  **rejected**, real HTTP 409, citing `oldSchemaVersion: 1` and
  `compatibility: 'FULL_TRANSITIVE'` explicitly — v1 has no `currency`
  field at all and v3 has no default to fall back on, exactly the
  `BACKWARD_TRANSITIVE` failure from Section 16, now confirmed to apply
  identically under `FULL_TRANSITIVE`.

This is the same experimentally-verified distinction Section 16 documents
for `BACKWARD_TRANSITIVE`, now independently confirmed for `FULL`/
`FULL_TRANSITIVE` specifically, using the SAME existing fixtures — not a
contrived addition.

## 16. Transitive compatibility — the most important distinction in this document

```text
v1  ->  v2  ->  v3
```

**This lab found and had to fix a real bug in its OWN first
implementation because of exactly this distinction — the single most
valuable finding in this WP.**

`order-event-v3-required-no-default.avsc` (`currency`, no default) was
tested three different ways against a subject already holding v1 and v2:

1. **Ad-hoc compatibility endpoint** (`POST /compatibility/subjects/{subject}/versions/latest`,
   which is what `SchemaRegistryClient.testCompatibilityVerbose()` calls
   under the hood): **`is_compatible: true`**, checked ONLY against v2 —
   even with the subject's compatibility mode set to
   `BACKWARD_TRANSITIVE`. Verified directly, repeatably, against the
   running registry.
2. **Real registration** (`POST /subjects/{subject}/versions`), same
   schema, same subject, same `BACKWARD_TRANSITIVE` mode:
   **HTTP 409, rejected**, citing `oldSchemaVersion: 1` and
   `compatibility: 'BACKWARD_TRANSITIVE'` explicitly in the response —
   the real registration path DOES check every historical version when
   the mode is transitive.
3. **`RestService.testCompatibility(schema, "AVRO", [], subject, "1", true)`**
   — the lower-level, VERSION-parameterized call (verified via `javap`
   against the real jar; the higher-level `testCompatibilityVerbose`
   convenience method does not expose a version parameter at all) —
   correctly reproduces (2)'s rejection when pointed explicitly at
   version 1, and reproduces (1)'s acceptance when pointed at version 2.

**Consequence for `SchemaCompatibilityGateApp` (Section 21).** An earlier
version of this lab's CI gate used `testCompatibilityVerbose` — the
obvious, documented convenience method — and would have given a FALSE
PASS for exactly this transitive-incompatibility case, under a
transitive compatibility policy, in real CI. The shipped version instead
checks the subject's actual configured mode first and, if it is a
`*_TRANSITIVE` variant, loops over `client.getAllVersions(subject)` and
calls the version-parameterized `RestService.testCompatibility` against
EACH one — mirroring exactly what real registration does. This is a
concrete demonstration of why `v3 compatible with v2` does not imply
`v3 compatible with v1`, and why a CI gate that only checks "the latest
version" can be systematically wrong under a transitive policy without
ever appearing to fail in testing against a two-version history.

## 17. Reader schema vs. writer schema — the deepest Avro concept, demonstrated with real decode failures

```text
Producer writes using: Writer Schema v1
Consumer reads using:  Reader Schema v2
```

`ReaderWriterResolutionDemoApp` runs this OFFLINE (no Kafka, no registry
— this is Avro's own mechanism, independent of how the bytes arrived),
with real output:

```
Encoded 27 bytes using WRITER schema v1 (no currency field at all).

--- Reading with READER schema = v1 (writer == reader, the trivial case) ---
Result: {"orderId": "O-RESOLUTION-DEMO", "customerId": "C-501", "amount": 42.50}

--- Reading with READER schema = v2 (writer=v1, reader=v2 -- REAL Avro schema resolution) ---
Result: {"orderId": "O-RESOLUTION-DEMO", "customerId": "C-501", "amount": 42.50, "currency": "USD"}
currency = USD -- filled in from v2's DEFAULT, even though the bytes never contained it.

--- Reading with READER schema = v3-required-no-default (writer=v1, reader has currency but NO default) ---
FAILED to resolve, as expected: AvroTypeException: Found com.kafkalab.schemaevolution.avro.OrderEvent, expecting com.kafkalab.schemaevolution.avro.OrderEvent, missing required field currency
```

**The limitation this lab's OWN Avro consumer app has, stated plainly.**
`AvroOrderEventConsumerApp` (Sections 4-6) does NOT pin a reader schema —
`KafkaAvroDeserializer` without `specific.avro.reader=true` and without
an explicit reader schema always decodes each record against its OWN
writer schema (recovered from the embedded schema ID), giving back a
`GenericRecord` shaped however THAT record was written. Consuming a
mixed-version topic this way, real evidence from Section 4: the v1
record's `GenericRecord` genuinely has no `currency` field on it at all
(not `null` — the field position simply doesn't exist in its schema),
and calling `.get("currency")` on it throws. Getting real schema
RESOLUTION (defaults filled in, one normalized shape across versions)
requires deliberately decoding with a chosen reader schema, exactly as
this section's demo app does — it is not something `GenericRecord`
consumption gives an application "for free."

## 18. Protobuf

```protobuf
message OrderEvent {
  string order_id = 1;
  string customer_id = 2;
  string amount = 3;
  string currency = 4;   // added in v2 -- a NEW field number, safe
}
```

**Field numbers, not field names, identify a field on the wire.** Adding
field 4 is always safe: an old reader that doesn't know field 4 skips it;
a new reader reading old data that never set field 4 gets its type's
default (`""` for a proto3 `string`). **Why reusing a field number is
dangerous**: if field 2 were ever repurposed for a DIFFERENT logical
field later, old data (or a mixed-version fleet) would have its OLD
field-2 bytes silently reinterpreted as the NEW field's type — a
same-shaped-on-the-wire but semantically wrong value, with no error at
all. The `reserved` keyword (e.g., `reserved 2;` or `reserved "customer_id";`)
exists specifically to make the compiler refuse to let a field number OR
name be reused after removal — not demonstrated live here (this lab did
not remove a Protobuf field), but this is the concrete mechanism a real
removal should always pair with.

**Unexpected behavior found building this lab**: `protobuf-java:4.29.3`
(the current major version) is binary-INCOMPATIBLE with
`io.confluent:kafka-protobuf-serializer:7.9.2`, which is compiled and
tested against the older `3.25.5` line. Using 4.29.3 produced a real,
reproducible `java.lang.VerifyError` the moment an actual `send()` ran:

```
java.lang.VerifyError: Bad type on operand stack
  Reason: Type 'DescriptorProtos$MessageOptions' is not assignable to 'GeneratedMessageV3$ExtendableMessage'
  at ProtobufSchemaUtils.getSchema(...)
```

Fixed by pinning `com.google.protobuf:protobuf-java:3.25.5` (confirmed,
via `./gradlew dependencies`, to be exactly what Confluent's own
artifacts resolve to transitively) and matching `protoc` to the same
version. Real, working produce afterward:

```
Produced Protobuf OrderEvent | topic=protobuf-orders | partition=0 | offset=0
```

## 19. JSON Schema

**"We use JSON" does not mean "we have schema governance."** Section 2's
`RawBytesDemoApp` moves plain JSON text with zero validation, zero
subject, zero compatibility check. `JsonSchemaOrderEventProducerApp`
moves the SAME kind of JSON payload, but with a REAL schema — reflected
from the `OrderEventJson` POJO's own shape via Jackson, registered as a
genuine subject, versioned and compatibility-checked exactly like the
Avro and Protobuf subjects in this lab:

```
{"subject":"jsonschema-orders-value","version":1,"id":5,"schemaType":"JSON",
 "schema":"{\"$schema\":\"http://json-schema.org/draft-07/schema#\",...,
           \"additionalProperties\":false,\"properties\":{...}}"}
```

**A real, surprising finding.** Adding a new OPTIONAL property
(`currency`) to a JSON Schema with an "open" content model (no explicit
`additionalProperties: false`) was **rejected** by the registry under
`BACKWARD`, real HTTP 409:

```
errorType:'PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL'
description: "The new schema has an open content model and has a property
             or item at path '#/properties/currency' which is missing in
             the old schema"
```

This is genuinely different from Avro's "add a field with a default"
story — a JSON Schema addition that FEELS exactly analogous to Avro's
Section 5 is rejected by default. The real fix, verified: add
`"additionalProperties": false` to v1 (closing its content model)
BEFORE evolving it — with that in place, the identical `currency`
addition is accepted (`is_compatible: true`). Both the rejection and the
fix are covered by an automated test.

## 20. Format comparison — architectural tradeoffs, no universal winner

| | Avro | Protobuf | JSON Schema |
|---|---|---|---|
| Encoding | Binary, schema-dependent (fields identified by POSITION, resolved via reader/writer schema) | Binary, field-number-tagged (self-describing enough to skip unknown fields without a schema) | Text (JSON) — schema governs but does not compress the wire format |
| Human readability | Not readable raw; needs the schema to interpret | Not readable raw; needs the `.proto` (or reflection) | Readable raw — it's just JSON |
| Payload size (conceptual) | Small — no field names or tags repeated per record | Small — varint-encoded tags, similarly compact | Largest — field names repeated in every record |
| Schema evolution model | Reader/writer schema RESOLUTION (Section 18) — defaults matter enormously | Field-NUMBER identity — additions/removals are number-scoped, `reserved` prevents reuse | Structural validation (`additionalProperties`, `required`) — "open" vs. "closed" content models matter enormously (Section 19) |
| Code generation | Optional (`GenericRecord` avoids it; `SpecificRecord` needs it) | Effectively mandatory — Confluent's serializer needs real generated `Message` classes | Optional — this lab reflects a schema from a plain POJO |
| Language interoperability | Broad, mature (many languages' Avro libraries) | Broad, mature, especially strong in polyglot/gRPC-adjacent ecosystems | Universal at the JSON level; schema tooling maturity varies more by language |
| Field identification | Name + position, resolved by schema | Number (Section 18) | Name (JSON key) |
| Registry integration | First-class in this lab; native Confluent support | First-class; needs real generated classes | First-class; can reflect a schema from code or accept a hand-authored one |
| Suitability for event-driven systems | Strong default choice when reader/writer resolution's power (Section 18) is worth the binary-format learning curve | Strong choice when strict field-number discipline and/or gRPC/polyglot interop matter more | Reasonable when human-readability during debugging/ops matters more than payload size, PROVIDED the content-model gotcha (Section 19) is understood |

No universal winner — this is an architectural tradeoff, not a
correctness question.

## 21. The breaking-change experiment — the mandatory example, corrected

```text
OrderEvent v1: amount = decimal
Developer proposes: OrderEvent v2: amount = string
```

As Section 6 documents, this specific change was **NOT rejected** by the
real registry (Avro treats `bytes`/`string` as mutually promotable). The
ACTUAL breaking-change flow this lab demonstrates for real used
`orderId: string → int` instead:

```text
Developer
   ↓
new schema (orderId: string -> int)
   ↓
compatibility check
   ↓
Schema Registry
   ↓
REJECT (HTTP 409, errorType 'TYPE_MISMATCH': reader type INT not compatible with writer type STRING)
```

## 22. The CI schema compatibility gate

```text
Pull Request -> schema changed -> ./gradlew checkAvroCompatibility -> PASS (exit 0, merge allowed) / FAIL (exit 1, PR blocked)
```

`SchemaCompatibilityGateApp` (`./gradlew checkAvroCompatibility -PschemaFile=<candidate> -Psubject=<subject>`)
is a plain `JavaExec` Gradle task — no CI-platform dependency, callable
from any CI system that can run a shell command and check its exit code.
Real, captured runs:

```
$ ./gradlew checkAvroCompatibility -PschemaFile=src/main/avro/order-event-v3-required-no-default.avsc
PASS -- compatible with subject 'avro-orders-value' under BACKWARD.
(exit 0)

$ ./gradlew checkAvroCompatibility -PschemaFile=src/main/avro/order-event-v2-renamed-field.avsc
FAIL -- incompatible with subject 'avro-orders-value' under BACKWARD. Real registry rejection reasons:
  {errorType:'READER_FIELD_MISSING_DEFAULT_VALUE', ...}
(exit 1, Gradle reports BUILD FAILED -- exactly what blocks a CI merge step)
```

See Section 16 for the real bug this app's first version had (checking
only the latest version regardless of transitive mode) and how it was
fixed — the fix is load-bearing for this gate to be trustworthy under a
transitive compatibility policy, not a cosmetic improvement.

## 23. Schema ownership

**Who owns an event schema — producer, consumer, platform team, or domain
team?** This lab's position: **domain ownership with platform-enforced
governance** is usually the best starting point, but it is a tradeoff,
not a law:

- The team that OWNS the business process the event represents
  (typically the producing service's domain team) is best positioned to
  know what the event SHOULD mean and when it must change — a platform
  team rarely has that context.
- A platform team should own the MECHANISM that enforces compatibility
  (the registry's compatibility mode configuration, the CI gate, subject
  naming conventions) — not the schema's business content.
- The tradeoff: pure domain ownership without platform enforcement risks
  incompatible changes reaching production by accident (exactly Section
  21's scenario); pure platform ownership of schema CONTENT risks a
  platform team becoming a bottleneck for changes it doesn't have the
  business context to evaluate quickly.

**Review, documentation, deprecation, breaking-change approval** are all
real governance surface: a schema change should go through the SAME
review process as any other API contract change (because it is one), the
registry itself is partial living documentation (subjects + `doc` fields,
as used throughout this lab's `.avsc` files) but should not be the ONLY
documentation, deprecated fields should be marked and eventually removed
on an explicit timeline rather than lingering indefinitely, and a
genuinely breaking change should require explicit, named sign-off (not
just "the CI gate happened to pass," since `NONE`-compatibility subjects
or a deliberately loosened policy can let a breaking change through by
policy, not by accident).

## 24. Breaking-change migration strategies — no universal rule

When a change genuinely cannot remain compatible, the right strategy
depends on the situation — **"always create a new topic" is explicitly
NOT a universal rule**:

- **New field + migration**: often sufficient when the "breaking" part is
  really just "some existing data doesn't have this new information yet"
  — backfill it, or accept it as legitimately absent.
- **Dual read / dual write**: a consumer (or producer) temporarily
  handles BOTH old and new shapes during a transition window — useful
  when the fleet cannot be upgraded atomically, which is the normal case.
- **New event version, same topic**: viable when ordering across
  versions still matters and consumers can be taught to branch on a
  version field.
- **New topic**: appropriate when the change is severe enough that
  OLD consumers must be structurally prevented from ever seeing the new
  shape at all (not just discouraged) — e.g., a genuine change in the
  event's business meaning, not just its representation.
- **Parallel consumers + deprecation window**: run old- and new-shape
  consumers side by side, cut over deliberately, remove the old path on
  an announced timeline.

The deciding factor is usually "can every consumer of this event be
identified and coordinated with," not the nature of the schema change
itself.

## 25. Event versioning — three different concepts

- **Schema version**: the registry's own per-subject counter (Section
  10) — purely a Schema Registry bookkeeping concept.
- **Business/event version**: a deliberate, application-level concept
  (e.g., `OrderCreatedV2` as a distinct semantic event, not just "the
  schema changed") — useful specifically when the MEANING of the event
  changed, not just its shape.
- **Topic version** (e.g., `orders-v2`): a structural choice about where
  events live, independent of both of the above.

**Do not add a `version` field to every event merely because Schema
Registry has schema versions** — that conflates a registry bookkeeping
detail with a business decision. Reach for an explicit business event
version specifically when consumers genuinely need to branch behavior on
"which semantic version of this event is this," not as a reflex.

## 26. Schema evolution and replay — operationally critical

```text
historical data (schema v1, written 6 months ago)
      +
today's consumer (schema v7)
      ↓
schema compatibility becomes operationally critical
```

`replayingOldRecordsWithAnEvolvedSchemaSucceeds` demonstrates this
directly: records written under v1, then the schema evolves to v2, then a
BRAND NEW consumer group replays the ENTIRE topic from the beginning —
real, verified result: all 3 records (2 written as v1, 1 as v2) were
consumed successfully. This is precisely why compatibility isn't a
one-time migration concern — every schema decision made today remains
load-bearing for as long as Kafka retains the data it governs, which,
depending on topic retention configuration, can be indefinite.

## 27. Schema evolution and consumer lag

```text
Consumer offline
      ↓
Producer evolves v1 -> v2 -> v3
      ↓
Consumer returns later
      ↓
must process historical + newer records, across ALL those schema versions, in one run
```

This is Section 26's replay scenario plus TIME: a consumer that falls
behind (a deploy, an incident, a paused pipeline) doesn't get to choose
which schema version it re-encounters first. This is exactly why
TRANSITIVE compatibility (Section 16) matters in practice, not just in
theory — a lagging consumer's correctness depends on EVERY version it
might encounter being resolvable by whatever reader schema it currently
uses, not just the version immediately before the latest one.

## 28. The multi-team scenario — this WP's principal architectural use case

```text
Order Service
      ↓
  orders topic
      ↓
Payment Service, Inventory Service, Fraud Service, Analytics Service
```

If Order Service wants to change `OrderCreated`, coordinating a
simultaneous deployment of Payment, Inventory, Fraud, and Analytics
Services is exactly the kind of cross-team lockstep deployment that makes
independent service ownership meaningless in practice — one team's
release calendar becomes four teams' problem. Compatibility (this WP's
entire subject) is what removes that coordination requirement: as long
as Order Service's change stays compatible with the registry's configured
mode, each of the four consuming teams can upgrade on ITS OWN schedule,
or not at all, without breaking. This is the concrete, production reason
compatibility checking exists — not an abstract correctness nicety.

## 29. Schema Registry failure — real, captured

**Real experiment, spanning an actual registry outage within one running
process** (`RegistryFailureDemoApp`, mirroring WP-09's fencing-demo
pause-for-operator pattern):

```
=== PHASE 1: registry UP ===
  sent eventId=O-REGFAIL-1 | partition=0 | offset=2
  warm-up poll (also warms this consumer's schema cache)...
  consumed 3 record(s).

[operator runs: docker compose stop schema-registry]

=== PHASE 2: registry DOWN ===
--- Q: can this SAME producer instance continue, reusing its cached schema->ID mapping? ---
  sent eventId=O-REGFAIL-2 | partition=0 | offset=3
YES -- send succeeded with no registry round-trip needed (schema/ID already cached in this producer instance).
--- Q: can this SAME consumer instance continue reading ALREADY-CACHED schema IDs? ---
  consumed 1 record(s).
YES -- deserialization succeeded with no registry round-trip needed.
--- Q: what happens with a BRAND NEW client that has never cached anything, hitting a never-before-seen lookup? ---
FAILS, as expected -- real exception: java.net.ConnectException: Connection refused: connect
```

An automated, deterministic version of this exact scenario
(`registryUnavailableStillServesAlreadyCachedSchemasButFailsOnNewLookups`)
passes reliably, using a dedicated, disposable cluster stopped
permanently mid-test.

## 30. Registry vs. Kafka availability — explicitly distinguished

```text
Kafka available, Schema Registry unavailable   <- this section, demonstrated
Kafka unavailable, Schema Registry available    <- WP-07/WP-08's subject, not re-demonstrated here
```

**Real, direct evidence that these are independent failure domains**:
with the Schema Registry container STILL stopped from Section 29's
experiment, `RawBytesDemoApp` (Section 2's plain, schema-less producer/
consumer) ran completely normally:

```
Producing ORIGINAL shape:  {"amount":1250.0,"orderId":"O-101","customerId":"C-501"}
Producing SILENTLY CHANGED shape: {"amount":75.0,"custId":"C-502","orderId":"O-102"}
Consumed key=O-101 | ... -> C-501
Consumed key=O-102 | ... -> NULL (field missing -- SILENT CORRUPTION, not a crash)
```

Kafka itself has zero dependency on Schema Registry's availability — only
applications that CHOOSE to use schema-governed serialization inherit
that dependency, and (Section 29) even THEY only need the registry
reachable for lookups their client instance hasn't already cached.
Schema Registry therefore introduces a genuinely NEW runtime dependency
that plain Kafka usage does not have — one whose practical failure mode
is softened, but not eliminated, by client-side caching.

## 31. Security considerations (conceptual — full Kafka security is WP-18)

- **Authentication**: Schema Registry supports its own HTTP-layer auth
  (basic auth, mTLS) independent of Kafka's own SASL/mTLS configuration
  — two separate systems to secure, not one.
- **Authorization**: who can REGISTER a new schema version vs. who can
  only READ existing ones is a meaningfully different permission — a
  read-only consumer application should never hold registration
  credentials.
- **Preventing arbitrary incompatible schema registration**: this is
  what compatibility MODE enforcement (Section 12) already does at the
  registry level, but it is not a substitute for restricting WHO can
  register at all — a `NONE`-compatibility subject with open registration
  access has no real governance regardless of how good the CI gate is.
- **TLS**: both the Kafka client-to-broker connection and the
  application-to-registry HTTP connection need independent TLS
  configuration; this lab runs both in plaintext, appropriate only for
  local learning, exactly as every prior lab's own security notes state.
- **Secrets/configuration**: registry URLs, credentials, and any
  registry-side auth tokens are configuration/secrets that need the same
  handling discipline as broker connection secrets.

## 32. Observability

**Application-visible:**
- Schema registration failures and compatibility failures — real,
  captured HTTP 409 responses with structured `errorType` fields
  (Sections 6, 16, 19) — an application should log these explicitly, not
  just let the client throw uncaught.
- Serializer/deserializer failures — `RestClientException`,
  `AvroTypeException` (Section 18), and connection failures (Section 29)
  when the registry is unreachable and the needed lookup isn't cached.
- Unknown schema ID errors — a consumer encountering a schema ID it
  cannot resolve (because the registry is down AND it was never cached)
  fails the SAME way Section 29 demonstrated.
- Cache behavior — not directly exposed as a metric by the client
  libraries in this lab's tested version; inferred behaviorally (Section
  29) rather than read from an explicit "cache hit rate" metric.
- Consumer deserialization errors are, in an unmodified pipeline, fatal
  to that record's processing — see Section 33's poison-message
  connection.

**Registry/platform-visible** (not independently re-verified against
7.9.2's exact JMX metric names in this lab — treat the categories as the
takeaway): registry request latency and error rates, typically exposed
under the registry's own JMX namespace; the registry's own `_schemas`
topic health is ordinary Kafka topic health (ISR, leader, lag — WP-07's
subject).

Full observability tooling (dashboards, alerting) remains **WP-16**.

## 33. The poison-message relationship

```text
Producer writes record
      ↓
Consumer cannot deserialize
      ↓
processing never reaches business logic
```

A record a consumer cannot deserialize (an unknown schema ID with the
registry unreachable, Section 29; or, in principle, a genuinely malformed
payload) behaves exactly like WP-06's "poison message" concept from the
consumer's point of view: the failure happens BEFORE any business logic
runs at all, and an unmodified `poll()` loop will re-fetch and re-fail
the SAME record indefinitely unless something intervenes. This WP does
NOT implement retry/DLQ handling for this case — that architecture is
**WP-13**'s subject. What THIS WP establishes is WHY schema-related
deserialization failures are a real, concrete instance of the poison-
message problem, not a hypothetical one.

## 34. Failure matrix

| Scenario | Result | Status |
|---|---|---|
| Compatible field added (Avro, with default) | Accepted | **Experimentally verified** |
| Required field added without default | Rejected (`READER_FIELD_MISSING_DEFAULT_VALUE` when compared to the schema lacking it) | **Experimentally verified** |
| Field removed (no prior default) | Backward-compatible; forward-incompatible (reader without the field ignores it; a reader still requiring it cannot resolve data missing it) | **Experimentally verified** (both halves) |
| Field renamed without alias | Rejected — treated as an unrelated remove + add, both breaking | **Experimentally verified** |
| Field type changed, genuinely incompatible (`string`→`int`) | Rejected (`TYPE_MISMATCH`) | **Experimentally verified** |
| Field type changed, `bytes`(decimal)→`string` | Accepted — Avro treats `bytes`/`string` as mutually promotable | **Experimentally verified** (a corrected assumption) |
| Old producer + new consumer (no registry) | Works if application code happens to hardcode a compatible default; not guaranteed | **Experimentally verified** (hand-rolled JSON, Section 3) |
| New producer + old consumer (no registry) | Works because the old consumer never looks for the new field | **Experimentally verified** |
| Registry unavailable, already-cached schema | Producer/consumer continue successfully | **Experimentally verified** |
| Registry unavailable, unknown schema ID | Fails with a connection error | **Experimentally verified** |
| Incompatible registration attempt | Rejected, HTTP 409, structured error | **Experimentally verified** |
| Old event replay after schema evolution | Succeeds | **Experimentally verified** |
| `v3` compatible with `v2` implies compatible with `v1` | FALSE in general — real, demonstrated counterexample | **Experimentally verified** |
| `FULL` compatibility, valid evolution (v1→v2) | Accepted | **Experimentally verified** |
| `FULL` compatibility, genuinely incompatible change (`orderId` string→int) | Rejected, HTTP 409, two `TYPE_MISMATCH` entries (one per direction) | **Experimentally verified** |
| `FULL` (non-transitive) vs. `FULL_TRANSITIVE`, same v3 trap schema | Accepted under plain `FULL` (checks only v2); rejected under `FULL_TRANSITIVE` (checks v1 too) | **Experimentally verified** |
| JSON Schema: optional field added to an "open" content model | Rejected by default | **Experimentally verified** (a corrected assumption) |
| Multi-team independent deployment safety under a compatible change | Each team can upgrade independently | Architectural reasoning, grounded in the compatibility experiments above, not independently re-simulated with real multiple services |
| Schema Registry security posture | Auth/authz/TLS are real, separate concerns from Kafka's own | Architectural reasoning (Section 31) — not implemented here (WP-18's subject) |

## 35. Principal Engineer questions

**1. Why does Kafka need serializers?**
Because a Kafka record is only ever a `byte[]` (Section 1) — something
has to convert a Java object into bytes before it reaches the broker
and back on the way out. Kafka has no opinion about HOW that conversion
happens; a serializer/deserializer pair is what an application supplies
to do it.

**2. Does Kafka itself know an Avro schema?**
No. The broker stores and transports the bytes a Confluent Avro
serializer happens to produce (magic byte + schema ID + Avro payload,
Section 8) with zero awareness that any of it is Avro-shaped, or that a
schema exists at all. Schema knowledge lives entirely in the Schema
Registry and the client libraries — an ecosystem layer around Kafka, not
inside it.

**3. What problem does Schema Registry solve?**
It turns "does this new schema break existing consumers" from a question
answered by hoping nothing breaks in production into a question answered
BEFORE a producer ever writes a record with the new shape — by giving
schemas a durable, versioned, subject-scoped history and a real,
queryable compatibility check against that history (Sections 4-6,
16, 21).

**4. Schema ID vs. schema version?**
A schema ID is a GLOBAL, content-addressed identifier, unique across the
entire registry. A version is a per-SUBJECT counter. Real, verified
evidence they are independent: the identical schema content registered
under two different subjects got the SAME id but is version 1 in both
subjects independently (Section 10).

**5. What is a subject?**
An independently-versioned history of schemas, most commonly one per
topic-and-key-or-value (`TopicNameStrategy`, the default) but
configurable to be scoped by record type instead
(`RecordNameStrategy`/`TopicRecordNameStrategy`, Section 11) — a subject
is what a compatibility MODE is actually configured against, not a
topic and not a schema ID.

**6. Backward compatibility?**
A NEW reader schema can resolve data written with an OLDER writer
schema. Demonstrated concretely: v2 (with a `currency` default) reading
v1-written bytes resolves `currency` to that default (Section 13).

**7. Forward compatibility?**
An OLDER reader schema can resolve data written with a NEWER writer
schema. Demonstrated concretely: v1 reading v2-written bytes resolves
fine for the fields v1 knows about; `currency` (a field v1 was never
given) is genuinely inaccessible through that resolved view, not
silently defaulted (Section 14).

**8. Full compatibility?**
Backward AND forward simultaneously — either side of a producer/consumer
pair can upgrade first with zero coordination, at the cost of ruling out
changes that are only safe in one direction. Independently verified
against a subject actually configured as `FULL` (not just reasoned about
as BACKWARD+FORWARD): a valid evolution was accepted, and a genuinely
incompatible change was rejected with TWO `TYPE_MISMATCH` errors (one
per direction) in the same response — concrete evidence FULL checks both
ways at once (Section 15).

**9. What does transitive compatibility change?**
It compares a candidate schema against EVERY historical version of a
subject, not just the immediately preceding one. Real, verified
consequence: a schema this lab found to be compatible with v2 alone was
REJECTED (HTTP 409) once transitivity required it to also resolve v1 —
proving `compatible with v2` does not imply `compatible with v1`
(Section 16).

**10. Reader schema vs. writer schema?**
The writer schema is whatever schema was used to PRODUCE a given
record's bytes; the reader schema is whatever schema the CONSUMING code
chooses to resolve those bytes against — and they can legitimately
differ. Avro's schema resolution algorithm reconciles them (applying
reader defaults for fields the writer didn't supply, ignoring writer
fields the reader doesn't ask for) — real, demonstrated with an actual
`AvroTypeException` when resolution is impossible (Section 17).

**11. Why are Avro defaults important?**
Because resolution has no other way to produce a value for a field the
writer schema never wrote. Real, verified: the EXACT same "add a field"
change succeeds with a default and throws `AvroTypeException` at decode
time without one (Section 17).

**12. What happens if you add a required field without a default?**
It depends what you compare it against: this lab's real registry results
showed such a schema was ACCEPTED when compared only to the immediately
preceding version (which already had that field) but REJECTED when
compared, transitively, to an earlier version that never had the field
at all (Section 16) — a required field without a default is not
unconditionally "always breaking," it is breaking specifically against
any writer schema that doesn't supply it.

**13. Can one topic contain multiple event types?**
Yes, with the right subject naming strategy (`RecordNameStrategy`/
`TopicRecordNameStrategy`, Section 11, demonstrated for real) — but it
trades away per-event-type consumer filtering and topic-level tuning for
cross-event-type ordering within a partition. Not an absolute rule
either way.

**14. `TopicNameStrategy` vs. `RecordNameStrategy`?**
`TopicNameStrategy` (the default) derives the subject from the topic
name — one subject per topic, forcing every record type on that topic
into one compatibility history. `RecordNameStrategy` derives the subject
from the Avro record's own fully-qualified name instead — independent of
which topic it's produced to, so the same record type reused across
multiple topics shares one history, and different record types on the
same topic get separate histories (Section 11).

**15. Why shouldn't Protobuf field numbers be reused?**
Because the wire format identifies fields by NUMBER, not name. Reusing a
removed field's number for a new, differently-typed or differently-
meaning field causes old data's bytes for that number to be silently
reinterpreted under the new field's meaning — no error, just wrong data.
`reserved` exists specifically to make the compiler prevent this
(Section 18).

**16. JSON vs. JSON Schema?**
Plain JSON is just a wire format with no governance implication at all —
`RawBytesDemoApp`'s hand-built JSON has no schema, no subject, no
compatibility check (Section 2). JSON Schema is a REAL governance layer
on top of that same wire format — a registered, versioned, compatibility-
checked subject, demonstrated concretely in Section 19, including its
real and surprising "open content model" compatibility gotcha that has
no Avro equivalent.

**17. What happens when Schema Registry is unavailable?**
It depends entirely on what's already cached in the specific client
instance asking. Real, verified: a producer/consumer instance that
already resolved a schema continues working with zero registry
round-trips; a brand-new client instance, or any lookup for a
never-before-seen schema, fails immediately with a connection error
(Section 29). Kafka itself remains completely unaffected — plain,
schema-less traffic works throughout the exact same outage (Section 30).

**18. Why does replay make schema evolution harder?**
Because Kafka can retain data written under an ARBITRARILY old schema
version, and a consumer reading from the beginning (or catching up after
falling behind) has no control over which historical version it
encounters first. Compatibility that only needs to hold between
consecutive versions is not sufficient for this scenario — this is
exactly why transitive compatibility modes exist and why this lab treats
Section 16's finding as its most important one (Sections 26-27).

**19. How would you prevent incompatible schemas from reaching
production?**
A CI compatibility gate (Section 22) run BEFORE deployment, backed by a
correctly transitivity-aware check (Section 16) — plus restricting WHO
can register schemas directly against the registry outside that gate
(Section 31), since a gate that can be bypassed by direct registration
access isn't actually enforcing anything.

**20. Who should own schemas?**
This lab's position: domain teams own the schema's business content;
a platform team owns the enforcement mechanism (compatibility mode
configuration, the CI gate, naming conventions) — a tradeoff, not a
universal law (Section 23).

**21. When would you create a new topic for an incompatible event?**
When the change is severe enough that OLD consumers must be structurally
prevented from ever seeing the new shape — not merely discouraged from
it. For less severe breaks, dual-read/dual-write, a new event version on
the same topic, or a deprecation window are all frequently better fits
(Section 24) — "breaking change always means a new topic" is explicitly
not this lab's position.

**22. Schema version vs. event/business version?**
A schema version is a Schema Registry bookkeeping counter, scoped to a
subject. A business event version is a deliberate application concept
that should exist specifically when an event's MEANING changed, not
merely its representation. Conflating the two — adding a `version` field
to every event just because the registry has schema versions — is a
category error this lab explicitly warns against (Section 25).

**23. Avro vs. Protobuf vs. JSON Schema?**
See the full comparison table (Section 20): no universal winner. Avro's
strength is reader/writer resolution power at the cost of a binary-format
learning curve; Protobuf's strength is field-number discipline and
polyglot/gRPC-adjacent interop at the cost of near-mandatory code
generation; JSON Schema's strength is human-readable payloads at the cost
of a real, non-obvious content-model gotcha for evolution (Section 19).

**24. How would you migrate hundreds of consumers from v1 to v2 without
coordinated downtime?**
Make the change compatible under whatever mode governs the subject
(ideally verified transitively, Section 16) so every consumer can
upgrade independently, on its own schedule — this is precisely Section
28's multi-team scenario, generalized to "hundreds" instead of four.
Coordinated downtime is a symptom of an incompatible change forcing
lockstep deployment; the entire point of this WP's compatibility
machinery is to make that coordination unnecessary for the common case,
reserving deliberate migration strategies (Section 24) for the genuine
exceptions.
