# Contributing

This repository is built incrementally, as a curriculum, not shipped as one large
drop of content. These rules exist to keep it that way and to keep the teaching
quality consistent as it grows.

## Engineering rules

1. No fake production claims. If something hasn't been run and observed, it is
   not described as a production fact.
2. No arbitrary performance numbers. Any benchmark figure must document the
   hardware and workload it came from, or it does not appear.
3. No obsolete Kafka architecture as the primary path. Labs target KRaft.
   ZooKeeper is historical context only.
4. No unnecessary abstraction. A lab that demonstrates one concept should not
   accumulate a generic framework around it.
5. No giant commits. Work lands in reviewable work packages (WPs), each scoped to
   a coherent piece of the curriculum.
6. No copied tutorial repository. External repositories listed in
   `docs/references/REFERENCE_REPOSITORIES.md` are used to validate architecture
   and terminology, never to copy code or structure wholesale.
7. No unexplained configuration. Every important configuration key introduced in
   a doc or lab must explain the trade-off it controls, not just its name and
   default.
8. Every important configuration must explain its trade-off — this is the same
   rule as #7, stated because it is the rule most tempting to skip under time
   pressure.
9. Prefer deterministic, reproducible labs. A lab whose outcome depends on
   unstated timing or environment assumptions is a bug in the lab.
10. Every failure lab needs recovery instructions. "Break it" labs must always
    include how to bring the system back to a known-good state.
11. Code must compile. No placeholder code, no `TODO` where an implementation was
    promised.
12. Tests must be meaningful — they verify the behavior the lab teaches, not just
    that code runs without throwing.
13. Documentation must match code. If a doc changes behavior described in a lab,
    the lab changes with it in the same work package.
14. Prefer official Kafka terminology over vendor-specific or informal terms.
15. Architecture decisions (ADRs, system designs, the Principal Engineer
    framework) must include the alternatives considered, not just the choice
    made.
16. Security shortcuts used for local development convenience must be clearly
    marked as local-development-only, with the production alternative named.
17. Never commit secrets. No credentials, tokens, or private keys, including in
    example configuration — use obvious placeholders instead.
18. Do not hide Kafka behavior behind frameworks. When a framework (Spring Kafka,
    Kafka Streams DSL) is introduced, the underlying Kafka mechanism it wraps is
    explained alongside it.
19. Maintain a clear separation between application behavior and Kafka behavior
    in both code and documentation, so a reader can tell which layer is
    responsible for a given guarantee or failure mode.
20. Optimize for engineering understanding, not file count or repository size.

## The learning loop every topic follows

```text
Concept → Architecture → Internal mechanism → Java implementation → Run it →
Observe it → Break it intentionally → Understand the failure → Troubleshoot it →
Fix it → Measure it → Production considerations → Architecture trade-offs →
ADR / decision → Principal Engineer questions
```

Any contribution that introduces a new non-trivial topic should aim to complete
this loop, even if later stages (ADR, Principal Engineer questions) are added in
a follow-up work package rather than all at once.

## Lab quality standard

Every lab directory must contain a `README.md` with these sections, in this
order:

```text
Objective
Prerequisites
Architecture
Concepts
Setup
Commands
Implementation
Expected output
Verification
Experiment
Failure injection
Troubleshooting
Cleanup
Production considerations
Principal Engineer questions
```

A lab without a meaningful "Failure injection" section has not taught the
distributed-systems half of the topic.

## ADR standard

Architecture Decision Records live in `adrs/` and follow:

```text
Status
Context
Problem
Decision drivers
Options considered
Decision
Consequences
Risks
Operational implications
Alternatives
Validation
```

An ADR is written **after** the lab or design exercise that produced the
evidence for it, never before. An ADR written ahead of the supporting work is an
opinion, not a decision record.

## Documentation quality

Documentation explains **why**, not just what. A configuration reference like
"`replication.factor` controls the number of replicas" is not acceptable on its
own — it must be connected to why replication exists, how followers replicate,
how the in-sync replica set affects durability, how it interacts with `acks` and
`min.insync.replicas`, what happens on broker failure, and the durability/
availability trade-off involved. Use Mermaid diagrams where GitHub rendering
supports them, especially for anything with more than two sequential steps.

## Pull requests

- Keep PRs scoped to one work package.
- Describe the objective, the changes, and — for anything touching the
  curriculum structure — what learning question the change answers.
- Do not merge your own architectural changes without review; this repository
  is a teaching artifact, and unreviewed drift compounds quickly in a curriculum.

## Security

Never commit real credentials, connection strings, or tokens. Example
configuration (e.g., `docker-compose.yml`, SASL/TLS examples) must use obviously
fake values and must state clearly when a shortcut (plaintext listeners, default
passwords) is local-development-only and not appropriate beyond that.
