# Kafka Security (WP-18)

## Prerequisites

- WP-04/WP-05 (partitioning, consumer groups) — ACL scoping (Section 5)
  operates on the same topic/partition/group concepts those WPs
  establish.
- Every platform/ directory's own "PLAINTEXT-only, local dev" security
  note — this WP is the one that finally builds the real alternative
  those notes all pointed toward.

## 1. Why a new, single-node environment

`platform/kafka-security/` is deliberately separate from, and simpler
than, the WP-07 3-broker cluster: a single combined broker+controller
node. TLS handshakes, SASL authentication, and ACL enforcement are all
properties of ONE broker's configuration — they don't need multi-broker
replication to demonstrate, and a single cert/keystore avoids also
having to secure inter-broker replication traffic, a real but separate
concern this WP scopes out deliberately (see Section 9).

## 2. A real, self-signed CA — and a real Windows/Docker path finding

`certs/generate-certs.sh` runs `openssl` and `keytool` INSIDE the
pinned `apache/kafka:4.3.1` image (both confirmed present there) to
generate a real CA, sign a real broker certificate, and build real
PKCS12 keystores/truststores — LOCAL-DEVELOPMENT-ONLY, explicitly: a
real deployment gets its certificate from an actual CA or a
cert-management system, never a throwaway script.

Two real, unglamorous findings building this ONE script:

1. **A Windows/Git-Bash Docker path bug.** `docker run -v host:/container`
   needs the CONTAINER side (`/certs`) left untouched by path
   translation, but disabling that translation (`MSYS_NO_PATHCONV=1`)
   ALSO stops the HOST side from being translated — a plain
   `/d/Kafka/...` POSIX path then silently resolves to the WRONG
   location. Confirmed the hard way: a fresh `docker run -v
   "$(pwd):/certs"` could read the generated files back, but a plain
   host `ls` in that exact directory showed nothing. Fixed with
   `cygpath -w`, giving Docker Desktop the real, explicit Windows path
   instead of relying on (now-disabled) automatic translation.
2. **A container permission mismatch.** The image's default user
   (`appuser`, UID 1000) couldn't write to the bind-mounted directory at
   all (`Permission denied`) — fixed with `--user root` for this
   generation step specifically (a throwaway container, not the actual
   broker), with a `chmod 644` at the end so the broker container's own
   `appuser` can still read the files back.

## 3. TLS hostname verification checks the SAN, not just the CN

A first version of the broker certificate had only `CN=security-broker`
— real handshakes from clients connecting via `localhost` (the
published host port) failed outright, with only `SSL handshake failed`
as a symptom, no further detail. Modern TLS hostname verification
checks the certificate's **SAN** (Subject Alternative Name) extension,
not the CN. The fix required BOTH the keystore generation AND the CSR
request to explicitly request the SAN (`keytool` does not carry it
forward automatically), AND an explicit `-extfile` on the CA's signing
step (`openssl x509 -req` does not copy extensions from the CSR by
default) — three separate places a SAN can silently go missing, all
three fixed in `generate-certs.sh`. The real SAN this lab's broker
certificate carries: `security-broker`, `localhost`, and `127.0.0.1` —
every way this lab's own clients actually connect (in-network and
host-side).

## 4. SCRAM-SHA-512, and why not PLAIN

`sasl.enabled.mechanisms=SCRAM-SHA-512` — deliberately not
`PLAIN`. PLAIN sends credentials as-is over whatever transport carries
it (TLS still protects the WIRE here, since every client-facing
listener is SASL_SSL, but that's ONE layer, not two). SCRAM's real
challenge-response design means the broker never receives, and never
needs to store, the plaintext password at all — an independent
protection even if TLS were somehow compromised. Real credentials are
provisioned via `kafka-storage.sh format --add-scram
'SCRAM-SHA-512=[name=admin,password=admin-secret]'` at storage-format
time (`entrypoint.sh`, a fully custom entrypoint overriding the image's
own docker wrapper — the SAME pattern WP-11's Connect worker already
established — since the docker wrapper's `KafkaDockerWrapper setup`
step has no env-var passthrough for `--add-scram`).

## 5. Real ACL enforcement: deny, grant, and a real ordering finding

Every one of this lab's ACL experiments is real, not simulated:

- `aReaderWithNoGrantedAclsIsDeniedProducing` — a real, authenticated,
  non-super-user identity with ZERO ACLs gets a real
  `TopicAuthorizationException` trying to produce.
- `writeAclAloneAllowsProducingButNotConsuming` — granting ONLY
  `WRITE`+`DESCRIBE` lets that same identity produce, but consuming
  (which additionally needs `READ` on the topic AND `READ` on the
  consumer GROUP) still fails. **A real finding**: the failure surfaces
  as `GroupAuthorizationException`, not `TopicAuthorizationException` —
  Kafka checks GROUP authorization (joining/fetching as this specific
  consumer group) BEFORE it ever reaches topic-level READ
  authorization, so with both missing, the group check is the one that
  fires first.
- `grantingReadAndGroupAclsThenAllowsConsuming` — granting BOTH the
  topic `READ` and the group `READ` (two separate ACL bindings, two
  separate resource types) finally lets the same identity consume.
- `revokingAnAclDeniesFurtherAccess` — a real ACL, granted then
  revoked via the Admin API, denies a subsequent produce attempt —
  confirmed with a real, bounded re-poll (ACL changes propagate through
  the cluster metadata log, not instantaneously).

## 6. Two client-facing listeners, and a real advertised-listener finding

A first version of this environment used ONE client-facing listener,
advertised as `localhost:9093` (the container-internal port). Every
host-side test call TIMED OUT with `Timed out waiting for a node
assignment` — a real, confirmed instance of the exact class of bug
`platform/kafka-cluster/README.md`'s own "Why three different host
ports" section already names: a client's FIRST connection (bootstrap)
reaches the broker fine via whatever address is dialed, but every
SUBSEQUENT, metadata-directed reconnect (produce, topic management,
literally everything past the initial handshake) uses the ADVERTISED
address — and `localhost:9093` is not reachable from the host at all
(only the published port, 9096, mapped to a DIFFERENT container port,
9094, is). The fix: TWO real listeners, `INTERNAL` (advertised as
`security-broker:9093`, for in-container/docker-network clients like
this environment's own healthcheck) and `CLIENT` (advertised as
`localhost:9096`, for this lab's own host-side Java tests) — the same
two-listener shape `platform/kafka-cluster/` already uses for exactly
this reason, now hit for real in a SASL_SSL context too.

## 7. Enabling the authorizer also gates the broker's own internal traffic

A real, non-obvious finding: `StandardAuthorizer` applies to EVERY
listener, including the PLAINTEXT-only, never-externally-reachable
`CONTROLLER` listener. The broker's own self-registration with the
controller (`BROKER_REGISTRATION`, `CONTROLLER_REGISTRATION`) runs as
`User:ANONYMOUS` on that listener (PLAINTEXT has no authentication at
all) — without `User:ANONYMOUS` ALSO listed in `super.users`, the
broker's own internal bootstrap traffic is denied by the very
authorizer meant to protect CLIENT traffic (confirmed: a real
`ClusterAuthorizationException`, `"needs CLUSTER_ACTION permission"`,
the broker never finishing startup). Safe specifically because the
`CONTROLLER` listener is never published to the host or reachable
outside this compose network — `ANONYMOUS` here means "this single
process talking to itself," not an exposed attack surface.

## 8. Secrets: what this lab does and does not do

Every password in this environment (`admin-secret`, `reader-secret`,
`lab-security-changeit`) is a REAL, working credential — and every one
of them is also checked into this repository, in plain text, on
purpose: this is a learning lab, not a production system, and the
whole point is that a reader can run every experiment exactly as
written without provisioning their own secrets infrastructure first.
**None of this is how a real deployment manages secrets.** A real
system:

- Never checks in a keystore password, a SCRAM password, or a CA
  private key — these come from a secrets manager (Vault, AWS Secrets
  Manager, Kubernetes Secrets) at deploy time, injected as environment
  variables or mounted files the application never logs or persists.
- Rotates SCRAM credentials and TLS certificates on a real schedule,
  not "whenever someone remembers" — this lab's certs are valid for
  3650 days specifically BECAUSE rotation is out of scope here.
- Uses a REAL CA (internal or public), not a self-signed one a lab
  script generates fresh.

## 9. Lab limitations

- **Mutual TLS (mTLS) is named, not built.** `ssl.client.auth=none` —
  the broker verifies its OWN certificate to clients, but clients don't
  present one back. SASL/SCRAM already handles client authentication
  here, a real, common production choice; mTLS would add a SECOND,
  independent authentication layer (useful for defense-in-depth, or
  for machine-to-machine traffic where SASL credentials are less
  natural), named as an alternative architecture rather than built.
- **The `CONTROLLER` listener stays PLAINTEXT** — a real, deliberate
  single-node scope choice (Section 7); a multi-node production
  deployment would secure inter-controller Raft traffic too, typically
  with mTLS specifically (SASL/SCRAM is less natural for
  broker-to-broker system traffic than for external clients).
- **No credential rotation experiment** — Section 8 names it as a real
  production requirement this lab doesn't build; rotating a SCRAM
  credential or a TLS certificate live, without downtime, is real,
  substantial additional scope.
- **No quotas / rate limiting** — Kafka's own client-quota mechanism
  (a real, separate authorization-adjacent concern: not "can this
  principal do X" but "how MUCH of X can this principal do") is not
  covered by this WP.

## 10. Principal Engineer questions

**1. Section 3's SAN finding needed fixing in THREE separate places
(keystore generation, CSR, and CA signing) — why does TLS certificate
tooling make this so easy to get wrong?** Each step in a real CA
signing workflow is, by design, a SEPARATE trust boundary — the CSR is
what the REQUESTER asks for, and the CA's signing step is what the CA
actually GRANTS, and a CA is not obligated to honor everything a CSR
requests (this is a real security property, not an accident: a CA that
blindly copied every CSR extension would be a much weaker root of
trust). The tooling reflects that real distinction; the cost is that a
SAN request has to be stated explicitly and consistently at each step,
which is exactly the kind of detail that's easy to silently drop.

**2. Section 5 shows a real check ORDER (group before topic) — why
does that ordering matter beyond just picking the right exception
type in a test?** In a REAL incident, the specific exception a client
reports tells an operator WHICH ACL to check first — a
`GroupAuthorizationException` means "look at the consumer group's ACLs
first," even if the topic ACL also turns out to be missing. Getting
the ordering wrong in your own mental model means debugging the WRONG
ACL first when a real access-denied report comes in.

**3. Section 6's two-listener fix is the SAME shape
`platform/kafka-cluster/` already uses — why did this WP need to
rediscover it rather than just copying that pattern from the start?**
A fair question, and the honest answer is that this WP's FIRST attempt
didn't, and paid the real cost (a genuinely confusing timeout failure)
of rediscovering why that pattern exists — which is itself the more
durable lesson: the PATTERN's existence in `platform/kafka-cluster/`
was already documented, but the REASON it's necessary is something
this WP had to hit directly to internalize, not just read about.

**4. Section 7's fix adds `User:ANONYMOUS` to `super.users` — isn't
"ANONYMOUS is a super user" exactly the kind of misconfiguration a
security lab should be warning against?** Yes, in general — and this
WP names the SPECIFIC condition that makes it safe here: the listener
ANONYMOUS authenticates on is NEVER reachable from outside this
compose network. The real principle isn't "ANONYMOUS as super user is
always wrong," it's "granting broad trust to an identity is only as
safe as the boundary around who can ACT as that identity" — exactly
the same reasoning that makes `ssl.client.auth=none` (Section 9)
defensible given SASL already authenticates clients on the listeners
that matter.

**5. Why does this lab use SCRAM-SHA-512 specifically, rather than
SCRAM-SHA-256?** Both are real, supported SCRAM mechanisms; 512 is the
stronger hash, and Kafka's own defaults and most current production
guidance favor it when there's no specific compatibility reason to use
256 (an older client library lacking 512 support, for instance) — a
real, deliberate choice here, not an arbitrary one, though 256 would
work identically for every experiment this lab runs.

**6. This lab's revoke test (Section 5) waits with a bounded poll for
the ACL change to take effect — what's actually causing that delay?**
ACL changes are real records appended to the cluster's own metadata log
(the same `__cluster_metadata` topic every other piece of cluster state
lives in, KRaft's own mechanism) — every broker's authorizer has to
receive and apply that record before it affects local authorization
decisions. The delay is real, not a test artifact, and is the same
class of propagation delay any KRaft metadata change has, just applied
to ACLs specifically here.

**7. Section 8 says secrets here are deliberately checked in — when,
if ever, would checking in even a LAB credential be a real, not just
low-risk, problem?** The moment this environment's credentials
overlap with anything real — reusing a real password pattern an
engineer might reuse elsewhere out of habit, or (worse) if this
environment were ever pointed at a real, non-lab network reachable by
anyone besides the person running it locally. Neither is true here
(single-node, host-network-only, throwaway self-signed CA), which is
precisely the boundary that makes "checked-in lab secrets" a reasonable
choice FOR THIS REPOSITORY specifically, not a general endorsement of
checking in credentials.
