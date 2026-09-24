# ADR 0003: SASL/SCRAM-SHA-512 + SASL_SSL, not mutual TLS, for the security lab

## Status

Accepted (WP-18; documented retrospectively in WP-20)

## Context

WP-18 needed real client authentication alongside encryption in
transit. Kafka supports both SASL mechanisms (username/password-style
credentials, verified server-side) and mutual TLS (client certificates)
for authenticating clients, and the two are not mutually exclusive in
production.

## Problem

Which authentication mechanism should the lab demonstrate as its
primary path?

## Decision drivers

- The lab needed to teach a credential model most engineers already
  have an intuition for (centrally-issued, rotatable credentials)
  before layering cluster-specific PKI concerns on top.
- The lab's authorization mechanism (`StandardAuthorizer`, ADR 0002)
  needed to be demonstrable independent of the transport-security
  choice — coupling the two would blur which guarantee comes from
  which layer.
- `CONTRIBUTING.md` #16 requires any local-dev shortcut (a self-signed
  CA, checked-in lab credentials) to be marked as such with the
  production alternative named.

## Options considered

1. **Mutual TLS (client certificates).** A legitimate, arguably stronger
   production option — not chosen for this lab specifically because
   provisioning and rotating per-client certificates is a heavier
   local-lab setup than SCRAM credentials for the same pedagogical
   payoff (real grant/deny/revoke via `StandardAuthorizer`).
2. **SASL/SCRAM-SHA-512 over SASL_SSL.** Chosen — separates the
   authentication mechanism (SCRAM, a salted-hash credential check)
   cleanly from the transport-encryption mechanism (TLS), and maps more
   directly onto how most organizations already manage human/service
   identities (a central identity system issuing credentials) than
   per-client certificate issuance does.

## Decision

`lab-17-security` uses SASL/SCRAM-SHA-512 authentication over
SASL_SSL, with a real self-signed CA and CA-signed broker certificate
(real SANs) for transport encryption — both explicitly marked
local-development-only.

## Consequences

- The lab cleanly demonstrates three separable concerns: encryption in
  transit (TLS), authentication (SCRAM), and authorization
  (`StandardAuthorizer`) — a learner can reason about each
  independently.
- mTLS's specific operational model (certificate issuance, rotation,
  revocation-list or short-lived-cert strategy) is not demonstrated by
  this lab and would need separate treatment if an organization's
  actual environment relies on it.

## Risks

A learner might incorrectly generalize "SCRAM is the standard choice"
rather than "SCRAM was the pedagogically clearer choice for this lab" —
mitigated by stating the trade-off explicitly here and in the
architecture-decision-framework doc (Decision 4).

## Operational implications

Production deployment of either mechanism requires infrastructure this
lab's self-signed CA and checked-in credentials explicitly do not
provide — a real CA (internal or public) and a real secrets manager,
named directly in the lab's own README per `CONTRIBUTING.md` #16.

## Alternatives

See Options considered above.

## Validation

`lab-17-security` demonstrates a real client failing to connect without
valid SCRAM credentials, and succeeding once configured with them,
against a real TLS-encrypted listener — not simulated.
