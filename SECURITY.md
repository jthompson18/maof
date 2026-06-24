# Security Policy

MAOF ships governance-critical machinery: registry signing, policy-as-code, RBAC, multi-tenancy,
and idempotent side effects. We take security reports seriously and appreciate responsible disclosure.

## Supported versions

| Version | Supported |
| ------- | --------- |
| 1.x     | Yes       |
| < 1.0   | No        |

Security fixes land on the latest `1.x` release.

## Hardening

The defaults are tuned for offline local development. Production deployments must override them —
message signing, non-default credentials, DB pool sizing, and approval-service ingress. See the
**Production hardening** section of [`docs/deployment.md`](docs/deployment.md).

## Reporting a vulnerability

**Please do not open a public issue, pull request, or discussion for security reports.**

Report privately through GitHub's coordinated disclosure flow: open the repository's **Security** tab and
click **"Report a vulnerability"**, or go straight to
[the new-advisory page](https://github.com/jthompson18/maof/security/advisories/new).

Please include:

- the affected version(s) and component (e.g. registry, policy engine, transport adapter),
- a description of the issue and its impact,
- reproduction steps or a proof of concept, and
- any suggested remediation.

## What to expect

- **Acknowledgement** within 3 business days.
- An initial **assessment and severity** within 10 business days.
- Coordinated disclosure: we'll agree on a timeline, prepare a fix and a GitHub Security Advisory, and
  credit you (unless you prefer to remain anonymous).

Thank you for helping keep MAOF and its adopters safe.
