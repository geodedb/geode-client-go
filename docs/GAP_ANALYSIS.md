# GAP Analysis

Last updated: 2026-01-30
Reviewed by: Adversarial Security Review Agent
Repository: devnw/codepros/geode/geode-client-go
Commit: b27e102
Review type: Comprehensive adversarial security review (Round 6 - no new findings)

## Summary

Open findings by severity:

- Critical: 0
- High: 0
- Medium: 0
- Low: 0
- Info: 0

Closed findings: 27 (Issues #1-#23, #25-#28)
Won't Do findings: 1 (Issue #24)

Top risks (remaining):

- None

## Stack-ranked findings

| Rank | Severity | RiskScore | Finding | Component | CWE/OWASP | Evidence (file:line) | GitLab Issue | Fix Summary | Owner | SLA/Due | Status |
|------|----------|-----------|---------|-----------|-----------|----------------------|--------------|-------------|-------|---------|--------|
| 1 | High | 48 | Go toolchain pinned to vulnerable 1.24.0 (stdlib CVEs) (confidence::confirmed) | Supply chain | CWE-1104 | go.mod:3, .gitlab-ci.yml:22-30 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/28> | Upgraded Go toolchain to 1.24.12 in go.mod and CI; osv-scanner clean | TBD | 30d (2026-03-01) | **Closed** |
| 2 | Medium | 36 | CA/cert/key loading allows special files to bypass size limits (confidence::confirmed) | TLS config | CWE-400 | security.go:95-115, security.go:150-186 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/27> | Added readLimitedFile to enforce regular files and size limits; tests in security_test.go | TBD | 90d (2026-04-30) | **Closed** |
| 3 | Medium | 36 | Unbounded inbound frame size (confidence::confirmed) | Transport/Protocol | CWE-400 / API4:2023 | transport.go:205, transport.go:254 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/22> | Added 32MB frame limit via readLineLimited; tests in transport_test.go | TBD | 90d (2026-04-30) | **Closed** |
| 4 | Medium | 32 | Explicit CA path fails open to system CAs (confidence::confirmed) | TLS config | CWE-295 | security.go:96, security.go:135 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/23> | Explicit CA paths now fail closed; tests in security_test.go | TBD | 90d (2026-04-30) | **Closed** |
| 5 | Medium | 32 | CI components pinned to mutable @v1 tags (confidence::confirmed) | CI/CD supply chain | CWE-829 | .gitlab-ci.yml:10-58 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/24> | Won't Do: catalog has other controls; pinning not used | TBD | 90d (2026-04-30) | **Closed (Won't Do)** |
| 6 | Low | 24 | SBOM scan allowed to fail (confidence::confirmed) | CI/CD | CWE-693 | .gitlab-ci.yml:53-55 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/25> | SBOM scan now required (allow_failure: false) | TBD | 180d (2026-07-29) | **Closed** |
| 7 | Low | 24 | Gitleaks allowlist excludes tests/ and testdata/ (confidence::confirmed) | CI/CD secrets | CWE-798 | .gitleaks.toml:16-19 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/26> | Removed broad allowlist entries for tests/testdata | TBD | 180d (2026-07-29) | **Closed** |

## Findings by area

App/API/Auth

- Issue #27: CA/cert/key loading allows special files to bypass size limits. **Closed**
- Issue #22: Unbounded inbound frame size allows client memory exhaustion. **Closed**
- Issue #23: Explicit CA path fails open to system CAs (pinning bypass). **Closed**

Data

- No new findings in this round.

CI-CD

- Issue #28: Go toolchain pinned to vulnerable 1.24.0 (stdlib CVEs). **Closed**
- Issue #24: CI components pinned to mutable @v1 tags. **Closed (Won't Do)**
- Issue #25: SBOM scan allowed to fail. **Closed**
- Issue #26: Gitleaks allowlist excludes tests/ and testdata/. **Closed**

IaC

- No new findings in this round.

Container

- No new findings in this round.

Ops

- No new findings in this round.

## Quick wins / Medium / Strategic

Quick wins (low effort)

- Tighten gitleaks allowlist to specific safe fixtures (Issue #26). **Done**
- Require SBOM scan to pass on protected branches (Issue #25). **Done**

Medium (engineering effort)

- Add max inbound frame size and reject oversized frames (Issue #22). **Done**
- Fail closed when explicit CA path is missing or unreadable (Issue #23). **Done**
- Reject non-regular cert/key/CA files and enforce bounded reads (Issue #27). **Done**

Strategic (pipeline hardening)

- Pin CI catalog components to immutable SHAs and document update process (Issue #24). **Won't Do**
- Upgrade Go toolchain to patched version (Issue #28). **Done**

## Won't Do

- Issue #24: Marked Won't Do because ci-catalog has other compensating controls and version pinning will not be used.

## Historical findings (closed)

The following issues were addressed in prior reviews (Rounds 1-2) and are closed. See each issue for historical evidence and remediation details.

| Issue | Title | Status |
|-------|-------|--------|
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/1> | Security: Benchmark tool defaults insecure=true (CWE-295) | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/2> | Security: Gitleaks allowlist disables scanning for all source files | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/3> | Security: Hostname validation function exists but is never called | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/4> | Security: Complex manual lock management in QueryContext may lead to lock leaks | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/5> | Security: Example code demonstrates insecure=true pattern | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/6> | Security: CI pipeline allows test failures without blocking | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/7> | Security: Pre-commit CI skips golangci-lint including gosec security checks | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/8> | Security: No size limit on certificate file loading | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/9> | Hardening: Add client-side connection rate limiting | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/10> | Supply Chain: Compiled binary tracked in git repository | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/11> | Security: Client cert/key files lack size limits (DoS) | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/12> | Supply Chain: CI catalog components use mutable @latest tags | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/13> | Security: Deprecated driver methods use unbounded context | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/14> | Security: gosec linter disabled for test files | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/15> | Security: Gitleaks allowlists all test files | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/16> | Hardening: Pre-commit YAML check uses --unsafe flag | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/17> | Hardening: Error messages include network addresses | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/18> | Supply Chain: Docker images use mutable :latest tag in Makefile and tests | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/19> | Security: No upper bound on page_size configuration (potential DoS) | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/20> | Security: Benchmark CLI uses unbounded context.Background() | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/21> | Hardening: CODEOWNERS missing security-sensitive code files | Closed |
