# Publishing to PyPI

MAOF is open source (Apache-2.0) and published to **public PyPI** as [`maof`](https://pypi.org/project/maof/),
so anyone can `pip install maof`. Releases are uploaded by the [`release`](../.github/workflows/release.yml)
GitHub Actions workflow using **PyPI Trusted Publishing (OIDC)**: there are no API tokens or passwords
stored anywhere; PyPI verifies the workflow's GitHub identity at upload time.

## One-time setup (Trusted Publishing)

Before the **first** release, register MAOF as a trusted publisher on PyPI. Because the project does not
exist on the index yet, use the *pending publisher* form:

1. Create a PyPI account at <https://pypi.org> (and a TestPyPI account at <https://test.pypi.org> for dry
   runs). Enable 2FA; it is required to upload.
2. PyPI → **Your account ▸ Publishing ▸ Add a pending publisher** (GitHub), and enter:
   - **PyPI Project Name:** `maof`
   - **Owner:** `jthompson18`
   - **Repository name:** `maof`
   - **Workflow name:** `release.yml`
   - **Environment name:** `pypi`
3. In the GitHub repo, create the **`pypi`** environment (*Settings ▸ Environments ▸ New environment*).
   Optionally add a required-reviewer rule so a human approves each publish.

Repeat step 2 on TestPyPI (environment `testpypi`) if you want the dry-run path below.

## Cutting a release

1. Bump the version in [`pyproject.toml`](../pyproject.toml) and [`src/maof/__init__.py`](../src/maof/__init__.py),
   and add a section to [`CHANGELOG.md`](../CHANGELOG.md).
2. Make sure the gate is green: `ruff check . && black --check . && mypy --strict src && pytest`.
3. Tag and push:
   ```bash
   git tag v<version> && git push origin v<version>
   ```
4. The `release` workflow builds (wheel + sdist), validates (`twine check`), writes `SHA256SUMS`, attests
   build provenance, and the `publish` job uploads to PyPI via OIDC. Within ~a minute, `pip install maof`
   serves the new version.

## Build & validate locally

```bash
pip install build twine
python -m build           # dist/maof-<version>-py3-none-any.whl + .tar.gz
twine check dist/*        # metadata + long-description render cleanly
```

## TestPyPI dry run (recommended before the first real release)

With a TestPyPI pending publisher configured, push a pre-release tag (e.g. `v<version>rc1`), or upload
manually:

```bash
python -m build
twine upload --repository testpypi dist/*
pip install --index-url https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple/ maof   # deps still come from real PyPI
```

## Supply chain

- **Vulnerability audit**: CI's `supply-chain` job runs `pip-audit` over the full dependency surface
  (`.[all,dev]`) on every push/PR and fails on known CVEs.
- **SBOM**: the same job publishes a CycloneDX SBOM (`sbom.cdx.json`) as a build artifact; hand it to
  security review / ingestion tooling as-is.
- **Provenance**: release artifacts are attested with GitHub artifact attestation (sigstore-backed), and
  `gh-action-pypi-publish` additionally emits [PEP 740](https://peps.python.org/pep-0740/) attestations
  that show on the PyPI project page. Consumers verify a downloaded wheel against this repo:

  ```bash
  gh attestation verify dist/maof-<version>-py3-none-any.whl --repo jthompson18/maof
  shasum -a 256 --check dist/SHA256SUMS
  ```

Run the same checks locally before tagging:

```bash
pip install pip-audit cyclonedx-bom build twine
pip-audit --skip-editable
cyclonedx-py environment --output-format JSON -o sbom.cdx.json
python -m build && twine check dist/*
```

## Installing from a private mirror

Public PyPI is the canonical source, but adopters who mirror packages internally can still install via the
standard pip flags without any MAOF-specific configuration:

```bash
pip install --index-url https://pkgs.internal.example.com/simple/ "maof[all]"
# or keep PyPI for public deps and add the internal index:
pip install --extra-index-url https://pkgs.internal.example.com/simple/ "maof[postgres,rabbitmq]"
```
