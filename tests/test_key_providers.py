"""Signing-key providers (env / file / Vault) + build_signer.

The seam that moves HMAC keys out of process env when deployments demand it.
Vault is exercised with an injected fake client here (offline); the live
dev-mode Vault check lives in the local-only integration suite.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from maof.config import Settings
from maof.errors import ConfigError
from maof.transport.signing import (
    EnvKeyProvider,
    FileKeyProvider,
    VaultKeyProvider,
    build_signer,
)


def test_env_provider_mirrors_settings() -> None:
    keys, active = EnvKeyProvider("s3cret", "k1").load()
    assert keys == {"k1": "s3cret"} and active == "k1"


def test_env_provider_allows_empty_secret_for_dev_mode() -> None:
    keys, active = EnvKeyProvider("", "default").load()
    assert keys == {"default": ""} and active == "default"


def test_file_provider_flat_shape(tmp_path: Path) -> None:
    path = tmp_path / "keys.json"
    path.write_text(json.dumps({"default": "alpha", "rotated": "beta"}), encoding="utf-8")
    keys, active = FileKeyProvider(path).load()
    assert keys == {"default": "alpha", "rotated": "beta"}
    assert active == "default"


def test_file_provider_structured_shape_with_active_kid(tmp_path: Path) -> None:
    path = tmp_path / "keys.json"
    path.write_text(
        json.dumps({"keys": {"k1": "alpha", "k2": "beta"}, "active_kid": "k2"}),
        encoding="utf-8",
    )
    keys, active = FileKeyProvider(path).load()
    assert active == "k2" and keys["k2"] == "beta"


@pytest.mark.parametrize("content", ["not json", json.dumps([]), json.dumps({"k": ""})])
def test_file_provider_fails_closed_on_bad_content(tmp_path: Path, content: str) -> None:
    path = tmp_path / "keys.json"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(ConfigError):
        FileKeyProvider(path).load()


def test_file_provider_fails_closed_on_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        FileKeyProvider(tmp_path / "absent.json").load()


def _fake_vault(data: dict[str, Any]) -> Any:
    def read_secret_version(path: str, mount_point: str = "secret") -> dict[str, Any]:
        return {"data": {"data": data}}

    return SimpleNamespace(
        secrets=SimpleNamespace(
            kv=SimpleNamespace(v2=SimpleNamespace(read_secret_version=read_secret_version))
        )
    )


def test_vault_provider_reads_kv2_data() -> None:
    provider = VaultKeyProvider(
        secret_path="maof/signing", client=_fake_vault({"default": "v-secret", "old": "v-old"})
    )
    keys, active = provider.load()
    assert keys == {"default": "v-secret", "old": "v-old"}
    assert active == "default"


def test_vault_provider_fails_closed_on_empty_secret() -> None:
    with pytest.raises(ConfigError):
        VaultKeyProvider(secret_path="maof/signing", client=_fake_vault({})).load()


def test_vault_provider_fails_closed_without_path() -> None:
    with pytest.raises(ConfigError):
        VaultKeyProvider(client=_fake_vault({"default": "x"})).load()


def test_build_signer_env_default_signs_and_verifies() -> None:
    signer = build_signer(Settings(msg_signing_secret="abc", msg_signing_key_id="default"))
    headers = signer.headers(b"body")
    signer.verify(b"body", headers)


def test_build_signer_file_provider(tmp_path: Path) -> None:
    path = tmp_path / "keys.json"
    path.write_text(json.dumps({"default": "filekey"}), encoding="utf-8")
    signer = build_signer(Settings(signing_key_provider="file", signing_keys_file=str(path)))
    signer.verify(b"x", signer.headers(b"x"))


def test_build_signer_file_provider_fails_closed_without_path() -> None:
    with pytest.raises(ConfigError):
        build_signer(Settings(signing_key_provider="file"))
