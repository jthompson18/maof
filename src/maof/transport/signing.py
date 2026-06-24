"""HMAC message signing + verification.

``sig = hex(HMAC_SHA256(message_body_bytes, secret))`` keyed by ``kid``.
Verification looks up the secret by ``kid``, recomputes, and constant-time
compares on lowercased hex. Multiple keys are supported for rotation, and the
header aliases below are accepted on verify.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from maof.errors import ConfigError, SignatureError

if TYPE_CHECKING:
    from maof.config import Settings

KID_HEADER_ALIASES = ("kid", "x-hmac-kid", "x-sign-kid")
SIG_HEADER_ALIASES = ("sig", "x-hmac-sig", "x-signature")


def compute_signature(body: bytes, secret: str) -> str:
    """Lowercase hex HMAC-SHA256 of ``body`` under ``secret``."""
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _extract(headers: Mapping[str, str], aliases: tuple[str, ...]) -> str | None:
    lowered = {k.lower(): v for k, v in headers.items()}
    for alias in aliases:
        if alias in lowered:
            return lowered[alias]
    return None


class Signer:
    """Signs with one active key; verifies against any held key (rotation)."""

    def __init__(self, keys: Mapping[str, str], active_kid: str = "default") -> None:
        if active_kid not in keys:
            raise ConfigError(f"active signing key id {active_kid!r} not present in keys")
        self._keys: dict[str, str] = dict(keys)
        self._active_kid = active_kid

    @property
    def active_kid(self) -> str:
        return self._active_kid

    def sign(self, body: bytes) -> str:
        return compute_signature(body, self._keys[self._active_kid])

    def headers(self, body: bytes) -> dict[str, str]:
        """Canonical signing headers for a message body."""
        return {"kid": self._active_kid, "sig": self.sign(body)}

    def verify(self, body: bytes, headers: Mapping[str, str]) -> None:
        """Raise :class:`SignatureError` unless ``headers`` carry a valid signature."""
        kid = _extract(headers, KID_HEADER_ALIASES)
        sig = _extract(headers, SIG_HEADER_ALIASES)
        if not kid or not sig:
            raise SignatureError("missing signing headers (kid/sig)")
        secret = self._keys.get(kid)
        if secret is None:
            raise SignatureError(f"unknown signing key id: {kid!r}")
        expected = compute_signature(body, secret)
        if not hmac.compare_digest(expected.lower(), sig.lower()):
            raise SignatureError("signature mismatch")

    def is_valid(self, body: bytes, headers: Mapping[str, str]) -> bool:
        try:
            self.verify(body, headers)
        except SignatureError:
            return False
        return True


def generate_ed25519_keypair() -> tuple[bytes, bytes]:
    """Generate an Ed25519 (private_pem, public_pem) pair (requires the ``crypto`` extra)."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519

    private = ed25519.Ed25519PrivateKey.generate()
    private_pem = private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    public_pem = private.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return private_pem, public_pem


class Ed25519Signer:
    """Asymmetric signer with the same header contract as
    :class:`Signer` — better for cross-org boundaries (third-party product
    teams, A2A): verifiers hold only public keys and cannot forge approvals."""

    def __init__(
        self,
        *,
        private_key_pem: bytes | None = None,
        public_keys: Mapping[str, bytes] | None = None,
        active_kid: str = "default",
    ) -> None:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import ed25519

        self._active_kid = active_kid
        self._private: ed25519.Ed25519PrivateKey | None = None
        if private_key_pem:
            loaded = serialization.load_pem_private_key(private_key_pem, password=None)
            if not isinstance(loaded, ed25519.Ed25519PrivateKey):
                raise ConfigError("Ed25519Signer requires an Ed25519 private key")
            self._private = loaded
        self._publics: dict[str, ed25519.Ed25519PublicKey] = {}
        for kid, pem in (public_keys or {}).items():
            public = serialization.load_pem_public_key(pem)
            if not isinstance(public, ed25519.Ed25519PublicKey):
                raise ConfigError(f"public key {kid!r} is not Ed25519")
            self._publics[kid] = public

    @property
    def active_kid(self) -> str:
        return self._active_kid

    def headers(self, body: bytes) -> dict[str, str]:
        if self._private is None:
            raise ConfigError("Ed25519Signer has no private key; verify-only")
        return {"kid": self._active_kid, "sig": self._private.sign(body).hex()}

    def verify(self, body: bytes, headers: Mapping[str, str]) -> None:
        from cryptography.exceptions import InvalidSignature

        kid = _extract(headers, KID_HEADER_ALIASES)
        sig = _extract(headers, SIG_HEADER_ALIASES)
        if not kid or not sig:
            raise SignatureError("missing signing headers (kid/sig)")
        public = self._publics.get(kid)
        if public is None:
            raise SignatureError(f"unknown signing key id: {kid!r}")
        try:
            public.verify(bytes.fromhex(sig), body)
        except (InvalidSignature, ValueError) as exc:
            raise SignatureError("signature mismatch") from exc

    def is_valid(self, body: bytes, headers: Mapping[str, str]) -> bool:
        try:
            self.verify(body, headers)
        except SignatureError:
            return False
        return True


@runtime_checkable
class KeyProvider(Protocol):
    """Sources the HMAC key set: ``load() -> (keys_by_kid, active_kid)``.

    The seam that keeps secrets out of process env when the deployment demands
    it — env (default), mounted file, or Vault; adopters can implement their own
    (AWS/GCP secret managers, SOPS, …). Providers fail closed with ConfigError.
    """

    def load(self) -> tuple[dict[str, str], str]: ...


class EnvKeyProvider:
    """The default: one secret + key id straight from Settings/env.

    An empty secret is permitted (dev/embedded mode) — enforcement lives with
    the consumer's ``require_signature`` gate, preserving existing behavior.
    The file/vault providers, being explicit opt-ins, fail closed instead.
    """

    def __init__(self, secret: str, kid: str = "default") -> None:
        self._secret = secret
        self._kid = kid

    def load(self) -> tuple[dict[str, str], str]:
        return {self._kid: self._secret}, self._kid


class FileKeyProvider:
    """Keys from a JSON file (k8s/docker mounted secret).

    Accepts ``{"keys": {kid: secret, ...}, "active_kid": "..."}`` or the flat
    ``{kid: secret, ...}`` form (active key: ``default`` if present, else the
    first kid in sorted order).
    """

    def __init__(self, path: str | Path) -> None:
        self._raw = str(path)
        self._path = Path(path) if self._raw.strip() else None

    def load(self) -> tuple[dict[str, str], str]:
        if self._path is None:
            raise ConfigError("signing_keys_file is empty (signing_key_provider=file)")
        try:
            parsed = json.loads(self._path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise ConfigError(f"signing keys file unreadable: {self._path} ({exc})") from exc
        except json.JSONDecodeError as exc:
            raise ConfigError(f"signing keys file is not valid JSON: {self._path}") from exc
        if isinstance(parsed, dict) and isinstance(parsed.get("keys"), dict):
            keys = {str(k): str(v) for k, v in parsed["keys"].items()}
            active = str(parsed.get("active_kid") or _default_kid(keys))
        elif isinstance(parsed, dict):
            keys = {str(k): str(v) for k, v in parsed.items()}
            active = _default_kid(keys)
        else:
            raise ConfigError(f"signing keys file must hold a JSON object: {self._path}")
        if not keys or not all(keys.values()):
            raise ConfigError(f"signing keys file holds no usable keys: {self._path}")
        return keys, active


class VaultKeyProvider:
    """Keys from a HashiCorp Vault KV-v2 secret whose data is ``{kid: secret}``.

    Lazy-imports ``hvac`` (the ``vault`` extra); a pre-built client is injectable
    for tests and custom auth methods. Active key: ``default`` if present, else
    the first kid in sorted order.
    """

    def __init__(
        self,
        *,
        url: str = "",
        token: str = "",
        secret_path: str = "",
        mount: str = "secret",
        client: Any | None = None,
    ) -> None:
        self._url = url
        self._token = token
        self._secret_path = secret_path
        self._mount = mount
        self._client = client

    def load(self) -> tuple[dict[str, str], str]:
        if not self._secret_path:
            raise ConfigError("vault_secret_path is empty (signing_key_provider=vault)")
        client = self._client
        if client is None:
            try:
                import hvac
            except ImportError as exc:  # pragma: no cover - depends on installed extras
                raise ConfigError(
                    "VaultKeyProvider requires the 'vault' extra (pip install maof[vault])"
                ) from exc
            client = hvac.Client(url=self._url or None, token=self._token or None)
        try:
            response = client.secrets.kv.v2.read_secret_version(
                path=self._secret_path, mount_point=self._mount
            )
            data = response["data"]["data"]
        except Exception as exc:  # noqa: BLE001 - fail closed on any Vault error
            raise ConfigError(f"Vault read failed for {self._secret_path!r}: {exc}") from exc
        keys = {str(k): str(v) for k, v in dict(data).items() if str(v)}
        if not keys:
            raise ConfigError(f"Vault secret {self._secret_path!r} holds no usable keys")
        return keys, _default_kid(keys)


def _default_kid(keys: dict[str, str]) -> str:
    return "default" if "default" in keys else sorted(keys)[0]


def build_signer(settings: Settings) -> Signer:
    """Construct the message Signer from the configured key provider."""
    provider = settings.signing_key_provider
    source: KeyProvider
    if provider == "env":
        source = EnvKeyProvider(settings.msg_signing_secret, settings.msg_signing_key_id)
    elif provider == "file":
        source = FileKeyProvider(settings.signing_keys_file)
    elif provider == "vault":
        source = VaultKeyProvider(
            url=settings.vault_url,
            token=settings.vault_token,
            secret_path=settings.vault_secret_path,
            mount=settings.vault_mount,
        )
    else:  # pragma: no cover - Settings restricts the literal
        raise ConfigError(f"unknown signing_key_provider: {provider!r}")
    keys, active_kid = source.load()
    return Signer(keys, active_kid=active_kid)


def ensure_signing_configured(settings: Settings) -> None:
    """Fail closed when verification is required but no signing key is configured.

    The env provider permits an empty secret for the offline/embedded dev path; when
    ``require_signature`` is set (the default), refuse to start rather than run with
    verification silently disabled. The file/vault providers already fail closed in
    ``load()``.
    """
    if (
        settings.require_signature
        and settings.signing_key_provider == "env"
        and not settings.msg_signing_secret
    ):
        raise ConfigError(
            "message signing is required (require_signature=true) but MSG_SIGNING_SECRET is "
            "empty. Set MSG_SIGNING_SECRET, use signing_key_provider=file|vault, or set "
            "REQUIRE_SIGNATURE=false for a trusted single-process / offline deployment."
        )


__all__ = [
    "Signer",
    "Ed25519Signer",
    "generate_ed25519_keypair",
    "compute_signature",
    "KeyProvider",
    "EnvKeyProvider",
    "FileKeyProvider",
    "VaultKeyProvider",
    "build_signer",
    "ensure_signing_configured",
    "KID_HEADER_ALIASES",
    "SIG_HEADER_ALIASES",
]
