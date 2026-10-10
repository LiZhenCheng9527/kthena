# Copyright The Volcano Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ipaddress
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

DEFAULT_ROUTER_TTL_SECONDS = 90
MAX_ROUTER_TTL_SECONDS = 3600


def validate_router_endpoint(endpoint: str) -> bool:
    """Only plain http/https base URLs (scheme + host[:port]) are accepted.

    The endpoint is later concatenated with KV_EVENTS_PATH, so URLs carrying a
    path, query, fragment, or userinfo would produce wrong push targets and
    widen the abuse surface of the registration endpoint.
    """
    if not endpoint:
        return False
    try:
        parsed = urlparse(endpoint)
    except ValueError:
        return False
    return (
        parsed.scheme in ("http", "https")
        and bool(parsed.hostname)
        and parsed.path in ("", "/")
        and not parsed.params
        and not parsed.query
        and not parsed.fragment
        and parsed.username is None
        and parsed.password is None
    )


def _normalize_ip(host: str) -> Optional[str]:
    """Returns the canonical form of an IP literal, or None for non-IPs."""
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return None
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return str(ip)


def _endpoint_host(endpoint: str) -> str:
    hostname = urlparse(endpoint).hostname or ""
    return _normalize_ip(hostname) or hostname


def endpoint_matches_source(endpoint: str, source_host: Optional[str]) -> bool:
    """Whether endpoint points back at the host the registration came from.

    Routers register the push endpoint built from their own pod IP, so the
    endpoint host must be an IP literal equal to the connection's source IP.
    This keeps a caller from directing KV event pushes to any other address.
    """
    if not source_host:
        return False
    try:
        hostname = urlparse(endpoint).hostname
    except ValueError:
        return False
    endpoint_ip = _normalize_ip(hostname) if hostname else None
    return endpoint_ip is not None and endpoint_ip == _normalize_ip(source_host)


@dataclass
class RouterRegistration:
    router_id: str
    endpoint: str
    expires_at: float
    # Identifies one router process; a restarted router container re-registers
    # with a new generation even when its pod name (router_id) is unchanged.
    generation: str = ""


class RouterRegistry:
    """Tracks router instances registered for in-memory KV event push.

    Routers re-register periodically (heartbeat); registrations that are not
    renewed within their TTL are pruned and no longer receive events.

    Registrations are keyed by the endpoint host rather than the self-reported
    router_id, so a registration can only replace one for the same host.
    """

    def __init__(self):
        self._routers: Dict[str, RouterRegistration] = {}

    def register(self, router_id: str, endpoint: str,
                 ttl_seconds: int = DEFAULT_ROUTER_TTL_SECONDS,
                 generation: str = "") -> bool:
        """Register or renew a router.

        Returns True when the router needs a full snapshot: it is new, its
        previous registration expired, it re-registered with a new endpoint,
        or it re-registered with a new process generation (e.g. after a
        router container restart that lost the in-memory index).
        """
        if not router_id:
            raise ValueError("router_id must not be empty")
        if not validate_router_endpoint(endpoint):
            raise ValueError(f"invalid router endpoint: {endpoint!r}")
        endpoint = endpoint.rstrip("/")

        ttl = min(max(int(ttl_seconds), 1), MAX_ROUTER_TTL_SECONDS)
        now = time.monotonic()
        key = _endpoint_host(endpoint)
        existing = self._routers.get(key)
        needs_snapshot = (
            existing is None
            or existing.router_id != router_id
            or existing.endpoint != endpoint
            or existing.generation != generation
            or existing.expires_at <= now
        )
        self._routers[key] = RouterRegistration(
            router_id=router_id,
            endpoint=endpoint,
            expires_at=now + ttl,
            generation=generation,
        )
        if needs_snapshot:
            logger.info(
                f"Router registered: id={router_id}, endpoint={endpoint}, ttl={ttl}s")
        return needs_snapshot

    def active_endpoints(self) -> List[str]:
        self._prune()
        return [r.endpoint for r in self._routers.values()]

    def _prune(self) -> None:
        now = time.monotonic()
        expired = [key for key, reg in self._routers.items() if reg.expires_at <= now]
        for key in expired:
            reg = self._routers.pop(key)
            logger.info(f"Router registration expired: id={reg.router_id}, endpoint={reg.endpoint}")


_router_registry: Optional[RouterRegistry] = None


def get_router_registry() -> RouterRegistry:
    global _router_registry
    if _router_registry is None:
        _router_registry = RouterRegistry()
    return _router_registry
