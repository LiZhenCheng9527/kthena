# Copyright The Volcano Authors
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
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from kthena.runtime.app import KV_SYNC_MODE_MEMORY, create_application, get_app_state, main
from kthena.runtime.router_registry import RouterRegistry


class _FakeAsyncResponse:
    def __init__(self, status_code: int = 200, payload: dict | None = None, text: str = "OK"):
        self.status_code = status_code
        self._payload = payload or {"status": "ok"}
        self.text = text

    def json(self):
        return self._payload


class _FakeAsyncClient:
    def __init__(self, *args, **kwargs):
        pass

    async def post(self, url: str, json: dict | None = None):
        return _FakeAsyncResponse(status_code=200, payload={"url": url, "json": json})

    async def aclose(self):
        return None


class _FakeSyncResponse:
    def __init__(self, status_code: int = 200, payload: dict | None = None, text: str = "OK"):
        self.status_code = status_code
        self._payload = payload or {"status": "ok"}
        self.text = text

    def json(self):
        return self._payload


class _FakeSyncClient:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, json: dict | None = None):
        # Simulate engine endpoint success
        return _FakeSyncResponse(status_code=200, payload={"url": url, "json": json})


def _make_args():
    return SimpleNamespace(
        engine="vllm",
        host="127.0.0.1",
        port=0,
        engine_base_url="http://engine.local:8000",
        engine_metrics_path="/metrics",
        pod="pod-1.ns",
        model="test-model",
    )


def test_load_lora_adapter_success(monkeypatch):
    # Arrange: create app and inject fake async client and downloader
    app = create_application(_make_args())

    state = get_app_state(app)
    monkeypatch.setattr("httpx.AsyncClient", _FakeAsyncClient)
    state.client = _FakeAsyncClient()

    monkeypatch.setattr("kthena.runtime.app.download_model", lambda *args, **kwargs: None)
    monkeypatch.setattr("httpx.Client", _FakeSyncClient)

    with TestClient(app) as client:
        body = {
            "lora_name": "lora-sql",
            "source": "s3://bucket/lora-sql",
            "output_dir": "/tmp/lora",
            "config": {},
            "max_workers": 1,
            "async_download": False,
        }

        # Act
        resp = client.post("/v1/load_lora_adapter", json=body)

        # Assert
        assert resp.status_code == 200
        data = resp.json()
        assert data


def test_unload_lora_adapter_success(monkeypatch):
    # Arrange: create app and inject fake async client
    app = create_application(_make_args())

    state = get_app_state(app)
    monkeypatch.setattr("httpx.AsyncClient", _FakeAsyncClient)
    state.client = _FakeAsyncClient()

    with TestClient(app) as client:
        body = {"lora_name": "lora-sql"}

        # Act
        resp = client.post("/v1/unload_lora_adapter", json=body)

        # Assert
        assert resp.status_code == 200
        data = resp.json()
        assert data


def test_load_lora_adapter_async_download_success(monkeypatch):
    # Arrange: create app, inject clients, and patch download + sync client
    app = create_application(_make_args())
    state = get_app_state(app)
    monkeypatch.setattr("httpx.AsyncClient", _FakeAsyncClient)
    state.client = _FakeAsyncClient()

    monkeypatch.setattr("kthena.runtime.app.download_model", lambda *args, **kwargs: None)
    monkeypatch.setattr("httpx.Client", _FakeSyncClient)

    with TestClient(app) as client:
        body = {
            "lora_name": "lora-sql",
            "source": "s3://bucket/lora-sql",
            "output_dir": "/tmp/lora",
            "config": {},
            "max_workers": 1,
            "async_download": True,
        }

        # Act
        resp = client.post("/v1/load_lora_adapter", json=body)

        # Assert (background task returns 202 Accepted)
        assert resp.status_code == 202


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint, client_ip, forwarded, expected_status", [
    ("http://10.0.0.5:9080", "10.0.0.5", None, 200),
    ("http://[fd00::5]:9080", "fd00::5", None, 200),
    # The endpoint must point back at the caller.
    ("http://10.0.0.6:9080", "10.0.0.5", None, 403),
    ("http://router.kthena-system.svc:9080", "10.0.0.5", None, 403),
    # Forwarding headers are not trusted.
    ("http://10.0.0.6:9080", "10.0.0.5", "10.0.0.6", 403),
    ("http://10.0.0.5:9080/path", "10.0.0.5", None, 400),
    (None, "10.0.0.5", None, 400),
    (["http://10.0.0.5:9080"], "10.0.0.5", None, 400),
])
async def test_register_router_requires_endpoint_of_caller(
        monkeypatch, endpoint, client_ip, forwarded, expected_status):
    app = create_application(_make_args())
    state = get_app_state(app)
    state.kv_sync_mode = KV_SYNC_MODE_MEMORY
    registry = RouterRegistry()
    monkeypatch.setattr("kthena.runtime.app.get_router_registry", lambda: registry)

    headers = {"X-Forwarded-For": forwarded} if forwarded else {}
    transport = httpx.ASGITransport(app=app, client=(client_ip, 41000))
    async with httpx.AsyncClient(transport=transport, base_url="http://runtime") as client:
        resp = await client.post(
            "/kvcache/routers/register",
            json={"router_id": "router-a", "endpoint": endpoint},
            headers=headers,
        )

    assert resp.status_code == expected_status, resp.text
    expected_endpoints = [endpoint] if expected_status == 200 else []
    assert registry.active_endpoints() == expected_endpoints


@pytest.mark.asyncio
async def test_main_serves_without_trusting_forwarding_headers(monkeypatch):
    served = {}

    def fake_run(app, **kwargs):
        served["app"] = app
        served["kwargs"] = kwargs

    monkeypatch.setattr("kthena.runtime.app.uvicorn.run", fake_run)
    monkeypatch.setattr("sys.argv", ["kthena-runtime", "-E", "vllm", "-I", "pod-1.ns", "-N", "test-model"])
    main()
    assert served["kwargs"].get("proxy_headers") is False

    # Load the app the way Uvicorn serves it with the options of main(). With
    # proxy headers enabled, Uvicorn would take the client address from
    # X-Forwarded-For when the peer is a trusted proxy (127.0.0.1 by default).
    monkeypatch.delenv("FORWARDED_ALLOW_IPS", raising=False)
    config = uvicorn.Config(served["app"], **served["kwargs"], log_config=None)
    config.load()
    get_app_state(served["app"]).kv_sync_mode = KV_SYNC_MODE_MEMORY
    registry = RouterRegistry()
    monkeypatch.setattr("kthena.runtime.app.get_router_registry", lambda: registry)

    transport = httpx.ASGITransport(app=config.loaded_app, client=("127.0.0.1", 41000))
    async with httpx.AsyncClient(transport=transport, base_url="http://runtime") as client:
        resp = await client.post(
            "/kvcache/routers/register",
            json={"router_id": "router-a", "endpoint": "http://10.0.0.5:9080"},
            headers={"X-Forwarded-For": "10.0.0.5"},
        )

    assert resp.status_code == 403, resp.text
    assert registry.active_endpoints() == []
