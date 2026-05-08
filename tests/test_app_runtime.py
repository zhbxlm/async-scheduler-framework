from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI

from src.api.app_runtime import cache_auth_settings, mount_container_state, register_control_plane_resources


def test_mount_container_state_sets_declared_attributes_only():
    app = FastAPI()
    container = SimpleNamespace(foo=1, bar=2, baz=3)

    mount_container_state(app, container, ("foo", "bar"))

    assert app.state.foo == 1
    assert app.state.bar == 2
    assert not hasattr(app.state, "baz")


def test_cache_auth_settings_publishes_expected_fields():
    app = FastAPI()
    settings = SimpleNamespace(
        tenant=SimpleNamespace(
            super_admin_api_key="secret",
            multi_tenant_enabled=True,
            tenant_id_header="X-Tenant-Id",
        )
    )

    cache_auth_settings(app, settings)

    assert app.state.auth_settings == {
        "super_admin_key": "secret",
        "multi_tenant_enabled": True,
        "tenant_id_header": "X-Tenant-Id",
    }


def test_register_control_plane_resources_respects_settings_and_presence():
    class FakeManager:
        def __init__(self):
            self.resources = []

        def register_resource(self, resource):
            self.resources.append(resource)

    manager = FakeManager()
    container = SimpleNamespace(
        lifecycle_manager=manager,
        task_reconciler=object(),
        cron_scheduler=object(),
        compensation_service=object(),
        callback_dispatcher=object(),
    )
    settings = SimpleNamespace(
        background=SimpleNamespace(
            reconcile=SimpleNamespace(enabled=True),
            cron=SimpleNamespace(enabled=False),
        )
    )

    registered = register_control_plane_resources(container, settings)

    assert registered == ["task_reconciler", "compensation_service", "callback_dispatcher"]
    assert [resource.name for resource in manager.resources] == [
        "TaskReconciler",
        "CompensationService",
        "CallbackDispatcher",
    ]
