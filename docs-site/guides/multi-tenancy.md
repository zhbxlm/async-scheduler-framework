# Multi-tenancy

Each tenant has isolated quotas and allowed capabilities.

## Create a Tenant

```bash
scheduler tenant create --name team-a --quota '{"max_concurrent_tasks": 100}'
```

## Tenant API Keys

Each tenant is issued an API key used in the `X-API-Key` header when submitting tasks.

## Per-Tenant Quotas

```yaml
tenants:
  team-a:
    max_concurrent_tasks: 100
    priority_boost: 2
    allowed_capabilities:
      - image_generate
      - data_process
  team-b:
    max_concurrent_tasks: 50
    allowed_capabilities:
      - report_generate
```
