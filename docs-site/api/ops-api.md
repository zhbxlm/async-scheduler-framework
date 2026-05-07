# Ops API Reference

## Ops API (port 8000)

### GET /ops/v1/health
Public health check.

### GET /ops/v1/overview (auth)
System overview with per-capability stats, circuit states, utilization.

### GET /ops/v1/stats (auth)
Queue stats across all capabilities.

### Capabilities - /ops/v1/capabilities
| Method | Path | Description |
|--------|------|-------------|
| GET | / | List all |
| POST | / | Register (201) |
| GET | /{id} | Get detail |
| PUT | /{id} | Update |
| DELETE | /{id} | Unregister (204) |
| GET | /{id}/health | Health |

### Clusters - /ops/v1/clusters
| Method | Path | Description |
|--------|------|-------------|
| GET | / | List all |
| POST | / | Register (201) |
| GET | /{id} | Get detail |
| PUT | /{id} | Update |
| DELETE | /{id} | Unregister (204) |
| GET | /{id}/resources | Resources |

### Nodes - /ops/v1/nodes
| Method | Path | Description |
|--------|------|-------------|
| GET | / | List all |
| GET | /{id} | Get detail |
| POST | /{id}/invite | Invite to cluster |
| POST | /{id}/drain | Start drain |
| POST | /{id}/release | Release |
| DELETE | /{id} | Unregister (204) |

### DAGs - /ops/v1/dags
| Method | Path | Description |
|--------|------|-------------|
| GET | / | List registered DAGs |
| POST | / | Register (201) |
| GET | /{dag_id} | Get definition |
| PUT | /{dag_id} | Update |
| DELETE | /{dag_id} | Delete (204) |

### Schedules - /ops/v1/schedules
| Method | Path | Description |
|--------|------|-------------|
| GET | / | List all |
| POST | / | Create (201) |
| GET | /{id} | Get detail |
| PUT | /{id} | Update |
| POST | /{id}/toggle | Enable/disable |
| DELETE | /{id} | Delete (204) |

### Tenants - /ops/v1/tenants
| Method | Path | Description |
|--------|------|-------------|
| GET | / | List (super-admin) |
| POST | / | Register (super-admin, 201) |
| GET | /{id} | Get detail |
| PUT | /{id} | Update |
| DELETE | /{id} | Unregister (super-admin, 204) |
| POST | /{id}/api-keys | Generate API key |
| GET | /{id}/usage | Usage stats |

### Queue Ops
| Method | Path | Description |
|--------|------|-------------|
| GET | /queue/{cap}/snapshot | Queue snapshot |
| POST | /queue/{cap}/cleanup-stale | Clean stale |
| GET | /tasks/{id}/debug | Task debug |

## Health API
| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Basic |
| GET | /health/ready | Readiness |
| GET | /health/detailed | Component-level |
| GET | /health/redis | Redis |
| GET | /health/mysql | MySQL |
| GET | /metrics | Prometheus |

## Error Codes
| Code | Meaning |
|------|---------|
| 200 | Success |
| 201 | Created |
| 202 | Accepted (not ready) |
| 204 | No Content |
| 400 | Bad request |
| 401 | Invalid API key |
| 403 | Forbidden |
| 404 | Not found |
| 409 | Conflict |
| 422 | Validation error |
| 503 | Service unavailable |