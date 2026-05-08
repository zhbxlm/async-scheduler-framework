# Release Notes Draft — Phase 3 Architecture Normalization

## Highlights

This update completes a Phase 3 internal architecture normalization wave for `async-scheduler-framework`.
It does not introduce major new end-user features, but it significantly improves consistency and maintainability across the codebase.

## Included in this update

### Resource route standardization
- Standardized `clusters`, `nodes`, `dags`, and `schedules` routes
- Added a dedicated resource service layer
- Reduced direct route-level runtime orchestration and `app.state` coupling

### Runtime assembly standardization
- Unified startup/shutdown helper patterns across ops-api, task-api, and control-plane
- Made control-plane background resource registration more explicit and testable

### Container builder cleanup
- Reduced repeated builder plumbing inside `ServiceContainer`
- Kept runtime role ownership explicit while centralizing common infra setup

## Quality impact

This release improves:
- maintainability,
- testability,
- boundary clarity,
- future refactor safety.

## Compatibility

- No intended public API path changes
- No intended user-facing behavior changes
- No intended deployment model changes

## Recommended validation before final release

- Run full regression suite
- Review release diff for purely internal-impact confirmation
- Merge with changelog update and architecture summary
