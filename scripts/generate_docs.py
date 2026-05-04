"""Auto-generate API documentation and project overview."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

# ---------------------------------------------------------------------------
# Schema export
# ---------------------------------------------------------------------------

def export_openapi_schema(output_path: str = "docs/openapi.json") -> None:
    """Export FastAPI OpenAPI schema to JSON file."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.main import app

    schema = app.openapi()
    out = PROJECT_ROOT / output_path
    out.parent.mkdir(parents=True, exist_ok=True)

    with open(out, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)

    print(f"✅ OpenAPI schema written to {output_path}")
    print(f"   Endpoints: {sum(len(v) for v in schema.get('paths', {}).values())}")


# ---------------------------------------------------------------------------
# Markdown API docs
# ---------------------------------------------------------------------------

def generate_api_markdown(output_path: str = "docs/API.md") -> None:
    """Generate human-readable Markdown API documentation from OpenAPI schema."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.main import app

    schema = app.openapi()
    lines: list[str] = []

    # Header
    lines += [
        f"# {schema.get('info', {}).get('title', 'API')} Documentation",
        "",
        f"> Auto-generated on {datetime.now().strftime('%Y-%m-%d %H:%M')} — do not edit manually.",
        "",
        f"**Version:** `{schema.get('info', {}).get('version', 'unknown')}`  ",
        "",
    ]

    # Group endpoints by tag
    paths = schema.get("paths", {})
    by_tag: dict[str, list] = {}

    for path, methods in sorted(paths.items()):
        for method, op in methods.items():
            tags = op.get("tags", ["misc"])
            for tag in tags:
                by_tag.setdefault(tag, []).append((method.upper(), path, op))

    # Table of contents
    lines += ["## Contents", ""]
    for tag in sorted(by_tag):
        anchor = tag.lower().replace(" ", "-")
        lines.append(f"- [{tag}](#{anchor})")
    lines += ["", "---", ""]

    # Per-tag sections
    for tag in sorted(by_tag):
        lines += [f"## {tag}", ""]

        for method, path, op in sorted(by_tag[tag], key=lambda x: x[1]):
            summary = op.get("summary", "")
            description = op.get("description", "")

            lines += [
                f"### `{method} {path}`",
                "",
                f"**{summary}**" if summary else "",
                "",
            ]

            if description:
                lines += [description, ""]

            # Parameters
            params = op.get("parameters", [])
            if params:
                lines += ["**Parameters:**", ""]
                lines += ["| Name | In | Type | Required | Description |"]
                lines += ["|------|----|------|----------|-------------|"]
                for p in params:
                    schema_type = p.get("schema", {}).get("type", "any")
                    required = "✅" if p.get("required") else "—"
                    desc = p.get("description", "")
                    lines.append(
                        f"| `{p['name']}` | {p['in']} | {schema_type} | {required} | {desc} |"
                    )
                lines += [""]

            # Request body
            req_body = op.get("requestBody", {})
            if req_body:
                content = req_body.get("content", {})
                for media_type, media_info in content.items():
                    schema_ref = media_info.get("schema", {}).get("$ref", "")
                    model_name = schema_ref.split("/")[-1] if schema_ref else media_type
                    lines += [f"**Request Body:** `{model_name}` ({media_type})", ""]

            # Responses
            responses = op.get("responses", {})
            if responses:
                lines += ["**Responses:**", ""]
                lines += ["| Status | Description |"]
                lines += ["|--------|-------------|"]
                for status, resp in sorted(responses.items()):
                    desc = resp.get("description", "")
                    lines.append(f"| `{status}` | {desc} |")
                lines += [""]

            lines += ["---", ""]

    out = PROJECT_ROOT / output_path
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ API docs written to {output_path}")


# ---------------------------------------------------------------------------
# Project structure docs
# ---------------------------------------------------------------------------

def generate_structure_markdown(output_path: str = "docs/STRUCTURE.md") -> None:
    """Generate project structure documentation."""
    lines: list[str] = [
        "# Project Structure",
        "",
        f"> Auto-generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "```",
    ]

    def _walk(path: Path, prefix: str = "", depth: int = 0) -> None:
        if depth > 4:
            return
        try:
            children = sorted(
                [c for c in path.iterdir() if not c.name.startswith(".")],
                key=lambda p: (p.is_file(), p.name),
            )
        except PermissionError:
            return

        skip_dirs = {"__pycache__", ".git", ".pytest_cache", "node_modules",
                     ".mypy_cache", "dist", "build", "*.egg-info"}

        for i, child in enumerate(children):
            if child.name in skip_dirs or any(child.name.endswith(s) for s in [".pyc", ".pyo"]):
                continue
            connector = "└── " if i == len(children) - 1 else "├── "
            lines.append(f"{prefix}{connector}{child.name}")
            if child.is_dir():
                extension = "    " if i == len(children) - 1 else "│   "
                _walk(child, prefix + extension, depth + 1)

    lines.append(f"{PROJECT_ROOT.name}/")
    _walk(PROJECT_ROOT)
    lines += ["```", "", "## Module Descriptions", ""]

    module_docs = {
        "src/api/": "FastAPI routers and dependency injection",
        "src/api/routes/": "Individual route handlers per resource",
        "src/common/": "Shared utilities: DB, Redis, error handling, tracing, lifecycle",
        "src/models/": "Pydantic data models for tasks, nodes, clusters, etc.",
        "src/platform/": "Domain services: registries, scheduler, reconciler, queue",
        "src/workload/": "Ray workload execution and worker actors",
        "src/cli/": "Command-line interface (Click)",
        "src/agent/": "Node agent: heartbeat, ownership, server",
        "config/": "Application configuration (pydantic-settings based)",
        "tests/": "Test suite: unit, integration, benchmarks",
        ".github/workflows/": "CI/CD pipelines (GitHub Actions)",
        "docs/": "Documentation (auto-generated and manual)",
    }

    for module, desc in module_docs.items():
        lines.append(f"- **`{module}`** — {desc}")

    out = PROJECT_ROOT / output_path
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Structure docs written to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate project documentation")
    parser.add_argument("--schema", action="store_true", help="Export OpenAPI JSON schema")
    parser.add_argument("--api", action="store_true", help="Generate API Markdown docs")
    parser.add_argument("--structure", action="store_true", help="Generate structure docs")
    parser.add_argument("--all", action="store_true", help="Generate all docs")
    args = parser.parse_args()

    if args.all or args.schema:
        export_openapi_schema()
    if args.all or args.api:
        generate_api_markdown()
    if args.all or args.structure:
        generate_structure_markdown()

    if not any(vars(args).values()):
        parser.print_help()
