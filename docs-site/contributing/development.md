# Development Setup

```bash
git clone https://github.com/zhbxlm/async-scheduler-framework.git
cd async-scheduler-framework
pip install -e ".[metrics,dev]"
pre-commit install
```

## Running Tests

```bash
pytest tests/ -q
```

See [Testing](testing.md) for full details.
