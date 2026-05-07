# Code Style

- **Formatter**: black (line length 100)
- **Linter**: flake8
- **Type checker**: mypy (optional, non-blocking)
- **Security scanner**: bandit

```bash
black src/ tests/
flake8 src/ tests/
mypy src/ --ignore-missing-imports
bandit -r src/ -ll
```
