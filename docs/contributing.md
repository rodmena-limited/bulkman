# Contributing to Bulkman

Contributions are welcome! We value community input to make Bulkman robust and reliable.

## Development Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/bulkman.git
    cd bulkman
    ```

2.  **Install development dependencies:**
    ```bash
    pip install -e ".[dev]"
    ```

## Testing

We aim for high test coverage. Please ensure all tests pass before submitting a PR.

```bash
# Run full test suite with coverage
pytest tests/ --cov=bulkman --cov-report=term-missing
```

## Coding Standards

- **Type Hints:** All code must be fully type-hinted.
- **Linting:** We use `ruff` for linting and formatting.
- **Documentation:** Update documentation for any new features.

## Pull Request Process

1.  Fork the repo and create your branch from `main`.
2.  Add tests for any new functionality.
3.  Ensure the test suite passes.
4.  Submit a Pull Request with a clear description of your changes.
