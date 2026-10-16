# Orbit Cache Redis: development

This package declares support for `>=3.11,<3.15` in `pyproject.toml`. That declaration is distinct from evidence that hosted CI ran.

Use the checks for each language manifest present in this repository:

### Python package

```bash
python -m pip install -e '.[dev]' build
pytest
ruff check src tests
ruff format --check src tests
mypy
```

Optional SDKs and live-service tests are described by the package README. Do not run an opt-in live test against a shared or production service unless its own documentation explicitly supports that use.

The default test suite skips the Redis lock and OAuth adapter modules when their separate capability packages are not installed. To run those tests from a source checkout, install the sibling packages first (for example, `python -m pip install -e ../orbit-lock -e ../orbit-auth -e ../orbit-auth-oauth2`) and install this package with the matching `[lock]` and `[oauth2]` extras. These modules use fakes and do not require a live Redis service.

## Contribution expectations

The current local quality matrix passes on CPython 3.11, 3.12, 3.13, and 3.14. Redis and its lock
and OAuth capability adapters were installed together; unit tests use fake clients and the opt-in
live Redis test was skipped with `ORBIT_REDIS_URL` unset. Ruff lint and format checks, strict mypy,
and wheel/sdist builds pass on each interpreter. The run covered no hosted CI or live Redis service.

Add focused regression coverage for behavior changes. Update the README and relevant guide when a contract, configuration option, security property, lifecycle rule, or operational assumption changes. Report the exact versions and services exercised; local checks do not imply hosted CI or release validation.
