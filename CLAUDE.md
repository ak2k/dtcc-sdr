# Agent instructions

Contract for agents working in this repo. Read first; overrides defaults.

## Stack (one per concern; substitutes are bans)

| Concern | Use | Not |
|---|---|---|
| Package manager | `uv` | pip, poetry, pipenv, pyenv |
| Lint / format | `ruff` + `ruff format` | black, isort, flake8, pylint |
| Type checker | `basedpyright` strict | mypy, pyright |
| Boundary validation | Pydantic v2 (`extra="forbid"`, `frozen=True`) | dataclasses, attrs, TypedDict |
| HTTP | `httpx` | requests, aiohttp, urllib |
| Async runtime | `anyio` | raw asyncio |
| Logging | `structlog` | stdlib logging, `print()` |
| Paths | `pathlib.Path` | `os.path` |
| Tests | `pytest` + `hypothesis` | unittest |
| Errors | subclass `sec_nport.errors.AppError` | bare `Exception`, string errors |

## When you need it, use

Not every project hits these concerns. When yours does, this is the
choice that fits the rest of the stack — don't substitute.

| Concern | Use |
|---|---|
| Retry / backoff | `stamina` |
| CLI framework | `typer` (paired with `rich` for human-facing output) |
| Time injection in tests | `time-machine` |
| HTTP rate limiting (outbound) | `aiolimiter` |
| SQL | `sqlalchemy 2.0` Core (ORM only when session identity-map earns its keep) |
| Async file I/O | `anyio.Path` |

## Inner loop

```
make fix     # autofix + full check
make check   # full check (CI runs this)
```

Both must be green. `filterwarnings = ["error"]` and `xfail_strict = true`
are load-bearing — deprecation warnings and unexpected passes are real
failures, not noise. Nix users: `nix develop` first; everything else is identical.

## Principles

1. **Boundaries fail loudly.** Pydantic at every external edge with
   `extra="forbid"` unless an explicit comment justifies otherwise. Domain
   errors wrap third-party exceptions — `httpx.RequestError` never leaks
   to a caller. Shape drift is an alarm, not a silent fallthrough.

2. **Suppress with cost.** Every `# pyright: ignore[code]` and `# noqa: code`
   names the specific rule and a one-line reason. Suppression is annotated
   debt — visible, greppable, justified — not a workaround.

3. **Copy the canonical example.** Inventing a new shape for any of these
   is a deliberate choice. The defaults are:
   - service: `src/sec_nport/example_service.py`
   - config:  `src/sec_nport/config.py`
   - logging: `src/sec_nport/log.py`
   - test:    `tests/test_example_service.py`
   - error:   `src/sec_nport/errors.py`

4. **Diverge with a comment.** When tuning a default below (or deviating
   from the stack table), leave `# DIVERGE: <reason>` so future readers
   don't "fix" it back.

5. **Ask when guessing.** Unknown boundary shape, irresolvable type error,
   missing test infrastructure → ask. Don't invent the shape, don't
   suppress just to make it pass.

6. **Tests assert behavior, not implementation.** `hypothesis` for property
   tests at parsing boundaries; `httpx.MockTransport` for hermetic HTTP;
   `monkeypatch.setenv` for env-driven settings; assert on domain errors,
   not on HTTP status codes leaking through.

7. **Imports declare intent.** `from __future__ import annotations` at the
   top of every module; runtime-only third-party types inside `if
   TYPE_CHECKING:`.

8. **Async runtime is anyio.** Not raw asyncio. Compose with sync at the
   edges via `anyio.from_thread` / `anyio.to_thread`.

## Appropriate divergence

Defaults assume a **strict greenfield service**. These profiles are
legitimate alternatives — tune as a set, not piecemeal.

### Profile A — Distributable library or CLI

| Knob | Default | Tune to |
|---|---|---|
| `requires-python` | `>=3.13` | `>=3.10` (or per support window) |
| Ruff `D*` (docstrings) | enabled | drop for small internal surface |
| `pythonVersion` (basedpyright) | `"3.13"` | match `requires-python` lower bound |
| Coverage `fail_under` | 80 | 60 (CLI argv is hard to cover) |
| `PLR0913` (too many args) | strict | ignore (verb signatures are wide) |

Also: `vulture` for unused public API (gap between ruff and coverage).
Tune `ignore_decorators` for decorator-registered handlers.

For publishing: `hatch-vcs` derives version from git tags; pair with
`.git_archival.txt` + `.gitattributes export-subst` so flake-via-tarball
consumers resolve versions without `.git`. https://github.com/ofek/hatch-vcs

### Profile B — Reverse-engineering / scraping

| Knob | Default | Tune to |
|---|---|---|
| HTTP transport | `httpx` default | `httpx` + `httpx-curl-cffi` transport (fingerprint evasion; keeps the httpx API and `MockTransport` tests) |
| Pydantic `extra` | `"forbid"` | `"ignore"` (no stable upstream schema) |
| `typeCheckingMode` | `"strict"` | `"standard"` + strict per-module on public API |
| `reportMissingTypeStubs` | `"warning"` | `false` |
| `reportAny` | `"warning"` | `"none"` |

### Profile C — Single-file script

Skip the template. PEP 723 inline header:

```python
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx", "structlog"]
# ///
```

Graduate to the template once the script grows past one file or acquires a test.

## Known gotchas (non-obvious from the toolchain)

- `structlog.get_logger()` returns `Any`. Annotate via
  `if TYPE_CHECKING: from structlog.stdlib import BoundLogger`, then suppress
  the RHS with `# pyright: ignore[reportAny]` + reason.
- Use `http.HTTPStatus.NOT_FOUND` — stdlib, well-typed. Not `httpx.codes.NOT_FOUND`
  (mis-typed by httpx as tuple) and not bare `404` (PLR2004).
- `extra="forbid"` Pydantic models raise on any unknown upstream field.
  Intentional: drift fails at the boundary, not silently corrupting downstream.
