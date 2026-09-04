# Contributing

Thank you for considering contributing.

This is an unofficial, community-maintained project. It is not affiliated with
or endorsed by the upstream Absurd project. Maintenance, reviews, and releases
are provided on a best-effort basis, so response times may vary.

## Before you start

- Search the existing issues and pull requests to avoid duplicating work.
- Open an issue before starting a large feature or public API change so the
  direction can be discussed first.
- Report problems with the PostgreSQL schema or Absurd itself to the
  [upstream project](https://github.com/earendil-works/absurd).
- Report Elixir API, Postgrex integration, documentation, or OTP worker issues
  in this repository.

## Submit a pull request

1. Fork the repository on GitHub.
2. Create a branch in your fork for one focused change.
3. Install dependencies and make your change:

   ```console
   mix deps.get
   ```

4. Run the project checks:

   ```console
   mix check
   ```

5. Push the branch to your fork and open a pull request against `main`.

In the pull request, explain what changed, why it is useful, and any behavior or
compatibility tradeoffs reviewers should know about. Add focused tests for code
changes and update public documentation when behavior changes.

Please keep unrelated changes in separate pull requests. Small, reviewable
changes are easier to evaluate and more likely to be merged quickly.

## Integration tests

Most checks run without a database. Tests tagged `:postgresql` require
PostgreSQL 16 with the supported Absurd `0.5.0` schema installed and this
environment variable set:

```console
export ABSURD_INTEGRATION_DATABASE_URL=postgres://postgres:postgres@localhost:5432/absurd_test
mix test --only postgresql
```

The schema URL and checksum in `.github/workflows/ci.yml` are the canonical
versions used by CI. PostgreSQL integration tests are skipped when the database
URL is absent.

## Reporting a bug

A useful bug report includes:

- the Elixir and OTP versions;
- the Absurd Elixir SDK and PostgreSQL schema versions;
- a small reproduction or failing test;
- the expected and actual behavior; and
- relevant logs with credentials, task payloads, and other sensitive data
  removed.

## Project direction

The project aims to remain a focused Elixir client and OTP worker implementation
for Absurd rather than a separate workflow engine. See
[`ARCHITECTURE.md`](ARCHITECTURE.md) for the current boundaries and execution
model.

By opening a pull request, you agree that your contribution is provided under
the project's Apache-2.0 license.
