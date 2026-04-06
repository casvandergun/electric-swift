# Contributing to ElectricSwift

Thanks for taking the time to improve ElectricSwift. This project is a Swift
package for consuming ElectricSQL shape streams on Apple platforms, and
contributions are welcome when they keep the package reliable, focused, and
easy to adopt.

## Ways to Contribute

- Report bugs with a clear reproduction, expected behavior, actual behavior,
  and relevant platform or Swift toolchain details.
- Suggest focused improvements to the API, streaming behavior, decoding,
  snapshot handling, or documentation.
- Open pull requests for bug fixes, tests, documentation, or small feature
  additions.

For larger API or behavior changes, please open an issue first. It is much
easier to align on the design before a large patch exists.

## Development Setup

ElectricSwift uses Swift Package Manager.

Requirements:

- Swift 6.0 or newer
- macOS 14+ for local macOS test runs
- Xcode or a compatible Swift toolchain

Clone the repository and run:

```sh
swift build
swift test
```

The package currently supports iOS 17+ and macOS 14+.

## Branch and Pull Request Strategy

`main` is the release branch. It should stay buildable and testable at all
times.

Use short-lived branches from `main`:

- `fix/snapshot-replay`
- `feat/dynamic-params`
- `docs/readme-installation`
- `test/shape-stream-retry`

Pull requests should target `main`.

Before opening a pull request:

1. Rebase or merge the latest `main`.
2. Keep the change focused on one problem or feature.
3. Add or update tests for behavior changes.
4. Update documentation when public API or usage changes.
5. Run `swift test`.

Maintainers will usually squash-merge PRs after review and passing CI. Avoid
long-running branches unless a maintainer agrees that the work needs to land in
stages.

## Code Guidelines

- Prefer small, explicit changes over broad refactors.
- Match the existing Swift style and naming.
- Keep public API additions minimal and documented through tests or README
  updates.
- Avoid introducing dependencies unless there is a strong reason.
- Preserve Swift concurrency safety. Address `Sendable` and actor-isolation
  diagnostics rather than silencing them.
- Keep async tests deterministic. Prefer explicit synchronization or test
  transport behavior over timing-based sleeps.

## Testing Guidelines

Use Swift Testing for new unit tests.

Run the full suite before submitting:

```sh
swift test
```

If a test relies on async streaming behavior, avoid scheduler-sensitive FIFO
assumptions. Test transports should match the request being exercised, and
timeouts should be safety guards rather than the main synchronization
mechanism.

Integration tests that require a real Electric endpoint should remain skipped
by default unless the test clearly documents the required environment.

## Pull Request Review

A good pull request includes:

- A short description of the problem and solution.
- Notes about any public API changes.
- Test coverage for new behavior or bug fixes.
- Any follow-up work that is intentionally left out of scope.

Maintainers may ask for changes to API shape, tests, naming, or documentation.
Please keep review discussion focused on the patch at hand.

## Releases

Releases are created from `main` after CI passes. Release tags use semantic
versioning with a `v` prefix, for example:

```sh
v0.1.0
```

Until `1.0.0`, API changes may still happen as the package evolves. Even so,
contributors should treat public API changes carefully and explain migration
impact in the pull request.

## Security

Do not open public issues for vulnerabilities or sensitive reports. Contact the
maintainer privately so the issue can be handled before details are shared.

Do not include secrets, tokens, private endpoints, or customer data in issues,
pull requests, tests, or logs.
