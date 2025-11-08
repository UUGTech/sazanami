# Contributing to Sazanami

Thanks for spending time on Sazanami! Below is a quick guide to get you started.

## Reporting bugs / requesting features
- Please open an Issue with as much detail as possible: what behavior you saw, what you expected, and how to reproduce it.
- If you have a proposal, describe the use case, why existing APIs are insufficient, and any sketch of the API you have in mind.

## Pull requests
1. **Fork & branch**: create a feature branch (`git checkout -b feature/my-change`).
2. **Coding style**: run `gofmt ./...` before committing. The CI will also check formatting.
3. **Tests**: add tests for new behavior; make sure the suite passes locally.
   ```bash
   go test ./...
   go test -race ./...
   ```
4. **Docs & examples**: update README / examples if your change affects public APIs.
5. **PR checklist**:
   - describe the change and the motivation
   - mention related issues ("Fixes #123") if applicable
   - keep commits focused and rebased on main

## Coding guidelines
- The library targets Go 1.22+, uses only the standard library, and embraces small, composable helpers. Keep APIs explicit and avoid hidden global state.
- Prefer returning errors over panicking; policies (`Drop`, `Retry`, `Collect`, etc.) are the preferred way to control error handling.
- Hooks (`StageRecorder`, `CollectFailuresFunc`, etc.) should remain lightweight so they can be used in production or tests without extra deps.

If you are unsure about anything, feel free to open an Issue to discuss before investing a lot of time. Thanks!
