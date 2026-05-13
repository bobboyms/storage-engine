@AGENTS.md

## Claude Code Specifics

- The project skill `enforce-tdd-coverage` lives at `.claude/skills/enforce-tdd-coverage/SKILL.md` and should auto-trigger for any work touching `*.go`, `go.mod`, `go.sum`, or `Makefile`. If it does not load on its own, invoke it with `/enforce-tdd-coverage` before editing production code.
- Do not run destructive git operations (`reset --hard`, `push --force`, branch deletion, `clean -f`) without an explicit user request. Confirm before any action that affects shared state.
- Never use `--no-verify` or skip pre-commit hooks. If a hook rewrites files, re-stage and create a new commit — do not amend.
- Treat `make lint` (0 issues) and `make vuln` (0 affecting vulns) as blocking before reporting any implementation task as complete, alongside the TDD + 70% coverage gate.
- Use `/tmp` for coverage profiles and any temporary artifacts; never leave them in the repo.
