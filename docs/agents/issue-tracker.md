# Issue tracker: GitHub

Issues and specs for this repository live in the fork's GitHub Issues. Infer the repository from `git remote -v` and use `gh` for issue operations.

## Conventions

- Create, read, comment on, label, and close issues with the corresponding `gh issue` commands.
- Fetch comments and labels when a task depends on prior decisions.
- Treat pull requests as implementation surfaces, not feature-request intake.

## Wayfinding

The Wayfinder map is one issue labelled `wayfinder:map`; its child issues are linked sub-issues labelled `wayfinder:research`, `wayfinder:prototype`, `wayfinder:grilling`, or `wayfinder:task`. GitHub native issue dependencies are the canonical blocking relationship. When dependencies are unavailable, use an explicit `Blocked by: #...` line.

Claim the first unassigned, unblocked child in map order. Resolve it with an evidence-bearing comment, close it, and record the resulting decision on the map.
