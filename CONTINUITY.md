Goal (incl. success criteria):
- Implement `.env` hygiene and onboarding:
  - ignore and untrack `.env`,
  - add `.env.example`,
  - update README so new users copy the example and fill their own keys.
- Success criteria:
  - `.env` is no longer tracked.
  - `.env.example` is present and documented in README.
  - Instructions explicitly tell users to paste their own keys into `.env`.

Constraints/Assumptions:
- Do not modify unrelated dirty files in working tree.
- Keep token handling safe (never echo token value in outputs).

Key decisions:
- Apply focused changes only in `.gitignore`, `.env.example`, `README.md`, and this ledger.

State:
- Done: `.env` hygiene + onboarding changes implemented and pushed.
- Now: final handoff.
- Next: none pending.

Done:
- Verified `.env` ignore state:
  - `.gitignore` currently contains no `.env` pattern.
  - `git ls-files --stage -- .env` shows `.env` is tracked.
  - `git status --short -- .env` shows `.env` modified (`M .env`).
  - `git check-ignore` returns no matching ignore rule for `.env`.
- Implemented changes:
  - Added `.env` to `.gitignore`.
  - Added `.env.example` template with required variables and placeholders.
  - Updated README env section to use `cp .env.example .env` and fill local keys.
  - Untracked `.env` with `git rm --cached .env` (local file retained).
- Validation:
  - `make test` -> `7 passed`
  - `make lint` -> `All checks passed!`
- Git:
  - Commit: `72c531a` (`Stop tracking .env and add .env.example onboarding`)
  - Pushed to `origin/main`.

Now:
- Deliver completion summary with file references.

Next:
- None.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files/ids/commands):
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/CONTINUITY.md`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/.gitignore`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/.env`
- Commands used:
  - `nl -ba .gitignore`
  - `git ls-files --stage -- .env`
  - `git check-ignore -v .env`
  - `git status --short -- .env`
  - `git rm --cached .env`
  - `make test && make lint`
  - `git commit -m "Stop tracking .env and add .env.example onboarding"`
  - `git push origin main`
