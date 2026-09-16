# Working drafts

The Word documents in this directory are working drafts retained for shared
collaboration. They are not authoritative and are not intended for use,
citation, or release. The files are stored unchanged.

The latest saved manuscript is
[`Hypercap CC NLP Manuscript Sep 16.docx`](Hypercap%20CC%20NLP%20Manuscript%20Sep%2016.docx).
Earlier dated versions are retained for reference.

## Sharing across machines

Word `.docx` files directly in this folder are eligible for tracking. This is a
public repository, including document comments and tracked changes; only add
author-approved drafts without restricted data or private reviewer material.
Word lock files (`~$*`) and recovery files (`*.asd`, `*.wbk`) remain ignored.

Before editing on another machine, commit and push the saved draft from the
first machine, then run `git pull --ff-only` on the receiving machine. Avoid
editing the same draft on both machines at once: Git cannot merge Word content.
After saving changes or adding a draft, update its SHA-256 below before committing:

```bash
shasum -a 256 'working-drafts/Hypercap CC NLP Manuscript Sep 16.docx'
```

## Integrity

- `Hypercap CC NLP Manuscript Sep 16.docx`: SHA-256
  `2fd967391b35a9c9ae0d6c22f24a9143febc143ddb16553d5b463479508e30f9`
- `Hypercap CC NLP Manuscript Aug 29 BL.docx`: SHA-256
  `cee26696b56700bb9525ec4bf88601af42899984a6c38aff75b813e0468b8ef5`
- `Manuscript Aug 14 Draft.docx`: SHA-256
  `4f8f3394ffc2e22f362bd7fdf8f44e63bd3f2c0412034678ad3811d6dd43117d`
- `Cover Letter draft.docx`: SHA-256
  `9604b259645e5a73b8bc831477e34747bd4de7183a517218ef27d6dff6059842`
