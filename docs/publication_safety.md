# Publication Safety

This repo is designed to be public-safe. Keep it that way.

Do not commit:

- `.env` or local environment files
- `ops.live.local.json`
- raw, normalized, curated, quarantine, or ops data
- logs
- account IDs
- API keys
- private fee reports
- live-order code
- signed endpoint code
- paper-trading code
- private research plans or model artifacts

Before pushing:

```powershell
git status --short
pytest -q
```

The public repo should remain a collection and data-quality plant only.
