# UnitLeague Web

Two independent Next.js apps:

| Folder | Domain | Dev URL |
|--------|--------|---------|
| `main/` | `unitleague.com` | http://localhost:3000 |
| `admin/` | `admin.unitleague.com` | http://localhost:3001 |

Both read from the FastAPI service (`API_URL`, defaults to `https://api.unitleague.com`).

## Run

Requires Node 20.9+.

```bash
cd main   # or admin
npm install
npm run dev
```

## Deploy

Deploy each folder as its own project (e.g. Vercel root directory `web/main` and `web/admin`) and attach its domain.

Note: the admin app has no auth yet.
