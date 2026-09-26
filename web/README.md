# UnitLeague Web

One Next.js app serving both sites:

| Host | Routes |
|------|--------|
| `unitleague.com` | `app/page.js` (and everything outside `app/admin`) |
| `admin.unitleague.com` | `app/admin/*` — `proxy.js` rewrites by host |

Data comes from the FastAPI service (`API_URL`, defaults to `https://api.unitleague.com`).

## Run

Requires Node 20.9+.

```bash
npm install
npm run dev
```

- Base site: http://localhost:3000
- Admin site: http://admin.localhost:3000

To point at a local API: `API_URL=http://localhost:8000 npm run dev`.

## Deploy

Deploy the `web/` directory (e.g. Vercel with root directory `web`) and attach both `unitleague.com` and `admin.unitleague.com` to the same project.

Note: the admin site has no auth yet.
