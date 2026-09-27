import { API_URL } from "@/lib/api";

// Forwards API explorer requests to the FastAPI service server-side,
// since the API does not send CORS headers for the admin origin.
async function forward(request, { params }) {
  const { path } = await params;
  const target = new URL(`${API_URL}/${path.map(encodeURIComponent).join("/")}`);
  target.search = request.nextUrl.search;

  const hasBody = !["GET", "HEAD"].includes(request.method);
  const started = performance.now();
  let res;
  try {
    res = await fetch(target, {
      method: request.method,
      headers: hasBody ? { "content-type": "application/json" } : undefined,
      body: hasBody ? await request.text() : undefined,
      cache: "no-store",
    });
  } catch (err) {
    return Response.json({ detail: `Proxy could not reach API: ${err.message}` }, { status: 502 });
  }

  return new Response(res.body, {
    status: res.status,
    headers: {
      "content-type": res.headers.get("content-type") ?? "application/json",
      "x-upstream-ms": String(Math.round(performance.now() - started)),
    },
  });
}

export { forward as GET, forward as POST, forward as PUT, forward as PATCH, forward as DELETE };
