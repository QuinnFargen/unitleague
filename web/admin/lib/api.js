export const API_URL = process.env.API_URL ?? "https://api.unitleague.com";

export async function getOpenApiSpec() {
  const res = await fetch(`${API_URL}/openapi.json`, { cache: "no-store" });
  if (!res.ok) throw new Error(`GET /openapi.json failed: ${res.status}`);
  return res.json();
}
