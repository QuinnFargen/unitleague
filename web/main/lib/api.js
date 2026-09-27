const API_URL = process.env.API_URL ?? "https://api.unitleague.com";

export async function getLeagues() {
  const res = await fetch(`${API_URL}/mart/league`, { next: { revalidate: 300 } });
  if (!res.ok) throw new Error(`GET /mart/league failed: ${res.status}`);
  return res.json();
}
