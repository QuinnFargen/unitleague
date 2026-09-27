const API_URL = process.env.API_URL ?? "https://api.unitleague.com";

export const dynamic = "force-dynamic";

export default async function AdminHome() {
  const leagues = await fetch(`${API_URL}/mart/league`).then((r) => r.json());

  return (
    <main>
      <img src="/logo-black.png" alt="UNIT League" height={40} />
      <h1>Admin</h1>
      <h2>Leagues</h2>
      <table border="1" cellPadding="6" style={{ borderCollapse: "collapse" }}>
        <thead>
          <tr><th>ID</th><th>Abbr</th><th>Name</th><th>Status</th></tr>
        </thead>
        <tbody>
          {leagues.map((l) => (
            <tr key={l.league_id}>
              <td>{l.league_id}</td><td>{l.abbr}</td><td>{l.name}</td><td>{l.status}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  );
}
