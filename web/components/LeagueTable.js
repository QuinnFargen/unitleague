import { getLeagues } from "@/lib/api";

export default async function LeagueTable() {
  let leagues;
  try {
    leagues = await getLeagues();
  } catch (err) {
    return <p className="error">Could not load leagues.</p>;
  }

  return (
    <table>
      <thead>
        <tr>
          <th>League</th>
          <th>Name</th>
          <th>Status</th>
          <th>Season start</th>
        </tr>
      </thead>
      <tbody>
        {leagues.map((l) => (
          <tr key={l.league_id}>
            <td><strong>{l.abbr}</strong></td>
            <td>{l.name}</td>
            <td>{l.status}</td>
            <td>{l.season_start_dt ?? "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
