import LeagueTable from "@/components/LeagueTable";

export default function Home() {
  return (
    <main>
      <p className="tagline">A fantasy alternative to sports betting. Fake units, real standings.</p>
      <h2>Leagues</h2>
      <LeagueTable />
    </main>
  );
}
