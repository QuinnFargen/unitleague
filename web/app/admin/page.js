import Header from "@/components/Header";
import LeagueTable from "@/components/LeagueTable";

export const metadata = { title: "UNIT League Admin" };

export default function AdminHome() {
  return (
    <main>
      <Header label="Admin" />
      <h2>Leagues</h2>
      <LeagueTable />
    </main>
  );
}
