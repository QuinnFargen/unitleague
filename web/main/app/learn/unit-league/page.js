import Link from "next/link";
import Term from "@/components/learn/Term";

export const metadata = { title: "Unit League" };

const SYNDICATE_TYPES = [
  {
    name: "Unit",
    body: "Classic bankroll race. Everyone starts with the same units, and the standings are sorted by balance. Highest balance at the end of the season wins.",
  },
  {
    name: "H2H",
    body: "Head-to-head like fantasy football. Each round you're matched against another runner, and whoever gains more units that round takes the win. Standings are by record.",
  },
  {
    name: "Team",
    body: "Runners are grouped into teams, and team totals decide the standings. Good for offices, group chats, or rival friend groups.",
  },
];

const RARITIES = [
  { name: "Dollar", cost: 1, note: "Cheap and common. Small, steady bonuses." },
  { name: "Nickel", cost: 2, note: "Solid upgrades to odds, stake, or balance." },
  { name: "Dime", cost: 3, note: "Stronger effects that often scale or have conditions." },
  { name: "Whale", cost: 5, note: "Rare, build-defining effects, usually with a catch." },
];

export default function Page() {
  return (
    <>
      <h1>Unit League</h1>
      <p className="lede">How syndicates, runners, and Juice fit together.</p>

      <h2>Syndicates and runners</h2>
      <p>
        A <Term id="syndicate">syndicate</Term> is your league. Whoever creates it sets the name,
        the starting <Term id="unit">units</Term>, the maximum number of players, and whether it's
        public or private (joined with a short code and optional password). Each player in a
        syndicate is a <strong>runner</strong>. You can run in several syndicates at once, and each
        one tracks its own balance.
      </p>
      <p>
        Once the admin starts the syndicate, runners place picks on real games at real market odds.
        Results settle automatically when the games finish.
      </p>

      <h2>Syndicate types</h2>
      {SYNDICATE_TYPES.map(({ name, body }) => (
        <div key={name}>
          <h3><span className="badge">{name}</span></h3>
          <p>{body}</p>
        </div>
      ))}

      <h2>Syndicate settings</h2>
      <p>Admins can tune a syndicate to their group:</p>
      <ul>
        <li><strong>Minimum wagers / minimum units</strong>: stop anyone from sitting on a lead by not betting.</li>
        <li><strong>Juice cost multiplier</strong>: 2x, 3x, or 4x the cost of every enhancement.</li>
        <li><strong>Max active Edges</strong>: cap how many Edge cards a runner holds at once.</li>
        <li><strong>Max CLV / Team level</strong>: cap how far those enhancements can be stacked.</li>
        <li><strong>Blocked Juice</strong>: turn off specific enhancements or whole categories.</li>
      </ul>

      <h2>Juice</h2>
      <div className="callout">
        <p>
          At a sportsbook, <Term id="vig">juice</Term> is the fee that works against you. In UNIT
          League we flipped it: <Term id="juice-unit-league">Juice</Term> is the set of
          enhancements that work <em>for</em> you.
        </p>
      </div>
      <p>
        Each round you're offered a few Juice options and can add them to your runner. There are
        three kinds.
      </p>

      <h3>CLV Juice</h3>
      <p>
        Named after <Term id="clv">closing line value</Term>. There is one for each bet type:
        <strong> ML</strong> (<Term id="moneyline">moneyline</Term>),
        <strong> SPR</strong> (<Term id="spread">spread</Term>), and
        <strong> O/U</strong> (<Term id="total">totals</Term>). Leveling one up strengthens your
        bonus on that type of bet. CLV Juice is always on offer.
      </p>

      <h3>Team Juice</h3>
      <p>
        Tie a bonus to teams that share a trait: <strong>Color</strong>, <strong>Region</strong>,{" "}
        <strong>Mascot</strong>, or <strong>Conference</strong>. You pick a team that matches the
        attribute you were offered. Choosing more Team Juice for the same team stacks its level.
      </p>

      <h3>Edge Juice</h3>
      <p>
        Edges are cards with specific effects. Some add to the odds, some add stake or free units,
        and some change the rules of a round. They come in seven flavors:{" "}
        <strong>price</strong>, <strong>unit</strong>, <strong>balance</strong>,{" "}
        <strong>trigger</strong>, <strong>risk</strong>, <strong>CLV</strong>, and{" "}
        <strong>team</strong>. A few examples:
      </p>
      <ul>
        <li><strong>Line Shopper</strong> (price): +0.10 decimal odds on every bet.</li>
        <li><strong>Vig Cutter</strong> (balance): losing bets refund 20% of stake.</li>
        <li><strong>Hot Hand</strong> (trigger): +0.10 odds per consecutive win, resets on a loss.</li>
        <li><strong>Lock of the Day</strong> (risk): one bet gets 2x odds, but costs 15 units if it loses.</li>
        <li><strong>Degenerate</strong> (risk): 3x odds on everything, but you must stake half your balance every round.</li>
      </ul>
      <p>If you hit your syndicate's Edge limit, you sell one you hold to make room.</p>

      <h3>Rarity and cost</h3>
      <p>
        Edges are ranked with betting slang for bet sizes: a “dollar” is $100, a “nickel” $500,
        and a “dime” $1,000. The rarer the Edge, the more it costs.
      </p>
      <table>
        <thead>
          <tr><th>Rarity</th><th>Base cost</th><th>What to expect</th></tr>
        </thead>
        <tbody>
          {RARITIES.map(({ name, cost, note }) => (
            <tr key={name}><td>{name}</td><td>{cost}</td><td>{note}</td></tr>
          ))}
        </tbody>
      </table>

      <div className="callout">
        <p>
          Real books have no Juice that helps you. Every “boost” they offer is priced to keep their
          edge. See <Link href="/learn/danger">Danger</Link> for why.
        </p>
      </div>
    </>
  );
}
