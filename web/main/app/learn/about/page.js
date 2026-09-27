import Link from "next/link";
import Term from "@/components/learn/Term";

export const metadata = { title: "About" };

export default function Page() {
  return (
    <>
      <h1>About UNIT League</h1>
      <p className="lede">
        A fantasy-style betting league where the only thing on the line is bragging rights.
      </p>

      <p>
        UNIT League lets you and your friends make picks on real games, at real odds, and track who
        comes out ahead over a season. You never wager money, and you can never win or lose money
        on a pick. Everyone plays with <Term id="unit">units</Term> instead.
      </p>

      <h2>Units, not dollars</h2>
      <p>
        A <Term id="unit">unit</Term> is a standard bet size. Professional bettors think in units
        so their results compare across bankrolls. A “+12 unit season” means the same thing whether
        a unit is $1 or $1,000. UNIT League keeps the idea and drops the money:
      </p>
      <ul>
        <li>Every runner in a <Term id="syndicate">syndicate</Term> starts with the same number of units.</li>
        <li>You stake units on <Term id="moneyline">moneylines</Term>, <Term id="spread">spreads</Term>, <Term id="total">totals</Term>, and <Term id="parlay">parlays</Term> at real market odds.</li>
        <li>Winning bets add units and losing bets subtract them. Most units at the end wins.</li>
        <li>Units can't be bought, sold, withdrawn, or exchanged for anything.</li>
      </ul>
      <p>
        You get the fun parts: the research, the sweat, the group chat trash talk. You don't get the
        part where a bad Sunday costs you rent. It also creates an honest record. Over a season you
        will see how hard it is to beat the <Term id="vig">vig</Term>, and how rarely parlays hit.
      </p>

      <h2>What is a sportsbook?</h2>
      <p>
        A <Term id="sportsbook">sportsbook</Term> (or “book”) sets the odds and takes your bet
        directly. It is always the other side of your wager. Books don't need to predict games
        perfectly. They build a fee, the <Term id="vig">vig</Term>, into every price so that across
        millions of bets they keep a slice of the <Term id="handle">handle</Term> no matter who
        wins.
      </p>
      <p>
        Because the book is your opponent, it also controls the rules: which bets it takes, how
        much it lets you bet, and whether it keeps your account open. See{" "}
        <Link href="/learn/danger">Danger</Link> for how that plays out.
      </p>

      <h2>What is a prediction market?</h2>
      <p>
        A <Term id="prediction-market">prediction market</Term> is an <Term id="exchange">exchange</Term>. Instead of betting
        against the house, you buy and sell <Term id="event-contract">contracts</Term> with other traders. A contract pays out 1 if
        an event happens and 0 if it doesn't, so a price of 0.62 means the market puts the chance at
        about 62%.
      </p>
      <ul>
        <li>The exchange makes money from <Term id="maker-taker">fees</Term> and <Term id="bid-ask">spreads</Term> on every trade, not from you losing.</li>
        <li>
          Much of the action comes from <Term id="pm-market-maker">market makers</Term>: firms that
          post buy and sell orders on both sides so there's always someone to trade with. The big
          sportsbooks are getting into prediction markets through this role, so the house can still
          end up on the other side of your trade.
        </li>
        <li>Winners generally aren't <Term id="limiting">limited</Term> the way they are at books. Your opponent is another trader.</li>
        <li>It is still real money. Fees cut into every trade, and most traders still lose to better-informed ones.</li>
      </ul>

      <table className="compare">
        <thead>
          <tr><th></th><th>Sportsbook</th><th>Prediction market</th><th>UNIT League</th></tr>
        </thead>
        <tbody>
          <tr><td>Who you bet against</td><td>The house</td><td>Other traders</td><td>Not betting</td></tr>
          <tr><td>How it makes money</td><td>Vig and hold</td><td>Trading fees</td><td>Subscriptions &amp; donations</td></tr>
          <tr><td>Real money at risk</td><td>Yes</td><td>Yes</td><td>No</td></tr>
          <tr><td>Limits winners</td><td>Yes</td><td>Rarely</td><td>Never</td></tr>
        </tbody>
      </table>

      <div className="callout">
        <p>
          Ready for the details? <Link href="/learn/unit-league">Unit League</Link> covers the
          syndicate types, how a season runs, and the Juice you can add to your picks.
        </p>
      </div>
    </>
  );
}
