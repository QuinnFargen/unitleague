import Link from "next/link";
import Term from "@/components/learn/Term";

export const metadata = { title: "Danger" };

// -110 on every leg, each leg a true 50/50.
const PARLAYS = [
  { legs: 1, chance: "50%", fair: "+100", paid: "-110", ev: "-4.5%" },
  { legs: 2, chance: "25%", fair: "+300", paid: "+264", ev: "-8.9%" },
  { legs: 3, chance: "12.5%", fair: "+700", paid: "+596", ev: "-13.0%" },
  { legs: 4, chance: "6.25%", fair: "+1500", paid: "+1228", ev: "-17.0%" },
  { legs: 6, chance: "1.56%", fair: "+6300", paid: "+4741", ev: "-24.4%" },
  { legs: 10, chance: "0.10%", fair: "+102300", paid: "+64208", ev: "-37.2%" },
];

export default function Page() {
  return (
    <>
      <h1>The Danger</h1>
      <p className="lede">
        Sportsbooks are some of the most profitable businesses around. Here is how they make sure
        it stays that way.
      </p>

      <div className="callout">
        <p>
          The short version: the math is tilted against you on every bet. If you manage to beat it
          anyway, the book can cut you off.
        </p>
      </div>

      <h2>1. The vig is built into every price</h2>
      <p>
        Take a coin-flip game where both sides are listed at <Term id="american-odds">-110</Term>.
        You risk $110 to win $100 on either side. If the book takes $110 on each side, it collects
        $220 and pays the winner $210. The book keeps $10 no matter who wins. That's the{" "}
        <Term id="vig">vig</Term>.
      </p>
      <div className="math">{`Break-even win rate at -110  =  110 / (110 + 100)  =  52.4%
Expected value per bet at 50% =  0.5 × 1.909 − 1     =  −4.5%`}</div>
      <p>
        That's the <Term id="break-even">break-even rate</Term>. A bettor who is right exactly half the time slowly loses. Even a very good bettor who
        wins 55% of their -110 bets only earns about 5% per bet, and very few people sustain 55%
        over thousands of bets. The <Term id="implied-probability">implied probabilities</Term> of
        the two sides add up to 104.8%, not 100%. That extra 4.8%, the <Term id="overround">overround</Term>, is the book's cut.
      </p>

      <h2>2. Parlays multiply the book's edge</h2>
      <p>
        A <Term id="parlay">parlay</Term> looks like a bargain: a small stake for a big payout. But
        the book takes its cut on every leg, and those cuts compound. Here is what happens when
        every leg is a true 50/50 priced at -110:
      </p>
      <table>
        <thead>
          <tr><th>Legs</th><th>Chance to hit</th><th><Term id="fair-odds">Fair</Term> payout</th><th>Book pays</th><th>Your <Term id="expected-value">EV</Term></th></tr>
        </thead>
        <tbody>
          {PARLAYS.map((p) => (
            <tr key={p.legs}><td>{p.legs}</td><td>{p.chance}</td><td>{p.fair}</td><td>{p.paid}</td><td>{p.ev}</td></tr>
          ))}
        </tbody>
      </table>
      <p>
        A 4.5% house edge on a single bet becomes 17% on a 4-leg parlay and over 37% on a 10-leg
        parlay. That's why parlays and <Term id="sgp">same-game parlays</Term> are where books make most of their money.
        Parlay <Term id="hold">hold</Term> regularly runs 20-30%, compared with about 5% on straight
        bets. It's also why books advertise the one-in-a-thousand ticket that hit, never the 999
        that didn't.
      </p>

      <h2>3. Promos are marketing, not gifts</h2>
      <ul>
        <li>
          <strong><Term id="bonus-bet">Bonus bets</Term></strong> usually pay only the winnings, not the stake. A $100 bonus
          bet at even odds is worth about $50 in expectation, not $100.
        </li>
        <li>
          <strong>Deposit matches</strong> come with <Term id="playthrough">playthrough</Term> requirements. You often have to
          wager the bonus many times over before you can withdraw it, paying vig each time.
        </li>
        <li>
          <strong><Term id="promo">Odds boosts</Term></strong> are usually applied to parlays or
          long shots where the hold is already high, so the “boosted” price is often still below
          fair.
        </li>
        <li>
          <strong>VIP hosts</strong> go to the customers who lose the most, with free tickets,
          trips, and credit to keep them betting.
        </li>
      </ul>

      <h2>4. How sportsbooks stop winners</h2>
      <p>
        A book can decline any bet and close any account. Every account is tied to your real
        identity through <Term id="kyc">KYC</Term>, and your betting behavior is scored
        continuously. Losers get promos. Winners get limited. These are some of the common ways
        they find winners and shut them down.
      </p>

      <h3>Top-down betting gets limited fast</h3>
      <p>
        Top-down betting means using the prices at sharp, high-limit books (<Term id="market-maker">market makers</Term>) as the “true” odds, then
        betting stale or mispriced lines at slower recreational books (<Term id="soft-book">soft books</Term>) before they move. It's one of
        the few repeatable ways to beat the vig, and books know it. If your bets keep landing just
        before the line moves your way (<Term id="steam">steam</Term>) and you keep beating the{" "}
        <Term id="clv">closing line</Term>, your account gets flagged as{" "}
        <Term id="sharp">sharp</Term>. Your <Term id="max-bet">max bet</Term> can drop from thousands to a few dollars,
        often within weeks and sometimes after a handful of bets. That's{" "}
        <Term id="limiting">limiting</Term>, and it usually applies to your whole account.
      </p>

      <h3>Bet sizing that's too specific</h3>
      <p>
        Recreational bettors bet round numbers: $20, $50, $100. Bettors using a formula to size
        their stakes (for example, the <Term id="kelly-criterion">Kelly criterion</Term>) end up with amounts like $137.42, or bet
        exactly the posted maximum. Odd amounts, max bets placed right after a line opens, and
        stakes that grow with your edge all tell the book's risk team you're calculating rather
        than playing. That can be enough to get limited even before you've won much.
      </p>

      <h3>Round robin parlays</h3>
      <p>
        A <Term id="round-robin">round robin</Term> turns a handful of picks into every possible
        smaller parlay. Four picks in 2-leg combinations becomes six parlays. For a casual bettor
        it's a quiet way to multiply the vig: six tickets means paying the parlay edge six times.
        When a sharp builds round robins out of +EV legs, the book sees it quickly. Common
        responses include lower round robin limits, removing the option from the account, or
        limiting the account entirely.
      </p>

      <h3>Bearding: using someone else's account</h3>
      <p>
        Once limited, some bettors turn to <Term id="bearding">bearding</Term>: placing bets
        through a friend's or relative's account, or paying someone to open one. Books look for
        shared devices, IP addresses, payment methods, and betting patterns that match a limited
        account. If they catch it, they can void the bets, seize the balance, and <Term id="account-closure">ban</Term> everyone
        involved.
      </p>
      <div className="callout">
        <p>
          <strong>This is a crime, not a loophole.</strong> Opening or using an account under
          someone else's identity to get around a book's restrictions can be prosecuted as fraud,
          and in many states it's a felony. The person who lends their name is exposed too.
        </p>
      </div>

      <h3>Other tools books use</h3>
      <ul>
        <li><strong>Voided bets</strong>: books can cancel winning bets they call a <Term id="palp">“palpable error”</Term> in the line.</li>
        <li><strong>Withdrawal friction</strong>: extra verification, pending periods, and reversal windows make it easy to re-bet winnings instead of cashing out.</li>
        <li><strong>Lower limits where you're good</strong>: player props and niche markets often have much lower limits and higher hold than main lines.</li>
        <li><strong>Line delays</strong>: live bets may be held for a few seconds, then rejected if the price has moved against the book.</li>
      </ul>

      <h2>5. The human cost</h2>
      <p>
        Mobile betting puts a casino in your pocket, open 24/7, with instant deposits and a
        feed of personalized promos. <Term id="chasing">Chasing</Term> losses, betting more to feel the same rush, and hiding
        how much you bet are warning signs of a gambling problem.
      </p>
      <div className="callout">
        <p>
          If betting is causing harm for you or someone you know, call or text{" "}
          <strong>1-800-MY-RESET</strong> (US) for free, confidential help. See{" "}
          <Link href="/responsible-play">Responsible Play</Link> for more.
        </p>
      </div>

      <p>
        Want the thrill without any of this? That's what{" "}
        <Link href="/learn/unit-league">UNIT League</Link> is for. Not sure yet? Run through the{" "}
        <Link href="/learn/decision">Decision</Link> walkthrough.
      </p>
    </>
  );
}
