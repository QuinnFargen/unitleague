import Link from "next/link";

export const metadata = { title: "FAQ" };

const FAQS = [
  {
    q: "Is UNIT League gambling?",
    a: <>No. You never wager or win money on a pick. Units have no cash value and can't be bought, sold, or withdrawn.</>,
  },
  {
    q: "Does it cost anything?",
    a: <>You can play on your own, or in one syndicate, for free. Joining more syndicates will cost a small $1 fee per runner. Fees never turn into units or winnings. See <Link href="/learn/pricing">Pricing</Link>.</>,
  },
  {
    q: "Where do the odds come from?",
    a: <>We use real market odds from sportsbooks, so your picks face the same prices, including the <Link href="/learn/terms#vig">vig</Link>, that a real bettor would.</>,
  },
  {
    q: "How do I start or join a syndicate?",
    a: <>Create one and share its code, or join a public syndicate. Private syndicates need the code and, if set, a password. See <Link href="/learn/unit-league">Unit League</Link>.</>,
  },
  {
    q: "What's Juice?",
    a: <>Enhancements you add to your runner to boost odds, stakes, or balance. Unlike sportsbook juice, ours works in your favor. See <Link href="/learn/unit-league">Unit League</Link>.</>,
  },
  {
    q: "Will UNIT League help me win at a real sportsbook?",
    a: <>That isn't the goal. It shows how hard winning is, and <Link href="/learn/danger">Danger</Link> explains why books limit anyone who does win.</>,
  },
  {
    q: "Who can play?",
    a: <>See our <Link href="/terms">Terms of Service</Link> for eligibility. Real-money sportsbooks require you to be 21+ in most US states.</>,
  },
];

export default function Page() {
  return (
    <>
      <h1>FAQ</h1>
      <p className="lede">Quick answers. Still stuck? <Link href="/contact">Contact us</Link>.</p>

      <div className="faq">
        {FAQS.map(({ q, a }) => (
          <details key={q}>
            <summary>{q}</summary>
            <p>{a}</p>
          </details>
        ))}
      </div>
    </>
  );
}
