import Link from "next/link";
import Term from "@/components/learn/Term";

export const metadata = { title: "Pricing" };

const PLANS = [
  { name: "First syndicate", price: "Free", body: "Everyone can join one syndicate at no cost." },
  { name: "Season", price: "$1", body: "Each sports season is $1 for unlimited syndicates." },
  { name: "Data", price: "$?", body: "Extra costs for proprietary data and edge for placing picks." },
];

export default function Page() {
  return (
    <>
      <h1>Pricing</h1>

      <div className="callout">
        <p>
          <strong>Everything is free for now.</strong> No payment system is set up yet. The prices
          below are what we're considering and may change.
        </p>
      </div>

      <p className="lede">
        Playing is free. Fees only cover extra <Term id="syndicate">syndicates</Term> and extras,
        and they never become <Term id="unit">units</Term>, winnings, or anything you can cash out.
      </p>

      <table>
        <thead>
          <tr><th>What</th><th>Price</th><th>Details</th></tr>
        </thead>
        <tbody>
          {PLANS.map(({ name, price, body }) => (
            <tr key={name}><td><strong>{name}</strong></td><td>{price}</td><td>{body}</td></tr>
          ))}
        </tbody>
      </table>

      <h2>Win a syndicate, play the next one free</h2>
      <p>
        Finish first in a syndicate and you get a coupon for a free entry into another syndicate.
        The prize is more play, never money.
      </p>

      <h2>Why charge anything?</h2>
      <p>
        Unit League doesn't take a cut of any bet. Small entry fees, subscriptions, and donations
        cover the cost of running it.
      </p>

      <p>
        New here? See <Link href="/learn/unit-league">how Unit League works</Link>, or read the{" "}
        <Link href="/learn/faq">FAQ</Link>.
      </p>
    </>
  );
}
