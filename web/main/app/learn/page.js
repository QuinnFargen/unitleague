import Link from "next/link";
import { LEARN_SECTIONS } from "@/lib/learn";

export const metadata = { title: "Learn" };

export default function Page() {
  return (
    <>
      <h1>Learn</h1>
      <p className="lede">
        UNIT League is built to show how sports betting actually works without putting a dollar
        on the line. Start anywhere below. Each section stands on its own.
      </p>

      <div className="card-grid">
        {LEARN_SECTIONS.map(({ href, label, blurb }) => (
          <Link key={href} href={href} className="card">
            <h3>{label}</h3>
            <p>{blurb}</p>
          </Link>
        ))}
      </div>

      <h2>Where to start</h2>
      <ul>
        <li>New here? Read <Link href="/learn/about">About</Link>, then <Link href="/learn/unit-league">Unit League</Link>.</li>
        <li>Thinking about a real sportsbook? Read <Link href="/learn/danger">Danger</Link> first, then run the <Link href="/learn/decision">Decision</Link> walkthrough.</li>
        <li>Hit a word you don't know? Any dotted-underlined word links to <Link href="/learn/terms">Terms</Link>.</li>
      </ul>
    </>
  );
}
