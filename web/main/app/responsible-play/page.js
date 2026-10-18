import Link from "next/link";
import { SUPPORT_EMAIL } from "@/lib/legal";

export const metadata = { title: "Responsible Play" };

// TODO(legal): re-check these organizations and phone numbers before launch.
const RESOURCES = [
  {
    name: "National Council on Problem Gambling",
    href: "https://www.ncpgambling.org/",
    body: "Free, confidential help 24/7 by phone, text, or chat, plus local treatment and support listings.",
  },
  {
    name: "Gamblers Anonymous",
    href: "https://www.gamblersanonymous.org/",
    body: "Peer support meetings, in person and online, for people who want to stop gambling.",
  },
  {
    name: "Gam-Anon",
    href: "https://www.gam-anon.org/",
    body: "Support for the family and friends of someone with a gambling problem.",
  },
];

const SIGNS = [
  "Betting more than you planned, or more than you can afford to lose.",
  "Chasing losses with bigger bets.",
  "Hiding how much you gamble from people close to you.",
  "Borrowing money or missing bills because of gambling.",
  "Feeling restless or irritable when you try to cut back.",
  "Gambling to escape stress, boredom, or bad feelings.",
];

export default function Page() {
  return (
    <main>
      <article className="learn-content">
        <h1>Responsible Play</h1>
        <p className="lede">
          Unit League is a game. No real money is wagered, units have no cash value, and nothing
          you win can be cashed out.
        </p>

        <div className="callout">
          <p>
            <strong>If gambling is causing you problems, help is free and confidential.</strong>{" "}
            Call or text <a href="tel:18006973738">1-800-MY-RESET (1-800-697-3738)</a>, available
            24/7.
          </p>
          <p>
            If you are in crisis or thinking about hurting yourself, call or text{" "}
            <a href="tel:988">988</a> (Suicide &amp; Crisis Lifeline).
          </p>
        </div>

        <h2>Why we care</h2>
        <p>
          Unit League exists so you can enjoy picking games without risking money. But practice
          with fake units can make real betting feel easier than it is. Winning streaks don't
          predict real results. Read more in <Link href="/learn/danger">The Danger</Link>.
        </p>

        <h2>Warning signs</h2>
        <p>If gambling anywhere shows up in your life like this, it may be time to get help:</p>
        <ul>
          {SIGNS.map((s) => <li key={s}>{s}</li>)}
        </ul>

        <h2>Self-help tips</h2>
        <ul>
          <li>Set a time limit for playing, and stick to it.</li>
          <li>Take breaks. Step away for a few days if picks start to feel stressful.</li>
          <li>Never treat game results as a reason to bet real money.</li>
          <li>Talk to someone you trust about how you're feeling.</li>
          <li>
            If you bet with real money elsewhere, use your sportsbook's deposit limits, time-outs,
            or self-exclusion tools, or your state's self-exclusion program.
          </li>
        </ul>

        <h2>Where to get help</h2>
        <div className="card-grid">
          {RESOURCES.map(({ name, href, body }) => (
            <a key={name} className="card" href={href} target="_blank" rel="noopener noreferrer">
              <h3>{name}</h3>
              <p>{body}</p>
            </a>
          ))}
        </div>

        <h2>Taking a break from Unit League</h2>
        <p>
          You can leave any syndicate at any time, or{" "}
          <Link href="/delete-account">delete your account</Link>. If you'd like help stepping away,
          email <a href={`mailto:${SUPPORT_EMAIL}`}>{SUPPORT_EMAIL}</a>.
        </p>

        <h2>18+ only</h2>
        <p>
          Unit League is for adults 18 and older. If you think a minor is using Unit League, please
          let us know and we'll remove the account.
        </p>
      </article>
    </main>
  );
}
