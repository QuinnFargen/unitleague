import Link from "next/link";
import { SUPPORT_EMAIL } from "@/lib/legal";

export const metadata = { title: "Download the App" };

const TESTFLIGHT_URL = "https://testflight.apple.com/join/YXHdmKBb";

const REQUIREMENTS = [
  "An iPhone or iPad running iOS / iPadOS 18.5 or later.",
  "An Apple Account signed in to the App Store on that device.",
  "Apple's free TestFlight app (installed in step 1 below).",
  "You must be 18 or older to use Unit League.",
];

export default function Page() {
  return (
    <main>
      <article className="learn-content">
        <h1>Download the App</h1>
        <p className="lede">
          Unit League for iPhone and iPad is in public beta through Apple's TestFlight.
        </p>

        <div className="callout">
          <p>
            <strong>This is beta software.</strong> The app is still being built and tested. Expect
            bugs, crashes, missing features, and changes without notice.
          </p>
          <p>
            Your picks, units, records, and syndicate data may be reset or lost during the beta.
            Beta builds expire after 90 days, so you'll need to update through TestFlight to keep
            playing.
          </p>
        </div>

        <p>
          <a className="btn btn-download" href={TESTFLIGHT_URL} target="_blank" rel="noopener noreferrer">
            Join the beta on TestFlight
          </a>
        </p>

        <h2>What you need</h2>
        <ul>
          {REQUIREMENTS.map((r) => <li key={r}>{r}</li>)}
        </ul>
        <p>Android isn't available yet.</p>

        <h2>How to install</h2>
        <ol>
          <li>
            On your iPhone or iPad, install{" "}
            <a href="https://apps.apple.com/app/testflight/id899247664" target="_blank" rel="noopener noreferrer">
              TestFlight
            </a>{" "}
            from the App Store.
          </li>
          <li>
            On the same device, open the{" "}
            <a href={TESTFLIGHT_URL} target="_blank" rel="noopener noreferrer">beta invite link</a>.
            If you're reading this on a computer, open the link on your phone instead.
          </li>
          <li>Tap <strong>Accept</strong>, then <strong>Install</strong> in TestFlight.</li>
          <li>Open Unit League from your home screen, then sign in or create an account.</li>
        </ol>
        <p>
          If the link says the beta is full or no longer accepting testers, check back later. Spots
          are limited.
        </p>

        <h2>Updates</h2>
        <p>
          New beta builds show up in TestFlight. Turn on automatic updates in TestFlight, or open it
          and tap <strong>Update</strong> next to Unit League.
        </p>

        <h2>Beta disclaimers</h2>
        <ul>
          <li>The app is provided as-is, for testing, with no guarantee of availability or accuracy.</li>
          <li>Features, scoring, and rules may change before the full release.</li>
          <li>
            TestFlight shares crash reports and basic device info with us so we can fix problems.
            See our <Link href="/privacy">Privacy Policy</Link>.
          </li>
          <li>
            Unit League is a game. No real money is wagered and units have no cash value. See{" "}
            <Link href="/responsible-play">Responsible Play</Link>.
          </li>
          <li>
            Use of the app is subject to our <Link href="/terms">Terms</Link>.
          </li>
        </ul>

        <h2>Feedback</h2>
        <p>
          Found a bug? Take a screenshot in the app and TestFlight will offer to send feedback, or{" "}
          <Link href="/report-a-problem">report a problem</Link> or email{" "}
          <a href={`mailto:${SUPPORT_EMAIL}`}>{SUPPORT_EMAIL}</a>.
        </p>
      </article>
    </main>
  );
}
