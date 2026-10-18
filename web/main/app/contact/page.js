import Link from "next/link";
import { SUPPORT_EMAIL } from "@/lib/legal";

export const metadata = { title: "Contact" };

export default function Page() {
  return (
    <main>
      <article className="learn-content">
        <h1>Contact</h1>
        <p className="lede">Questions, bugs, or feedback? We'd like to hear from you.</p>

        <h2>Email support</h2>
        <p>
          Email <a href={`mailto:${SUPPORT_EMAIL}`}>{SUPPORT_EMAIL}</a>. We usually reply within
          two business days.
        </p>
        {/* TODO(legal): confirm the support address and response time. */}
        <p>To help us help you, include:</p>
        <ul>
          <li>The email or profile name on your account.</li>
          <li>The syndicate involved, if any.</li>
          <li>What happened, and a screenshot if you have one.</li>
          <li>Your device and iOS version for bugs.</li>
        </ul>

        <h2>Report abuse</h2>
        <p>
          To report an offensive profile, syndicate name, or user, email us with "Report" in the
          subject line and the name of the profile or syndicate. We review reports within 24 hours.
          See the community rules in our <Link href="/terms">Terms of Service</Link>.
        </p>

        <h2>Account and privacy</h2>
        <ul>
          <li><Link href="/delete-account">Delete your account</Link></li>
          <li><Link href="/privacy">Privacy Policy</Link></li>
          <li><Link href="/terms">Terms of Service</Link></li>
        </ul>

        <h2>Need help with gambling?</h2>
        <p>
          Unit League doesn't use real money, but if gambling anywhere is causing you problems, call
          or text <a href="tel:18006973738">1-800-MY-RESET (1-800-697-3738)</a>. See{" "}
          <Link href="/responsible-play">Responsible Play</Link> for more resources.
        </p>
      </article>
    </main>
  );
}
