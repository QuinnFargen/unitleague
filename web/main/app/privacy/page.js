import Link from "next/link";
import DraftNotice from "@/components/DraftNotice";
import { EFFECTIVE_DATE, SUPPORT_EMAIL } from "@/lib/legal";

export const metadata = { title: "Privacy Policy" };

export default function Page() {
  return (
    <main>
      <article className="learn-content">
        <h1>Privacy Policy</h1>
        <p className="lede">Effective {EFFECTIVE_DATE}</p>

        <DraftNotice />

        <p>
          Unit League is a free sports-picks game played with fake units. This policy explains what
          we collect when you use the Unit League app and website, why, and what you can do about
          it. We keep it short because we collect very little.
        </p>

        <h2>What we collect</h2>
        <h3>When you sign in with Apple</h3>
        <ul>
          <li>
            <strong>Apple user ID.</strong> A unique identifier Apple gives us for your account. It
            is how we recognize you when you sign in again.
          </li>
          <li>
            <strong>Email address.</strong> Whatever Apple shares with us. If you chose "Hide My
            Email", this is a private relay address that forwards to you, and we never see your
            real address. You can change it in the app.
          </li>
          <li>
            <strong>Name.</strong> The name Apple shares the first time you sign in.
          </li>
        </ul>

        <h3>What you create in the app</h3>
        <ul>
          <li>Your profile name, symbol, color, and favorite team.</li>
          <li>
            The picks you make, the units you win or lose, and your history of transactions.
          </li>
          <li>
            The syndicates you create or join, including the name, symbol, and color you use in
            each one, your role, and the syndicate names and descriptions you write.
          </li>
          <li>When you created your account and when you last signed in.</li>
        </ul>
        <p>
          Your profile name, symbol, color, picks, and standings are visible to other members of
          your syndicates. Public syndicate names and descriptions are visible to anyone using the
          app.
        </p>

        <h3>Server logs</h3>
        <p>
          Our servers keep standard technical logs, such as IP address, request time, the page or
          endpoint requested, and error details. We use them to keep the service running and secure.
        </p>
        {/* TODO(legal): confirm log retention period with the API and website hosts. */}

        <h3>What we don't collect</h3>
        <p>
          We don't collect payment details, precise location, contacts, photos, or advertising
          identifiers, and we don't use third-party advertising or tracking tools.
        </p>
        {/* TODO(legal): re-check this list if analytics, crash reporting, or push notifications are added. */}

        <h2>How we use it</h2>
        <ul>
          <li>To sign you in and run your account.</li>
          <li>To show your picks, units, and standings to you and your syndicates.</li>
          <li>To answer support requests and enforce our <Link href="/terms">Terms</Link>.</li>
          <li>To find and fix bugs and protect the service from abuse.</li>
        </ul>

        <h2>Who we share it with</h2>
        <p>
          <strong>We don't sell your personal information, and we don't share it for advertising.</strong>{" "}
          We use a small number of service providers who process data on our behalf, only to run
          Unit League:
        </p>
        <ul>
          <li><strong>Neon</strong>, which hosts our database.</li>
          {/* TODO(legal): name the API host (e.g. the provider running the FastAPI service). */}
          <li><strong>Our API hosting provider</strong>, which runs the server the app talks to.</li>
          {/* TODO(legal): name the website host. */}
          <li><strong>Our website hosting provider</strong>, which serves unitleague.com.</li>
          <li><strong>Apple</strong>, which handles Sign in with Apple and, later, in-app purchases.</li>
        </ul>
        <p>
          We may also disclose information if the law requires it, to protect people's safety, or
          as part of a merger or sale of Unit League, in which case this policy would still apply.
        </p>

        <h2>How long we keep it</h2>
        <p>
          We keep your account data while your account exists. When you delete your account, we
          permanently delete your profile, picks, transactions, and syndicate memberships from our
          database. Server logs and database backups are deleted on their normal schedule.
        </p>
        {/* TODO(legal): state the actual backup and log retention windows (e.g. "within 30 days"). */}

        <h2>Deleting your account</h2>
        <p>
          You can delete your account at any time from inside the app or by emailing us. See{" "}
          <Link href="/delete-account">how to delete your account</Link>.
        </p>

        <h2>Your rights</h2>
        <p>
          Depending on where you live (for example, California under the CCPA, or the EU and UK
          under the GDPR), you may have the right to:
        </p>
        <ul>
          <li>Know what personal information we hold about you and get a copy of it.</li>
          <li>Correct information that is wrong.</li>
          <li>Delete your information.</li>
          <li>Object to or restrict how we use it.</li>
          <li>Opt out of the sale or sharing of your information. We don't sell or share it.</li>
          <li>Not be treated differently for using any of these rights.</li>
        </ul>
        <p>
          To use any of these rights, email <a href={`mailto:${SUPPORT_EMAIL}`}>{SUPPORT_EMAIL}</a>.
          We may need to confirm the request comes from you. If you are in the EU or UK, you can
          also complain to your local data protection authority. Where the GDPR applies, we rely on
          our contract with you (running your account) and our legitimate interest (keeping the
          service secure) to use your data.
        </p>

        <h2>Age requirement</h2>
        <p>
          Unit League is for people 18 and older. It is not directed at children, and we don't
          knowingly collect information from anyone under 18. If you believe a minor has an
          account, contact us and we will delete it.
        </p>

        <h2>Security</h2>
        <p>
          We use encrypted connections and limit access to our systems. No service is perfectly
          secure, so we can't promise that your information will never be accessed without
          permission.
        </p>

        <h2>Where your data is stored</h2>
        {/* TODO(legal): confirm the Neon and API host regions. */}
        <p>Our providers store and process data in the United States.</p>

        <h2>Changes</h2>
        <p>
          If we change this policy, we'll update the effective date above. If a change is
          significant, we'll tell you in the app before it takes effect.
        </p>

        <h2>Contact</h2>
        <p>
          Questions about privacy? Email{" "}
          <a href={`mailto:${SUPPORT_EMAIL}`}>{SUPPORT_EMAIL}</a> or visit the{" "}
          <Link href="/contact">contact page</Link>.
        </p>
      </article>
    </main>
  );
}
