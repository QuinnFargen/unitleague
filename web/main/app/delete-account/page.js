import Link from "next/link";
import { SUPPORT_EMAIL } from "@/lib/legal";

export const metadata = { title: "Delete Your Account" };

export default function Page() {
  return (
    <main>
      <article className="learn-content">
        <h1>Delete Your Account</h1>
        <p className="lede">You can delete your Unit League account at any time.</p>

        <h2>In the app</h2>
        <ol>
          <li>Open Unit League and go to the <strong>Profile</strong> tab.</li>
          <li>Tap your profile to open <strong>Edit Profile</strong>.</li>
          <li>Scroll down and tap <strong>Delete Account</strong>.</li>
          <li>Confirm, then sign in with Apple once more to verify it's you.</li>
        </ol>
        {/* TODO(legal): confirm these steps match the shipped iOS screens. */}

        <h2>By email</h2>
        <p>
          If you can't use the app, email{" "}
          <a href={`mailto:${SUPPORT_EMAIL}?subject=Delete%20my%20account`}>{SUPPORT_EMAIL}</a>{" "}
          with the subject "Delete my account". Send it from the email on your account (or tell us
          your profile name) so we can find it. We may ask you to confirm before deleting, and
          we'll reply once it's done.
        </p>
        {/* TODO(legal): commit to a turnaround time for email deletion requests (e.g. 30 days). */}

        <h2>What gets deleted</h2>
        <ul>
          <li>Your profile: name, email, symbol, color, and favorite team.</li>
          <li>Your units, picks, and transaction history.</li>
          <li>Your membership in every syndicate.</li>
          <li>Your link to Sign in with Apple. We also ask Apple to revoke Unit League's access.</li>
        </ul>
        <p>
          <strong>Deletion is permanent and can't be undone.</strong> If you sign in again later,
          you'll start a brand-new account.
        </p>

        <h2>Syndicates you run</h2>
        <p>
          If you created or run a syndicate that still has other members, it's handed to the
          longest-standing member so it can keep going. If you're the only member left, the
          syndicate is deleted too.
        </p>

        <h2>Backups and logs</h2>
        <p>
          Copies in database backups and server logs are removed on their normal schedule. See
          our <Link href="/privacy">Privacy Policy</Link> for details.
        </p>
      </article>
    </main>
  );
}
