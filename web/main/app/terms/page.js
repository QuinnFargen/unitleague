import Link from "next/link";
import DraftNotice from "@/components/DraftNotice";
import { EFFECTIVE_DATE, SUPPORT_EMAIL } from "@/lib/legal";

export const metadata = { title: "Terms of Service" };

export default function Page() {
  return (
    <main>
      <article className="learn-content">
        <h1>Terms of Service</h1>
        <p className="lede">Effective {EFFECTIVE_DATE}</p>

        <DraftNotice />

        <p>
          These terms are an agreement between you and Unit League ("we", "us"). By using the Unit
          League app or website, you agree to them. If you don't agree, please don't use Unit
          League.
        </p>
        {/* TODO(legal): name the legal entity behind Unit League (an individual or an LLC). */}

        <h2>1. What Unit League is</h2>
        <p>
          Unit League is a simulated sports-picks game. You make picks on real games using fake{" "}
          <strong>units</strong> and compete with friends in syndicates. No real money is wagered,
          and nothing you win can be turned into money.
        </p>

        <h2>2. Units have no cash value</h2>
        <ul>
          <li>Units are points in a game. They are not money and have no cash value.</li>
          <li>Units can't be bought, sold, traded, transferred, or cashed out, in or outside the app.</li>
          <li>Prizes in Unit League, such as a free syndicate entry, are more play, never money.</li>
          <li>We can adjust, reset, or remove units, for example to fix a bug or a grading error.</li>
        </ul>

        <h2>3. Who can use Unit League</h2>
        <p>
          You must be at least 18 years old to use Unit League. By using it, you confirm that you
          are. You are responsible for your account and for everything done with it.
        </p>
        {/* TODO(legal): confirm 18+ matches the App Store age rating chosen in App Store Connect. */}

        <h2>4. Community rules</h2>
        <p>
          You can create content other people see, such as your profile name, symbol, and syndicate
          names and descriptions.{" "}
          <strong>We have zero tolerance for objectionable content or abusive users.</strong> Don't
          post or use content that:
        </p>
        <ul>
          <li>Harasses, threatens, bullies, or demeans anyone.</li>
          <li>Is hateful or discriminates based on race, ethnicity, religion, gender, sexual orientation, disability, or similar traits.</li>
          <li>Is sexually explicit, violent, or graphic.</li>
          <li>Impersonates another person, team, league, or Unit League.</li>
          <li>Shares someone else's personal information.</li>
          <li>Is spam, advertising, or promotes real-money gambling services.</li>
          <li>Is illegal or infringes someone else's rights.</li>
        </ul>
        <p>
          You also agree not to cheat, use bots or scripts, exploit bugs, create multiple accounts
          to gain an advantage, or interfere with the service.
        </p>

        <h3>Reporting and enforcement</h3>
        <p>
          If you see content or a user that breaks these rules, report it through the app or email{" "}
          <a href={`mailto:${SUPPORT_EMAIL}`}>{SUPPORT_EMAIL}</a>. We review reports and act on
          them within 24 hours. We may remove content, rename profiles or syndicates, remove users
          from syndicates, and suspend or permanently ban accounts that break these rules, with or
          without warning.
        </p>
        {/* TODO(legal): Apple guideline 1.2 needs an in-app way to report content and block users. Confirm it ships, and that the 24-hour review commitment is realistic. */}

        <h2>5. Your content</h2>
        <p>
          You own the content you create. You give us a non-exclusive, worldwide, royalty-free
          license to store, display, and use it to run Unit League. This license ends when you
          delete the content or your account, except where other people have already seen it in a
          syndicate history.
        </p>

        <h2>6. Odds, scores, and data</h2>
        <p>
          Odds, lines, scores, and other sports data in Unit League come from third-party sources.
          They are provided "as is", may be delayed, incomplete, or wrong, and may differ from what
          a sportsbook shows. We may void or regrade picks affected by bad data.
        </p>

        <h2>7. Not betting advice</h2>
        <p>
          Nothing in Unit League is betting, financial, or professional advice. Results in a game
          with fake units don't predict how you would do with real money. If you choose to gamble
          elsewhere, that is your decision and your responsibility. See{" "}
          <Link href="/responsible-play">Responsible Play</Link>.
        </p>

        <h2>8. No affiliation</h2>
        <p>
          Unit League is not affiliated with, endorsed by, or sponsored by any sports league, team,
          player, or sportsbook. Team and league names are used only to identify the real games.
        </p>

        <h2>9. Purchases</h2>
        <p>
          Unit League is free to play. We may later offer optional paid extras, such as a season
          pass or extra syndicates. These would be sold through Apple's in-app purchase system and
          are covered by Apple's terms, including its refund policy; we don't process payments
          ourselves. Paid extras never become units, winnings, or anything with cash value.
        </p>

        <h2>10. Ending your account</h2>
        <p>
          You can stop using Unit League and{" "}
          <Link href="/delete-account">delete your account</Link> at any time. We can suspend or
          end your access if you break these terms or if we stop offering Unit League.
        </p>

        <h2>11. Disclaimers</h2>
        <p>
          Unit League is provided "as is" and "as available", without warranties of any kind,
          including that it will be uninterrupted, error-free, or accurate. We may change or
          discontinue any part of it at any time.
        </p>

        <h2>12. Limitation of liability</h2>
        <p>
          To the fullest extent the law allows, Unit League and the people behind it are not liable
          for any indirect, incidental, special, consequential, or punitive damages, or for any
          loss of data, profits, or money, including money lost gambling anywhere else. Our total
          liability for any claim about Unit League is limited to the greater of the amount you
          paid us in the 12 months before the claim or $50.
        </p>
        <p>Some places don't allow these limits, so they may not all apply to you.</p>

        <h2>13. Governing law</h2>
        {/* TODO(legal): fill in your state and county. */}
        <p>
          These terms are governed by the laws of the State of [YOUR STATE], without regard to its
          conflict-of-laws rules. Any dispute will be handled in the state or federal courts
          located in [YOUR COUNTY], [YOUR STATE].
        </p>

        <h2>14. Changes to these terms</h2>
        <p>
          We may update these terms. When we do, we'll change the effective date above, and for
          significant changes we'll tell you in the app before they take effect. If you keep using
          Unit League after that, you accept the new terms.
        </p>

        <h2>15. Contact</h2>
        <p>
          Questions about these terms? Email{" "}
          <a href={`mailto:${SUPPORT_EMAIL}`}>{SUPPORT_EMAIL}</a>. See also our{" "}
          <Link href="/privacy">Privacy Policy</Link>.
        </p>
      </article>
    </main>
  );
}
