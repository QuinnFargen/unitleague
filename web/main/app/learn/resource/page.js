export const metadata = { title: "Resource" };

const GROUPS = [
  { id: "addiction", label: "Addiction" },
  { id: "dangers", label: "Dangers" },
  { id: "financial", label: "Financial" },
  { id: "math", label: "Math" },
];

function Video({ id, title, note }) {
  return (
    <section className="video">
      {note && <p>{note}</p>}
      <div className="video-frame">
        <iframe
          src={`https://www.youtube-nocookie.com/embed/${id}`}
          title={title}
          loading="lazy"
          allow="accelerometer; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
          referrerPolicy="strict-origin-when-cross-origin"
          allowFullScreen
        />
      </div>
    </section>
  );
}

function Ext({ href, children }) {
  return <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>;
}

export default function Page() {
  return (
    <>
      <h1>Resource</h1>
      <p className="lede">Worth your time before you put real money on a game.</p>

      <nav className="section-filter" aria-label="Jump to group">
        {GROUPS.map(({ id, label }) => (
          <a key={id} href={`#${id}`}>{label}</a>
        ))}
      </nav>

      <h2 id="addiction" className="resource-group">Addiction</h2>
      <div className="callout">
        <p>
          <strong>National Problem Gambling Helpline</strong>: 24/7/365 confidential support and
          referrals. Call or text <a href="tel:18006973738">1-800-MY-RESET</a>.
        </p>
        <p>
          <strong>Gamblers Anonymous (GA)</strong>: recovery meetings. Call{" "}
          <a href="tel:18552225542">1-855-222-5542</a>.
        </p>
      </div>
      <p>
        If gambling is affecting you or someone close to you, help is free and confidential. The <Ext href="https://www.ncpgambling.org/help-treatment/">National Council on Problem Gambling</Ext>{" "}
        lists treatment options, support groups, and state resources.
      </p>

      <h2 id="dangers" className="resource-group">Dangers</h2>
      <p>
        <Ext href="https://www.youtube.com/@breakingpoints">Breaking Points</Ext> is an independent
        news show that isn't paid by the gambling industry. That's rare: nearly every sports podcast
        and many national news outlets now run sportsbook or prediction market ads, which makes it
        hard for them to cover the harm honestly.
      </p>
      <Video
        id="jSxZUw923gs"
        title="Former FanDuel CEO Admits Ads Are A LIE"
        note="A former industry insider on what sportsbook advertising leaves out."
      />
      <Video
        id="qJw7lIO9KeE"
        title="Why Online Gambling Is The Next Opioid Crisis"
        note="How mobile betting went mainstream, and the addiction it's driving."
      />

      <h2 id="financial" className="resource-group">Financial</h2>
      <p>
        <Ext href="https://www.youtube.com/@MoneyGuyShow">The Money Guy Show</Ext> covers personal
        finance. <strong>Is your Roth IRA funded?</strong> If not, that money has a better job than
        a bet slip.
      </p>
      <ul>
        <li>
          <Ext href="https://moneyguy.com/resource/financial-order-of-operations/">Financial Order of Operations</Ext>:
          their step-by-step plan for what to do with each dollar.
        </li>
        <li><Ext href="https://moneyguy.com/resources/">Money Guy resources</Ext>: free guides, checklists, and tools.</li>
      </ul>
      <p>
        Watch the <strong>first 20 minutes</strong> of this video to learn where sports betting
        should fall in your personal finance priorities.
      </p>
      <Video
        id="7LhSFW09tzs"
        title="How To Actually Make Money Sports Betting (Here's the Math)"
      />

      <h2 id="math" className="resource-group">Math</h2>
      <ul>
        <li>
          <Ext href="https://gambling-math.com">gambling-math.com</Ext>: calculators and plain-language
          explanations of odds, house edge, and why betting systems fail.
        </li>
        <li>
          <Ext href="https://sri.siena.edu/2026/04/13/more-than-a-quarter-of-americans-27-have-an-active-online-sports-betting-account-a-third-have-opened-an-account-at-least-once/">
            Siena Research Institute poll (April 2026)
          </Ext>
          : statistics on who bets and what it costs them. More than a quarter of Americans (27%)
          have an active online sports betting account, and a third have opened one at least once.
        </li>
      </ul>
    </>
  );
}
