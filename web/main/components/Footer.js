import Link from "next/link";

const GROUPS = [
  {
    title: "Learn",
    links: [
      { href: "/learn/how-it-works", label: "How It Works" },
      { href: "/learn/scoring", label: "Scoring & Rules" },
      { href: "/learn/faq", label: "FAQ" },
    ],
  },
  {
    title: "Support",
    links: [
      { href: "/contact", label: "Contact" },
      { href: "/report-a-problem", label: "Report a Problem" },
      { href: "/changelog", label: "What's New" },
    ],
  },
  {
    title: "Community",
    // TODO: replace with real profile URLs.
    links: [
      { href: "#", label: "Discord", external: true },
      { href: "#", label: "X / Twitter", external: true },
      { href: "#", label: "Instagram", external: true },
    ],
  },
  {
    title: "Legal",
    links: [
      { href: "/terms", label: "Terms of Service" },
      { href: "/privacy", label: "Privacy Policy" },
      { href: "/responsible-play", label: "Responsible Play" },
    ],
  },
];

export default function Footer() {
  return (
    <footer className="site-footer">
      <div className="site-footer-inner">
        <div className="footer-groups">
          {GROUPS.map(({ title, links }) => (
            <nav key={title} aria-label={title}>
              <h3>{title}</h3>
              <ul>
                {links.map(({ href, label, external }) => (
                  <li key={label}>
                    {external ? (
                      <a href={href} target="_blank" rel="noopener noreferrer">{label}</a>
                    ) : (
                      <Link href={href}>{label}</Link>
                    )}
                  </li>
                ))}
              </ul>
            </nav>
          ))}
        </div>

        <div className="footer-bottom">
          <p>UNIT League is a free-to-play game using fake units. No real money is wagered.</p>
          <p>© {new Date().getFullYear()} UNIT League</p>
        </div>
      </div>
    </footer>
  );
}
