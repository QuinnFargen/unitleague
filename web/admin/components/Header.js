"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV = [
  { href: "/api", label: "API" },
];

export default function Header() {
  const pathname = usePathname();
  const isActive = (href) => pathname === href || pathname.startsWith(`${href}/`);

  return (
    <header className="site-header">
      <div className="site-header-inner">
        <Link href="/" className="site-logo" aria-label="UNIT League admin home">
          <picture>
            <source srcSet="/logo-white.png" media="(prefers-color-scheme: dark)" />
            <img src="/logo-black.png" alt="UNIT League" className="logo" />
          </picture>
        </Link>

        <nav className="site-nav" aria-label="Admin">
          <ul>
            {NAV.map(({ href, label }) => (
              <li key={href}>
                <Link
                  href={href}
                  className={isActive(href) ? "active" : undefined}
                  aria-current={isActive(href) ? "page" : undefined}
                >
                  {label}
                </Link>
              </li>
            ))}
          </ul>
        </nav>

        <span className="admin-badge">Admin</span>
      </div>
    </header>
  );
}
