"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { LEARN_SECTIONS } from "@/lib/learn";

const NAV = [
  { href: "/bets", label: "Bet" },
  { href: "/syndicate", label: "Syndicate" },
  { href: "/research", label: "Research" },
  { href: "/juice", label: "Juice" },
  { href: "/profile", label: "Profile" },
  { href: "/learn", label: "Learn", children: LEARN_SECTIONS },
];

export default function Header() {
  const pathname = usePathname();
  const [open, setOpen] = useState(false);
  const close = () => setOpen(false);

  const isActive = (href) => pathname === href || pathname.startsWith(`${href}/`);

  return (
    <header className="site-header">
      <div className="site-header-inner">
        <Link href="/" className="site-logo" aria-label="UNIT League home" onClick={close}>
          <picture>
            <source srcSet="/logo-white.png" media="(prefers-color-scheme: dark)" />
            <img src="/logo-black.png" alt="UNIT League" className="logo" />
          </picture>
        </Link>

        <button
          type="button"
          className="nav-toggle"
          aria-expanded={open}
          aria-controls="site-nav"
          aria-label={open ? "Close menu" : "Open menu"}
          onClick={() => setOpen((o) => !o)}
        >
          <span aria-hidden="true">{open ? "✕" : "☰"}</span>
        </button>

        <div id="site-nav" className={`site-nav${open ? " open" : ""}`}>
          <nav aria-label="Main">
            <ul>
              {NAV.map(({ href, label, children }) => (
                <li key={href} className={children ? "has-menu" : undefined}>
                  <Link
                    href={href}
                    className={isActive(href) ? "active" : undefined}
                    aria-current={pathname === href ? "page" : undefined}
                    onClick={close}
                  >
                    {label}
                  </Link>
                  {children && (
                    <ul className="nav-menu" aria-label={`${label} sections`}>
                      {children.map((child) => (
                        <li key={child.href}>
                          <Link
                            href={child.href}
                            className={pathname === child.href ? "active" : undefined}
                            aria-current={pathname === child.href ? "page" : undefined}
                            onClick={(e) => {
                              close();
                              // Drop focus so :focus-within doesn't hold the menu open after navigating.
                              e.currentTarget.blur();
                            }}
                          >
                            {child.label}
                          </Link>
                        </li>
                      ))}
                    </ul>
                  )}
                </li>
              ))}
            </ul>
          </nav>

          <div className="auth-links">
            <Link href="/sign-in" className="btn btn-ghost" onClick={close}>Sign in</Link>
            <Link href="/sign-up" className="btn btn-primary" onClick={close}>Sign up</Link>
          </div>
        </div>
      </div>
    </header>
  );
}
