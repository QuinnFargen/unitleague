"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { LEARN_SECTIONS } from "@/lib/learn";

export default function LearnNav() {
  const pathname = usePathname();

  return (
    <nav className="learn-nav" aria-label="Learn">
      <Link href="/learn" className={`learn-nav-title${pathname === "/learn" ? " active" : ""}`}>
        Learn
      </Link>
      <ul>
        {LEARN_SECTIONS.map(({ href, label }) => {
          const active = pathname === href;
          return (
            <li key={href}>
              <Link
                href={href}
                className={active ? "active" : undefined}
                aria-current={active ? "page" : undefined}
              >
                {label}
              </Link>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}
