"use client";

import { useState } from "react";
import { TERMS, TERM_SECTIONS } from "@/lib/terms";

const SORTED = [...TERMS].sort((a, b) => a.term.localeCompare(b.term));

export default function Glossary() {
  const [section, setSection] = useState(null);
  const shown = section ? SORTED.filter((t) => t.section === section) : SORTED;

  // First term per letter within the current filter, for the jump links.
  const firstByLetter = new Map();
  for (const t of shown) {
    const letter = t.term[0].toUpperCase();
    if (!firstByLetter.has(letter)) firstByLetter.set(letter, t.id);
  }

  return (
    <>
      <nav className="letter-index" aria-label="Jump to letter">
        {[...firstByLetter].map(([letter, id]) => (
          <a key={letter} href={`#${id}`}>{letter}</a>
        ))}
      </nav>

      <div className="section-filter" role="group" aria-label="Filter by section">
        <button type="button" aria-pressed={section === null} onClick={() => setSection(null)}>
          All
        </button>
        {TERM_SECTIONS.map((s) => (
          <button
            key={s}
            type="button"
            aria-pressed={section === s}
            onClick={() => setSection((cur) => (cur === s ? null : s))}
          >
            {s}
          </button>
        ))}
      </div>

      <p className="term-count">
        {shown.length} terms{section ? ` in ${section}` : ""}
      </p>

      <dl className="term-list">
        {shown.map(({ id, term, def, section: s }) => (
          <div key={id} id={id}>
            <dt>
              {term}
              <span className="badge">{s}</span>
            </dt>
            <dd>{def}</dd>
          </div>
        ))}
      </dl>
    </>
  );
}
