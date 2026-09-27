import Link from "next/link";

// Inline link to a glossary entry on /learn/terms.
export default function Term({ id, children }) {
  return (
    <Link href={`/learn/terms#${id}`} className="term-link">
      {children}
    </Link>
  );
}
