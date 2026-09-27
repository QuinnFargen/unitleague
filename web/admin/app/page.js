import Link from "next/link";

const SECTIONS = [
  { href: "/api", title: "API", description: "Browse FastAPI endpoints, fill in parameters, and inspect the JSON response." },
];

export default function AdminHome() {
  return (
    <main>
      <h1>Admin</h1>
      <div className="section-cards">
        {SECTIONS.map(({ href, title, description }) => (
          <Link key={href} href={href} className="section-card">
            <h2>{title}</h2>
            <p>{description}</p>
          </Link>
        ))}
      </div>
    </main>
  );
}
