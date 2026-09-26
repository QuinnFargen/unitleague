export default function Header({ label }) {
  return (
    <header className="header">
      <picture>
        <source srcSet="/logo-white.png" media="(prefers-color-scheme: dark)" />
        <img src="/logo-black.png" alt="UNIT League" className="logo" />
      </picture>
      {label && <span className="badge">{label}</span>}
    </header>
  );
}
