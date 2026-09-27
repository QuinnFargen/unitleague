export default function Header() {
  return (
    <header>
      <picture>
        <source srcSet="/logo-white.png" media="(prefers-color-scheme: dark)" />
        <img src="/logo-black.png" alt="UNIT League" className="logo" />
      </picture>
    </header>
  );
}
