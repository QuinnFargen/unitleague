import "./globals.css";

export const metadata = {
  title: "UNIT League",
  description: "Fantasy-style betting leagues with fake units. No real money.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
