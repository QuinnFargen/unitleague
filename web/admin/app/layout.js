export const metadata = { title: "UNIT League Admin" };

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: "system-ui, sans-serif", maxWidth: 800, margin: "0 auto", padding: 16 }}>
        {children}
      </body>
    </html>
  );
}
