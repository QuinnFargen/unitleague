import LearnNav from "@/components/learn/LearnNav";

export default function LearnLayout({ children }) {
  return (
    <main className="learn-layout">
      <LearnNav />
      <article className="learn-content">{children}</article>
    </main>
  );
}
