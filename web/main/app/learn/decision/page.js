import Decision from "@/components/learn/Decision";

export const metadata = { title: "Decision" };

export default function Page() {
  return (
    <>
      <h1>Decision</h1>
      <p className="lede">
        UNIT League, or a real sportsbook or prediction market? Pick the tradeoffs you'd accept and
        see where you land.
      </p>
      <Decision />
    </>
  );
}
