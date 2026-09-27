import Glossary from "@/components/learn/Glossary";

export const metadata = { title: "Terms" };

export default function Page() {
  return (
    <>
      <h1>Terms</h1>
      <p className="lede">
        The language of betting. Terms that appear elsewhere in Learn link back here.
      </p>
      <Glossary />
    </>
  );
}
