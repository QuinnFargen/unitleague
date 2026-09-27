import ApiExplorer from "@/components/ApiExplorer";
import { API_URL, getOpenApiSpec } from "@/lib/api";

export const metadata = { title: "API · UNIT League Admin" };
export const dynamic = "force-dynamic";

export default async function ApiPage() {
  let spec;
  try {
    spec = await getOpenApiSpec();
  } catch (err) {
    return (
      <main>
        <h1>API</h1>
        <p className="error">Could not load the OpenAPI spec from {API_URL}: {err.message}</p>
      </main>
    );
  }

  return <ApiExplorer spec={spec} apiUrl={API_URL} />;
}
