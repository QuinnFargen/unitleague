"use client";

import { useEffect, useMemo, useState } from "react";
import JsonView from "@/components/JsonView";

const METHODS = ["get", "post", "put", "patch", "delete"];

function resolveRef(schema, spec) {
  if (!schema?.$ref) return schema;
  const name = schema.$ref.split("/").pop();
  return spec.components?.schemas?.[name] ?? {};
}

// Pydantic optionals come through as anyOf: [{type: X}, {type: "null"}].
function unwrapNullable(schema, spec) {
  const s = resolveRef(schema, spec) ?? {};
  if (s.anyOf) {
    const nonNull = s.anyOf.find((o) => o.type !== "null");
    return { ...resolveRef(nonNull, spec), title: s.title, default: s.default };
  }
  return s;
}

function exampleFromSchema(schema, spec, depth = 0) {
  const s = unwrapNullable(schema, spec);
  if (s.default !== undefined) return s.default;
  if (depth > 4) return null;
  switch (s.type) {
    case "object": {
      if (!s.properties) return {};
      return Object.fromEntries(
        Object.entries(s.properties).map(([k, v]) => [k, exampleFromSchema(v, spec, depth + 1)])
      );
    }
    case "array": return [exampleFromSchema(s.items, spec, depth + 1)];
    case "integer":
    case "number": return 0;
    case "boolean": return false;
    case "string": return "";
    default: return null;
  }
}

function buildEndpoints(spec) {
  const endpoints = [];
  for (const [path, item] of Object.entries(spec.paths ?? {})) {
    for (const method of METHODS) {
      const op = item[method];
      if (!op) continue;
      const bodySchema = op.requestBody?.content?.["application/json"]?.schema;
      const resolvedBody = bodySchema ? resolveRef(bodySchema, spec) : null;
      endpoints.push({
        id: op.operationId ?? `${method}_${path}`,
        method: method.toUpperCase(),
        path,
        tag: op.tags?.[0] ?? "other",
        summary: op.summary ?? "",
        description: op.description ?? "",
        params: (op.parameters ?? []).map((p) => ({
          ...p,
          schema: unwrapNullable(p.schema, spec),
        })),
        body: resolvedBody && {
          schema: resolvedBody,
          required: resolvedBody.required ?? [],
          example: JSON.stringify(exampleFromSchema(resolvedBody, spec), null, 2),
        },
      });
    }
  }
  return endpoints;
}

function groupByTag(endpoints) {
  const groups = new Map();
  for (const e of endpoints) {
    if (!groups.has(e.tag)) groups.set(e.tag, []);
    groups.get(e.tag).push(e);
  }
  return [...groups.entries()];
}

function initialInputs(endpoint) {
  return { path: {}, query: {}, body: endpoint.body?.example ?? "" };
}

function formatBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

export default function ApiExplorer({ spec, apiUrl }) {
  const endpoints = useMemo(() => buildEndpoints(spec), [spec]);
  const [selectedId, setSelectedId] = useState(endpoints[0]?.id);
  const [filter, setFilter] = useState("");
  const [inputs, setInputs] = useState({});
  const [results, setResults] = useState({});

  // Keep the selected endpoint in the URL hash so it survives reloads and can be shared.
  useEffect(() => {
    const syncFromHash = () => {
      const fromHash = decodeURIComponent(window.location.hash.slice(1));
      if (fromHash && endpoints.some((e) => e.id === fromHash)) setSelectedId(fromHash);
    };
    syncFromHash();
    window.addEventListener("hashchange", syncFromHash);
    return () => window.removeEventListener("hashchange", syncFromHash);
  }, [endpoints]);

  const select = (id) => {
    setSelectedId(id);
    history.replaceState(null, "", `#${encodeURIComponent(id)}`);
  };

  const q = filter.trim().toLowerCase();
  const visible = q
    ? endpoints.filter((e) => `${e.method} ${e.path} ${e.summary} ${e.tag}`.toLowerCase().includes(q))
    : endpoints;

  const endpoint = endpoints.find((e) => e.id === selectedId);

  return (
    <main className="explorer">
      <aside className="explorer-sidebar">
        <input
          type="search"
          className="input"
          placeholder="Filter endpoints…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          aria-label="Filter endpoints"
        />
        <nav aria-label="API endpoints">
          {groupByTag(visible).map(([tag, list]) => (
            <section key={tag}>
              <h3>{tag}</h3>
              <ul>
                {list.map((e) => (
                  <li key={e.id}>
                    <button
                      type="button"
                      className={`endpoint-link${e.id === selectedId ? " active" : ""}`}
                      onClick={() => select(e.id)}
                      title={e.summary}
                    >
                      <span className={`method method-${e.method.toLowerCase()}`}>{e.method}</span>
                      <span className="endpoint-path">{e.path}</span>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          ))}
          {visible.length === 0 && <p className="muted">No endpoints match.</p>}
        </nav>
      </aside>

      <section className="explorer-main">
        {endpoint ? (
          <EndpointPanel
            key={endpoint.id}
            endpoint={endpoint}
            apiUrl={apiUrl}
            inputs={inputs[endpoint.id] ?? initialInputs(endpoint)}
            setInputs={(next) => setInputs((all) => ({ ...all, [endpoint.id]: next }))}
            result={results[endpoint.id]}
            setResult={(r) => setResults((all) => ({ ...all, [endpoint.id]: r }))}
          />
        ) : (
          <p className="muted">Select an endpoint.</p>
        )}
      </section>
    </main>
  );
}

function EndpointPanel({ endpoint, apiUrl, inputs, setInputs, result, setResult }) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const pathParams = endpoint.params.filter((p) => p.in === "path");
  const queryParams = endpoint.params.filter((p) => p.in === "query");
  const isWrite = endpoint.method !== "GET";

  const setParam = (where, name, value) =>
    setInputs({ ...inputs, [where]: { ...inputs[where], [name]: value } });

  const buildUrl = () => {
    let path = endpoint.path;
    for (const p of pathParams) {
      const v = inputs.path[p.name] ?? "";
      path = path.replace(`{${p.name}}`, v === "" ? `{${p.name}}` : encodeURIComponent(v));
    }
    const qs = new URLSearchParams();
    for (const p of queryParams) {
      const v = inputs.query[p.name];
      if (v !== undefined && v !== "") qs.set(p.name, v);
    }
    const search = qs.toString();
    return `${path}${search ? `?${search}` : ""}`;
  };

  const url = buildUrl();

  const send = async () => {
    setError(null);
    const missing = endpoint.params.filter(
      (p) => p.required && !(inputs[p.in]?.[p.name] ?? "").toString().trim()
    );
    if (missing.length) {
      setError(`Missing required: ${missing.map((p) => p.name).join(", ")}`);
      return;
    }

    let body;
    if (endpoint.body) {
      try {
        body = JSON.stringify(JSON.parse(inputs.body || "{}"));
      } catch (err) {
        setError(`Body is not valid JSON: ${err.message}`);
        return;
      }
    }

    if (isWrite && !window.confirm(`${endpoint.method} ${url}\n\nThis writes to ${apiUrl}. Continue?`)) return;

    setLoading(true);
    const started = performance.now();
    try {
      const res = await fetch(`/proxy${url}`, {
        method: endpoint.method,
        headers: body ? { "content-type": "application/json" } : undefined,
        body,
      });
      const text = await res.text();
      let data;
      let isJson = true;
      try {
        data = JSON.parse(text);
      } catch {
        data = text;
        isJson = false;
      }
      setResult({
        status: res.status,
        ok: res.ok,
        ms: Math.round(performance.now() - started),
        upstreamMs: res.headers.get("x-upstream-ms"),
        size: new Blob([text]).size,
        data,
        isJson,
        url,
        method: endpoint.method,
        at: new Date().toLocaleTimeString(),
      });
    } catch (err) {
      setError(`Request failed: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const onKeyDown = (e) => {
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      send();
    }
  };

  const curl = [
    `curl -X ${endpoint.method} '${apiUrl}${url}'`,
    endpoint.body && `-H 'content-type: application/json'`,
    endpoint.body && `-d '${(inputs.body || "{}").replace(/'/g, "'\\''")}'`,
  ].filter(Boolean).join(" \\\n  ");

  return (
    <div className="endpoint-panel" onKeyDown={onKeyDown}>
      <header className="endpoint-header">
        <div className="endpoint-title">
          <span className={`method method-${endpoint.method.toLowerCase()}`}>{endpoint.method}</span>
          <code>{endpoint.path}</code>
        </div>
        {endpoint.summary && <p className="muted">{endpoint.summary}</p>}
        {endpoint.description && <p className="muted">{endpoint.description}</p>}
      </header>

      {pathParams.length > 0 && (
        <ParamGroup title="Path parameters" params={pathParams} values={inputs.path}
          onChange={(name, v) => setParam("path", name, v)} />
      )}

      {queryParams.length > 0 && (
        <ParamGroup title="Query parameters" params={queryParams} values={inputs.query}
          onChange={(name, v) => setParam("query", name, v)} />
      )}

      {endpoint.body && (
        <div className="param-group">
          <div className="param-group-head">
            <h3>Request body</h3>
            <button type="button" className="btn btn-small" onClick={() => setInputs({ ...inputs, body: endpoint.body.example })}>
              Reset to example
            </button>
          </div>
          <p className="muted small">
            Required: {endpoint.body.required.length ? endpoint.body.required.join(", ") : "none"}
          </p>
          <textarea
            className="input code-input"
            spellCheck={false}
            rows={Math.min(20, Math.max(6, (inputs.body.match(/\n/g)?.length ?? 0) + 2))}
            value={inputs.body}
            onChange={(e) => setInputs({ ...inputs, body: e.target.value })}
          />
        </div>
      )}

      {pathParams.length + queryParams.length === 0 && !endpoint.body && (
        <p className="muted small">No parameters.</p>
      )}

      <div className="send-row">
        <button type="button" className="btn btn-primary" onClick={send} disabled={loading}>
          {loading ? "Sending…" : "Send"}
        </button>
        <code className="request-url">{endpoint.method} {url}</code>
        <CopyButton text={curl} label="Copy curl" />
      </div>
      <p className="muted small">⌘/Ctrl + Enter to send{isWrite && " · write requests ask for confirmation"}</p>

      {error && <p className="error">{error}</p>}

      {result && <ResponsePanel result={result} />}
    </div>
  );
}

function ParamGroup({ title, params, values, onChange }) {
  return (
    <div className="param-group">
      <h3>{title}</h3>
      <div className="param-grid">
        {params.map((p) => {
          const type = p.schema?.type ?? "string";
          const value = values[p.name] ?? "";
          const id = `param-${p.in}-${p.name}`;
          return (
            <label key={p.name} htmlFor={id} className="param">
              <span className="param-label">
                {p.name}
                {p.required && <span className="required" title="Required">*</span>}
                <span className="param-type">{type}</span>
              </span>
              {type === "boolean" ? (
                <select id={id} className="input" value={value} onChange={(e) => onChange(p.name, e.target.value)}>
                  <option value="">{p.schema?.default !== undefined ? `(default: ${p.schema.default})` : "—"}</option>
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              ) : (
                <input
                  id={id}
                  className="input"
                  type={type === "integer" || type === "number" ? "number" : "text"}
                  value={value}
                  placeholder={p.schema?.default !== undefined ? `default: ${p.schema.default}` : ""}
                  onChange={(e) => onChange(p.name, e.target.value)}
                />
              )}
            </label>
          );
        })}
      </div>
    </div>
  );
}

function ResponsePanel({ result }) {
  const isTable =
    result.isJson &&
    Array.isArray(result.data) &&
    result.data.length > 0 &&
    result.data.every((r) => r && typeof r === "object" && !Array.isArray(r));
  const [view, setView] = useState("json");
  const pretty = result.isJson ? JSON.stringify(result.data, null, 2) : result.data;

  return (
    <div className="response">
      <div className="response-meta">
        <span className={`status ${result.ok ? "status-ok" : "status-err"}`}>{result.status}</span>
        <span>{result.ms} ms{result.upstreamMs && ` (API ${result.upstreamMs} ms)`}</span>
        <span>{formatBytes(result.size)}</span>
        {Array.isArray(result.data) && <span>{result.data.length} rows</span>}
        <span className="muted">at {result.at}</span>
        <div className="response-actions">
          {isTable && (
            <div className="segmented" role="group" aria-label="Response view">
              <button type="button" className={view === "json" ? "active" : undefined} onClick={() => setView("json")}>JSON</button>
              <button type="button" className={view === "table" ? "active" : undefined} onClick={() => setView("table")}>Table</button>
            </div>
          )}
          <CopyButton text={pretty} label="Copy JSON" />
        </div>
      </div>

      {view === "table" && isTable ? (
        <ResponseTable rows={result.data} />
      ) : result.isJson ? (
        <JsonView json={pretty} />
      ) : (
        <pre className="json-view">{pretty || "(empty response)"}</pre>
      )}
    </div>
  );
}

function ResponseTable({ rows }) {
  const columns = [...new Set(rows.flatMap((r) => Object.keys(r)))];
  const cell = (v) => (v === null || v === undefined ? "—" : typeof v === "object" ? JSON.stringify(v) : String(v));
  return (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>{columns.map((c) => <th key={c}>{c}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>{columns.map((c) => <td key={c}>{cell(r[c])}</td>)}</tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CopyButton({ text, label }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {}
  };
  return (
    <button type="button" className="btn btn-small" onClick={copy}>
      {copied ? "Copied" : label}
    </button>
  );
}
