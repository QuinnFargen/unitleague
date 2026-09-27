// Syntax-highlights an already pretty-printed JSON string.
const TOKEN = /("(?:\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*")(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;

// Highlighting spans get slow on very large payloads; fall back to plain text.
const MAX_HIGHLIGHT_CHARS = 500_000;

function highlight(json) {
  const parts = [];
  let last = 0;
  for (const m of json.matchAll(TOKEN)) {
    if (m.index > last) parts.push(json.slice(last, m.index));
    let cls;
    if (m[1]) cls = m[2] ? "json-key" : "json-string";
    else if (m[3]) cls = m[3] === "null" ? "json-null" : "json-bool";
    else cls = "json-number";
    parts.push(<span key={m.index} className={cls}>{m[1] ?? m[0]}</span>);
    if (m[2]) parts.push(m[2]);
    last = m.index + m[0].length;
  }
  if (last < json.length) parts.push(json.slice(last));
  return parts;
}

export default function JsonView({ json }) {
  return (
    <pre className="json-view">
      {json.length > MAX_HIGHLIGHT_CHARS ? json : highlight(json)}
    </pre>
  );
}
