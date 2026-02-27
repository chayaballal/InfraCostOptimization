import { useState, useRef, useEffect, useCallback } from "react";

const API = "http://localhost:8000";

// ── Markdown renderer (lightweight, no deps) ──────────────────────
function renderMarkdown(md) {
  if (!md) return "";
  return md
    .replace(/^#### (.+)$/gm, '<h4 class="md-h4">$1</h4>')
    .replace(/^### (.+)$/gm, '<h3 class="md-h3">$1</h3>')
    .replace(/^## (.+)$/gm, '<h2 class="md-h2">$1</h2>')
    .replace(/^# (.+)$/gm, '<h1 class="md-h1">$1</h1>')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g, '<code class="md-code">$1</code>')
    .replace(/```[\w]*\n([\s\S]*?)```/g, '<pre class="md-pre"><code>$1</code></pre>')
    .replace(/^\| (.+) \|$/gm, (_, row) => {
      const cells = row.split(" | ").map(c =>
        `<td class="md-td">${c.trim()}</td>`).join("");
      return `<tr>${cells}</tr>`;
    })
    .replace(/(<tr>[\s\S]*?<\/tr>)/g, (block) => {
      const rows = block.match(/<tr>[\s\S]*?<\/tr>/g) || [];
      if (!rows.length) return block;
      const [header, ...body] = rows;
      const headerRow = header.replace(/<td/g, '<th class="md-th"').replace(/<\/td>/g, '</th>');
      return `<table class="md-table"><thead>${headerRow}</thead><tbody>${body.join("")}</tbody></table>`;
    })
    .replace(/^---$/gm, '<hr class="md-hr"/>')
    .replace(/^\- (.+)$/gm, '<li class="md-li">$1</li>')
    .replace(/(<li[\s\S]*?<\/li>)/g, '<ul class="md-ul">$1</ul>')
    .replace(/\n\n/g, '</p><p class="md-p">')
    .replace(/^(?!<[hupltc])(.+)$/gm, '$1');
}

// ── Severity badge ────────────────────────────────────────────────
function Badge({ text }) {
  const map = {
    CRITICAL: "badge-critical", HIGH: "badge-high",
    MEDIUM: "badge-medium", LOW: "badge-low",
  };
  const cls = map[text?.toUpperCase()] || "badge-low";
  return <span className={`badge ${cls}`}>{text}</span>;
}

// ── Metric pill ───────────────────────────────────────────────────
function Pill({ label, value, warn }) {
  return (
    <div className={`pill ${warn ? "pill-warn" : ""}`}>
      <span className="pill-label">{label}</span>
      <span className="pill-value">{value ?? "—"}</span>
    </div>
  );
}

// ── Instance card (sidebar) ───────────────────────────────────────
function InstanceCard({ inst, selected, onClick }) {
  return (
    <button
      className={`inst-card ${selected ? "inst-card-selected" : ""}`}
      onClick={onClick}
    >
      <div className="inst-id">{inst.instance_id}</div>
      <div className="inst-meta">{inst.instance_name || "unnamed"}</div>
      <div className="inst-type">{inst.instance_type} · {inst.az}</div>
    </button>
  );
}

// ── Main App ──────────────────────────────────────────────────────
export default function App() {
  const [instances, setInstances]     = useState([]);
  const [selected, setSelected]       = useState([]);
  const [window, setWindow]           = useState(30);
  const [focus, setFocus]             = useState(["rightsizing", "risk_warnings", "full_report"]);
  const [question, setQuestion]       = useState("");
  const [output, setOutput]           = useState("");
  const [streaming, setStreaming]     = useState(false);
  const [status, setStatus]           = useState("idle"); // idle | loading | streaming | done | error
  const [error, setError]             = useState(null);
  const [loadingInst, setLoadingInst] = useState(true);
  const outputRef = useRef(null);
  const abortRef  = useRef(null);

  // fetch instances on mount
  useEffect(() => {
    fetch(`${API}/instances`)
      .then(r => r.json())
      .then(d => setInstances(d.instances || []))
      .catch(() => setError("Cannot reach backend. Is uvicorn running?"))
      .finally(() => setLoadingInst(false));
  }, []);

  // auto-scroll output
  useEffect(() => {
    if (outputRef.current) {
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
    }
  }, [output]);

  const toggleSelect = (id) =>
    setSelected(prev => prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]);

  const toggleFocus = (f) =>
    setFocus(prev => prev.includes(f) ? prev.filter(x => x !== f) : [...prev, f]);

  const handleAnalyse = useCallback(async () => {
    setOutput("");
    setError(null);
    setStatus("loading");
    setStreaming(true);

    abortRef.current = new AbortController();

    try {
      const res = await fetch(`${API}/analyse`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        signal: abortRef.current.signal,
        body: JSON.stringify({
          window_days:  window,
          instance_ids: selected,
          question:     question || null,
          focus,
        }),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "Backend error");
      }

      setStatus("streaming");
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const lines = buf.split("\n");
        buf = lines.pop();
        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const data = line.slice(6).trim();
          if (data === "[DONE]") { setStatus("done"); continue; }
          try {
            const { token } = JSON.parse(data);
            setOutput(prev => prev + token);
          } catch {}
        }
      }
      setStatus("done");
    } catch (e) {
      if (e.name !== "AbortError") {
        setError(e.message);
        setStatus("error");
      } else {
        setStatus("idle");
      }
    } finally {
      setStreaming(false);
    }
  }, [window, selected, question, focus]);

  const handleStop = () => {
    abortRef.current?.abort();
    setStreaming(false);
    setStatus("idle");
  };

  const focusOptions = [
    { id: "rightsizing",   label: "Rightsizing",      icon: "⚖" },
    { id: "risk_warnings", label: "Risk Warnings",    icon: "⚠" },
    { id: "full_report",   label: "Full Report",      icon: "📋" },
  ];

  const windowOptions = [10, 30, 60, 90];

  return (
    <>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

        :root {
          --bg:       #0a0c0f;
          --surface:  #111318;
          --border:   #1e2229;
          --border2:  #2a3040;
          --text:     #c8d0dc;
          --muted:    #4a5568;
          --accent:   #00d4ff;
          --accent2:  #0099bb;
          --green:    #00ff88;
          --amber:    #ffb800;
          --red:      #ff4444;
          --purple:   #a855f7;
          --mono:     'IBM Plex Mono', monospace;
          --sans:     'IBM Plex Sans', sans-serif;
        }

        html, body, #root { height: 100%; background: var(--bg); color: var(--text); font-family: var(--sans); }

        .app { display: grid; grid-template-columns: 280px 1fr; grid-template-rows: 56px 1fr; height: 100vh; }

        /* ── Header ── */
        .header {
          grid-column: 1/-1;
          display: flex; align-items: center; gap: 16px;
          padding: 0 24px;
          background: var(--surface);
          border-bottom: 1px solid var(--border);
        }
        .header-logo {
          font-family: var(--mono); font-size: 13px; font-weight: 600;
          color: var(--accent); letter-spacing: 0.08em;
        }
        .header-sep { flex: 1; }
        .status-dot {
          width: 8px; height: 8px; border-radius: 50%;
          background: var(--muted);
          transition: background 0.3s;
        }
        .status-dot.active { background: var(--green); box-shadow: 0 0 8px var(--green); animation: pulse 1s infinite; }
        .status-dot.error  { background: var(--red); }
        .status-dot.done   { background: var(--accent); }
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }
        .status-label { font-family: var(--mono); font-size: 11px; color: var(--muted); }

        /* ── Sidebar ── */
        .sidebar {
          background: var(--surface);
          border-right: 1px solid var(--border);
          display: flex; flex-direction: column;
          overflow: hidden;
        }
        .sidebar-section { padding: 16px; border-bottom: 1px solid var(--border); }
        .sidebar-label {
          font-family: var(--mono); font-size: 10px; font-weight: 600;
          color: var(--muted); letter-spacing: 0.12em; text-transform: uppercase;
          margin-bottom: 10px;
        }

        /* window selector */
        .window-pills { display: flex; gap: 6px; }
        .window-pill {
          flex: 1; padding: 6px 0; border-radius: 4px; border: 1px solid var(--border2);
          background: transparent; color: var(--muted); font-family: var(--mono);
          font-size: 11px; cursor: pointer; transition: all 0.15s;
        }
        .window-pill:hover { border-color: var(--accent2); color: var(--accent); }
        .window-pill.active { background: var(--accent); border-color: var(--accent); color: #000; font-weight: 600; }

        /* focus toggles */
        .focus-list { display: flex; flex-direction: column; gap: 6px; }
        .focus-toggle {
          display: flex; align-items: center; gap: 10px;
          padding: 8px 10px; border-radius: 4px; border: 1px solid var(--border2);
          background: transparent; color: var(--text); font-family: var(--sans);
          font-size: 13px; cursor: pointer; transition: all 0.15s; text-align: left;
        }
        .focus-toggle:hover { border-color: var(--accent2); }
        .focus-toggle.active { border-color: var(--accent); background: rgba(0,212,255,0.06); color: var(--accent); }
        .focus-icon { font-size: 14px; }
        .focus-check { margin-left: auto; width: 14px; height: 14px; border-radius: 2px;
          border: 1px solid var(--border2); display: flex; align-items: center; justify-content: center; }
        .focus-toggle.active .focus-check { background: var(--accent); border-color: var(--accent); }
        .focus-check::after { content: '✓'; font-size: 9px; color: #000; display: none; }
        .focus-toggle.active .focus-check::after { display: block; }

        /* instance list */
        .inst-list { flex: 1; overflow-y: auto; padding: 8px; }
        .inst-list::-webkit-scrollbar { width: 4px; }
        .inst-list::-webkit-scrollbar-track { background: transparent; }
        .inst-list::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 2px; }
        .inst-all-btn {
          width: 100%; padding: 7px 10px; margin-bottom: 6px;
          border-radius: 4px; border: 1px dashed var(--border2);
          background: transparent; color: var(--muted); font-family: var(--mono);
          font-size: 11px; cursor: pointer; transition: all 0.15s;
        }
        .inst-all-btn:hover { border-color: var(--accent2); color: var(--accent); }
        .inst-all-btn.active { border-color: var(--green); color: var(--green); }
        .inst-card {
          width: 100%; padding: 10px; margin-bottom: 4px;
          border-radius: 4px; border: 1px solid var(--border);
          background: transparent; text-align: left; cursor: pointer;
          transition: all 0.15s;
        }
        .inst-card:hover { border-color: var(--border2); background: rgba(255,255,255,0.02); }
        .inst-card-selected { border-color: var(--accent) !important; background: rgba(0,212,255,0.05) !important; }
        .inst-id { font-family: var(--mono); font-size: 11px; color: var(--accent); }
        .inst-meta { font-size: 12px; color: var(--text); margin-top: 2px; }
        .inst-type { font-family: var(--mono); font-size: 10px; color: var(--muted); margin-top: 2px; }

        /* loading skeleton */
        .skeleton { background: linear-gradient(90deg, var(--border) 25%, var(--border2) 50%, var(--border) 75%);
          background-size: 200% 100%; animation: shimmer 1.2s infinite; border-radius: 4px; height: 52px; margin-bottom: 4px; }
        @keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }

        /* ── Main panel ── */
        .main { display: flex; flex-direction: column; overflow: hidden; }

        .toolbar {
          display: flex; align-items: center; gap: 12px;
          padding: 12px 20px;
          border-bottom: 1px solid var(--border);
          background: var(--surface);
        }
        .question-input {
          flex: 1; padding: 8px 12px; border-radius: 4px;
          border: 1px solid var(--border2); background: var(--bg);
          color: var(--text); font-family: var(--sans); font-size: 13px;
          outline: none; transition: border 0.15s;
        }
        .question-input::placeholder { color: var(--muted); }
        .question-input:focus { border-color: var(--accent2); }

        .btn {
          padding: 8px 20px; border-radius: 4px; border: none;
          font-family: var(--mono); font-size: 12px; font-weight: 600;
          cursor: pointer; transition: all 0.15s; letter-spacing: 0.05em;
        }
        .btn-primary { background: var(--accent); color: #000; }
        .btn-primary:hover { background: #00eeff; }
        .btn-primary:disabled { background: var(--muted); cursor: not-allowed; color: var(--bg); }
        .btn-stop { background: transparent; border: 1px solid var(--red); color: var(--red); }
        .btn-stop:hover { background: rgba(255,68,68,0.1); }
        .btn-clear { background: transparent; border: 1px solid var(--border2); color: var(--muted); }
        .btn-clear:hover { border-color: var(--muted); color: var(--text); }

        .output-area {
          flex: 1; overflow-y: auto; padding: 24px 32px;
          font-family: var(--sans); font-size: 14px; line-height: 1.7;
        }
        .output-area::-webkit-scrollbar { width: 6px; }
        .output-area::-webkit-scrollbar-track { background: transparent; }
        .output-area::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 3px; }

        /* empty state */
        .empty-state {
          display: flex; flex-direction: column; align-items: center; justify-content: center;
          height: 100%; gap: 16px; color: var(--muted);
        }
        .empty-glyph { font-size: 48px; opacity: 0.3; }
        .empty-text { font-family: var(--mono); font-size: 13px; }
        .empty-hint { font-size: 12px; opacity: 0.6; }

        /* error */
        .error-box {
          margin: 24px; padding: 16px; border-radius: 6px;
          border: 1px solid var(--red); background: rgba(255,68,68,0.05);
          font-family: var(--mono); font-size: 12px; color: var(--red);
        }

        /* cursor blink */
        .cursor { display: inline-block; width: 2px; height: 1em; background: var(--accent);
          margin-left: 2px; vertical-align: text-bottom; animation: blink 0.8s step-end infinite; }
        @keyframes blink { 0%,100% { opacity: 1; } 50% { opacity: 0; } }

        /* ── Markdown styles ── */
        .md-h1 { font-size: 22px; font-weight: 600; color: var(--accent); margin: 24px 0 12px; border-bottom: 1px solid var(--border2); padding-bottom: 8px; }
        .md-h2 { font-size: 18px; font-weight: 600; color: var(--text); margin: 20px 0 10px; }
        .md-h3 { font-size: 15px; font-weight: 600; color: var(--accent); margin: 16px 0 8px; font-family: var(--mono); }
        .md-h4 { font-size: 13px; font-weight: 600; color: var(--amber); margin: 12px 0 6px; font-family: var(--mono); }
        .md-p { margin: 8px 0; }
        .md-code { font-family: var(--mono); font-size: 12px; padding: 2px 6px; border-radius: 3px;
          background: rgba(0,212,255,0.1); color: var(--accent); }
        .md-pre { background: #070910; border: 1px solid var(--border2); border-radius: 6px;
          padding: 16px; margin: 12px 0; overflow-x: auto; }
        .md-pre code { font-family: var(--mono); font-size: 11px; color: #8899aa; }
        .md-table { width: 100%; border-collapse: collapse; margin: 16px 0; font-size: 12px; }
        .md-th { background: var(--surface); padding: 8px 12px; text-align: left;
          font-family: var(--mono); font-size: 10px; color: var(--muted); letter-spacing: 0.08em;
          text-transform: uppercase; border: 1px solid var(--border2); }
        .md-td { padding: 7px 12px; border: 1px solid var(--border); color: var(--text);
          font-family: var(--mono); font-size: 12px; }
        tr:nth-child(even) .md-td { background: rgba(255,255,255,0.01); }
        tr:hover .md-td { background: rgba(0,212,255,0.03); }
        .md-hr { border: none; border-top: 1px solid var(--border2); margin: 20px 0; }
        .md-ul { padding-left: 20px; margin: 8px 0; }
        .md-li { margin: 4px 0; }

        /* badge */
        .badge { padding: 2px 8px; border-radius: 3px; font-family: var(--mono); font-size: 10px; font-weight: 600; letter-spacing: 0.05em; }
        .badge-critical { background: rgba(255,68,68,0.15); color: var(--red); border: 1px solid var(--red); }
        .badge-high     { background: rgba(255,184,0,0.15); color: var(--amber); border: 1px solid var(--amber); }
        .badge-medium   { background: rgba(168,85,247,0.15); color: var(--purple); border: 1px solid var(--purple); }
        .badge-low      { background: rgba(0,255,136,0.15); color: var(--green); border: 1px solid var(--green); }

        /* pill */
        .pill { display: flex; flex-direction: column; align-items: center; padding: 8px 12px;
          border-radius: 4px; border: 1px solid var(--border2); background: var(--bg); min-width: 80px; }
        .pill-warn { border-color: var(--amber); background: rgba(255,184,0,0.05); }
        .pill-label { font-family: var(--mono); font-size: 9px; color: var(--muted); letter-spacing: 0.1em; text-transform: uppercase; }
        .pill-value { font-family: var(--mono); font-size: 15px; font-weight: 600; color: var(--text); margin-top: 4px; }
        .pill-warn .pill-value { color: var(--amber); }
      `}</style>

      <div className="app">
        {/* ── Header ── */}
        <header className="header">
          <div className="header-logo">▸ EC2 ANALYSIS AGENT</div>
          <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--muted)" }}>
            powered by Groq · llama-3.3-70b-versatile
          </div>
          <div className="header-sep" />
          <div className={`status-dot ${status === "streaming" ? "active" : status === "error" ? "error" : status === "done" ? "done" : ""}`} />
          <div className="status-label">
            {status === "idle"      && "ready"}
            {status === "loading"   && "fetching data..."}
            {status === "streaming" && "generating..."}
            {status === "done"      && "complete"}
            {status === "error"     && "error"}
          </div>
        </header>

        {/* ── Sidebar ── */}
        <aside className="sidebar">
          {/* Window */}
          <div className="sidebar-section">
            <div className="sidebar-label">Time Window</div>
            <div className="window-pills">
              {windowOptions.map(w => (
                <button key={w} className={`window-pill ${window === w ? "active" : ""}`}
                  onClick={() => setWindow(w)}>{w}d</button>
              ))}
            </div>
          </div>

          {/* Focus */}
          <div className="sidebar-section">
            <div className="sidebar-label">Analysis Focus</div>
            <div className="focus-list">
              {focusOptions.map(f => (
                <button key={f.id}
                  className={`focus-toggle ${focus.includes(f.id) ? "active" : ""}`}
                  onClick={() => toggleFocus(f.id)}>
                  <span className="focus-icon">{f.icon}</span>
                  {f.label}
                  <span className="focus-check" />
                </button>
              ))}
            </div>
          </div>

          {/* Instances */}
          <div className="sidebar-section" style={{ paddingBottom: 8 }}>
            <div className="sidebar-label">Instances ({instances.length})</div>
          </div>
          <div className="inst-list">
            <button
              className={`inst-all-btn ${selected.length === 0 ? "active" : ""}`}
              onClick={() => setSelected([])}>
              ◈ ALL INSTANCES
            </button>
            {loadingInst
              ? [1,2,3].map(i => <div key={i} className="skeleton" />)
              : instances.map(inst => (
                  <InstanceCard key={inst.instance_id} inst={inst}
                    selected={selected.includes(inst.instance_id)}
                    onClick={() => toggleSelect(inst.instance_id)} />
                ))
            }
          </div>
        </aside>

        {/* ── Main panel ── */}
        <main className="main">
          <div className="toolbar">
            <input
              className="question-input"
              placeholder="Optional: ask a specific question, e.g. 'Which instances are safe to downsize this week?'"
              value={question}
              onChange={e => setQuestion(e.target.value)}
              onKeyDown={e => e.key === "Enter" && !streaming && handleAnalyse()}
            />
            {streaming
              ? <button className="btn btn-stop" onClick={handleStop}>■ STOP</button>
              : <button className="btn btn-primary" disabled={focus.length === 0} onClick={handleAnalyse}>
                  ▸ ANALYSE
                </button>
            }
            {output && !streaming &&
              <button className="btn btn-clear" onClick={() => { setOutput(""); setStatus("idle"); }}>
                CLEAR
              </button>
            }
          </div>

          <div className="output-area" ref={outputRef}>
            {error && <div className="error-box">⚠ {error}</div>}

            {!output && !error && (
              <div className="empty-state">
                <div className="empty-glyph">⬡</div>
                <div className="empty-text">EC2 FLEET ANALYSER</div>
                <div className="empty-hint">Select a time window, choose focus areas, and click Analyse</div>
                <div className="empty-hint">
                  {selected.length === 0
                    ? `All ${instances.length} instances will be analysed`
                    : `${selected.length} instance${selected.length > 1 ? "s" : ""} selected`}
                </div>
              </div>
            )}

            {output && (
              <div>
                <div
                  dangerouslySetInnerHTML={{ __html: renderMarkdown(output) }}
                />
                {streaming && <span className="cursor" />}
              </div>
            )}
          </div>
        </main>
      </div>
    </>
  );
}