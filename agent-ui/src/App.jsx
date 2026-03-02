import { useState, useRef, useEffect, useCallback } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer } from "recharts";

const API = "http://localhost:8000";

// Each markdown element maps to a styled React component.
// This replaces the old fragile regex-based renderer entirely.
const MD_COMPONENTS = {
  h1: ({ children }) => <h1 className="md-h1">{children}</h1>,
  h2: ({ children }) => <h2 className="md-h2">{children}</h2>,
  h3: ({ children }) => <h3 className="md-h3">{children}</h3>,
  h4: ({ children }) => <h4 className="md-h4">{children}</h4>,
  p:  ({ children }) => <p  className="md-p">{children}</p>,
  strong: ({ children }) => <strong className="md-strong">{children}</strong>,
  em:     ({ children }) => <em className="md-em">{children}</em>,
  hr:     () => <hr className="md-hr" />,
  ul: ({ children }) => <ul className="md-ul">{children}</ul>,
  ol: ({ children }) => <ol className="md-ol">{children}</ol>,
  li: ({ children }) => <li className="md-li">{children}</li>,
  blockquote: ({ children }) => <blockquote className="md-blockquote">{children}</blockquote>,
  code: ({ inline, children }) =>
    inline
      ? <code className="md-code">{children}</code>
      : <pre className="md-pre"><code>{children}</code></pre>,
  pre: ({ children }) => <>{children}</>,
  table: ({ children }) => (
    <div className="md-table-wrap">
      <table className="md-table">{children}</table>
    </div>
  ),
  thead: ({ children }) => <thead className="md-thead">{children}</thead>,
  tbody: ({ children }) => <tbody>{children}</tbody>,
  tr:    ({ children }) => <tr className="md-tr">{children}</tr>,
  th:    ({ children }) => <th className="md-th">{children}</th>,
  td:    ({ children }) => <td className="md-td">{children}</td>,
};

function InstanceCard({ inst, selected, onClick }) {
  return (
    <button
      className={`inst-card ${selected ? "inst-card-selected" : ""}`}
      onClick={onClick}
    >
      <div className="inst-id">{inst.instance_id}</div>
      <div className="inst-meta">{inst.instance_name || "unnamed"}</div>
      <div className="inst-type">{inst.instance_type} - {inst.az}</div>
    </button>
  );
}


function TimeSeriesChart({ instanceId, windowDays }) {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetch(`${API}/timeseries?instance_id=${instanceId}&window_days=${windowDays}`)
      .then(r => r.json())
      .then(d => {
        setData(d.timeseries || []);
      })
      .finally(() => setLoading(false));
  }, [instanceId, windowDays]);

  if (loading) return <div className="skeleton" style={{ height: 250, marginBottom: 24 }} />;
  if (!data.length) return null;

  return (
    <div style={{ height: 250, marginBottom: 24, padding: 16, border: "1px solid var(--border2)", borderRadius: 8, background: "#0d1017" }}>
      <h3 style={{ fontSize: 13, color: "var(--muted)", marginBottom: 12, fontFamily: "var(--mono)", textTransform: "uppercase" }}>
        Metrics timeline ({instanceId})
      </h3>
      <div style={{ height: 180 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 0, right: 10, left: -20, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a3040" vertical={false} />
            <XAxis dataKey="date" stroke="#4a5568" fontSize={10} tickMargin={8} />
            <YAxis stroke="#4a5568" fontSize={10} domain={[0, 100]} />
            <RechartsTooltip 
              contentStyle={{ backgroundColor: "#111318", border: "1px solid #2a3040", borderRadius: 4, fontSize: 12 }}
              itemStyle={{ padding: 0 }}
            />
            <Legend wrapperStyle={{ fontSize: 11, paddingTop: 10 }} />
            <Line type="monotone" name="CPU Avg %" dataKey="cpu_avg" stroke="#0099bb" strokeWidth={2} dot={false} />
            <Line type="monotone" name="CPU Max %" dataKey="cpu_max" stroke="#00d4ff" strokeWidth={1} strokeDasharray="4 4" dot={false} />
            <Line type="monotone" name="Mem Avg %" dataKey="mem_avg" stroke="#a855f7" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default function App() {
  const [instances, setInstances]     = useState([]);
  const [selected, setSelected]       = useState([]);
  const [win, setWin]                 = useState(30);
  const [focus, setFocus]             = useState(["rightsizing", "risk_warnings", "full_report"]);
  const [question, setQuestion]       = useState("");
  const [output, setOutput]           = useState("");
  const [streaming, setStreaming]     = useState(false);
  const [status, setStatus]           = useState("idle");
  const [error, setError]             = useState(null);
  const [loadingInst, setLoadingInst] = useState(true);
  const outputRef = useRef(null);
  const abortRef  = useRef(null);

  useEffect(() => {
    fetch(`${API}/instances`)
      .then(r => r.json())
      .then(d => setInstances(d.instances || []))
      .catch(() => setError("Cannot reach backend. Is uvicorn running?"))
      .finally(() => setLoadingInst(false));
  }, []);

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
          window_days:  win,
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
      const reader  = res.body.getReader();
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
  }, [win, selected, question, focus]);

  const handleStop = () => {
    abortRef.current?.abort();
    setStreaming(false);
    setStatus("idle");
  };

  const focusOptions = [
    { id: "rightsizing",   label: "Rightsizing",   symbol: "=" },
    { id: "risk_warnings", label: "Risk Warnings", symbol: "!" },
    { id: "full_report",   label: "Full Report",   symbol: "#" },
  ];

  const windowOptions = [10, 30, 60, 90];

  return (
    <>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

        :root {
          --bg:      #0a0c0f;
          --surface: #111318;
          --border:  #1e2229;
          --border2: #2a3040;
          --text:    #c8d0dc;
          --muted:   #4a5568;
          --accent:  #00d4ff;
          --accent2: #0099bb;
          --green:   #00ff88;
          --amber:   #ffb800;
          --red:     #ff4444;
          --purple:  #a855f7;
          --mono:    'IBM Plex Mono', monospace;
          --sans:    'IBM Plex Sans', sans-serif;
        }

        html, body, #root {
          height: 100%; width: 100%; margin: 0; padding: 0;
          overflow: hidden; background: var(--bg);
          color: var(--text); font-family: var(--sans);
        }

        .app {
          display: grid;
          grid-template-columns: 280px 1fr;
          grid-template-rows: 56px 1fr;
          height: 100vh; width: 100vw; overflow: hidden;
        }

        /* Header */
        .header {
          grid-column: 1 / -1;
          display: flex; align-items: center; gap: 16px;
          padding: 0 24px; width: 100%; min-width: 0;
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
          background: var(--muted); transition: background 0.3s; flex-shrink: 0;
        }
        .status-dot.active { background: var(--green); box-shadow: 0 0 8px var(--green); animation: pulse 1s infinite; }
        .status-dot.error  { background: var(--red); }
        .status-dot.done   { background: var(--accent); }
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }
        .status-label { font-family: var(--mono); font-size: 11px; color: var(--muted); white-space: nowrap; }

        /* Sidebar */
        .sidebar {
          background: var(--surface); border-right: 1px solid var(--border);
          display: flex; flex-direction: column; overflow: hidden; min-width: 0;
        }
        .sidebar-section { padding: 16px; border-bottom: 1px solid var(--border); }
        .sidebar-label {
          font-family: var(--mono); font-size: 10px; font-weight: 600;
          color: var(--muted); letter-spacing: 0.12em; text-transform: uppercase; margin-bottom: 10px;
        }

        /* Window pills */
        .window-pills { display: flex; gap: 6px; }
        .window-pill {
          flex: 1; padding: 6px 0; border-radius: 4px; border: 1px solid var(--border2);
          background: transparent; color: var(--muted);
          font-family: var(--mono); font-size: 11px; cursor: pointer; transition: all 0.15s;
        }
        .window-pill:hover { border-color: var(--accent2); color: var(--accent); }
        .window-pill.active { background: var(--accent); border-color: var(--accent); color: #000; font-weight: 600; }

        /* Focus toggles */
        .focus-list { display: flex; flex-direction: column; gap: 6px; }
        .focus-toggle {
          display: flex; align-items: center; gap: 10px; padding: 8px 10px;
          border-radius: 4px; border: 1px solid var(--border2);
          background: transparent; color: var(--text);
          font-family: var(--sans); font-size: 13px; cursor: pointer; transition: all 0.15s; text-align: left;
        }
        .focus-toggle:hover { border-color: var(--accent2); }
        .focus-toggle.active { border-color: var(--accent); background: rgba(0,212,255,0.06); color: var(--accent); }
        .focus-icon {
          width: 18px; height: 18px; border-radius: 3px; background: var(--border2);
          display: flex; align-items: center; justify-content: center;
          font-size: 11px; font-family: var(--mono); color: var(--muted); flex-shrink: 0;
        }
        .focus-toggle.active .focus-icon { background: rgba(0,212,255,0.15); color: var(--accent); }
        .focus-check {
          margin-left: auto; width: 14px; height: 14px; border-radius: 2px;
          border: 1px solid var(--border2); flex-shrink: 0; transition: all 0.15s;
        }
        .focus-toggle.active .focus-check {
          background: var(--accent); border-color: var(--accent);
          background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 10 8'%3E%3Cpath d='M1 4l3 3 5-6' stroke='%23000' stroke-width='1.5' fill='none' stroke-linecap='round'/%3E%3C/svg%3E");
          background-size: 8px; background-repeat: no-repeat; background-position: center;
        }

        /* Instance list */
        .inst-list { flex: 1; overflow-y: auto; padding: 8px; }
        .inst-list::-webkit-scrollbar { width: 4px; }
        .inst-list::-webkit-scrollbar-track { background: transparent; }
        .inst-list::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 2px; }
        .inst-all-btn {
          width: 100%; padding: 7px 10px; margin-bottom: 6px;
          border-radius: 4px; border: 1px dashed var(--border2);
          background: transparent; color: var(--muted);
          font-family: var(--mono); font-size: 11px; cursor: pointer; transition: all 0.15s;
        }
        .inst-all-btn:hover { border-color: var(--accent2); color: var(--accent); }
        .inst-all-btn.active { border-color: var(--green); color: var(--green); }
        .inst-card {
          width: 100%; padding: 10px; margin-bottom: 4px;
          border-radius: 4px; border: 1px solid var(--border);
          background: transparent; text-align: left; cursor: pointer; transition: all 0.15s;
        }
        .inst-card:hover { border-color: var(--border2); background: rgba(255,255,255,0.02); }
        .inst-card-selected { border-color: var(--accent) !important; background: rgba(0,212,255,0.05) !important; }
        .inst-id   { font-family: var(--mono); font-size: 11px; color: var(--accent); }
        .inst-meta { font-size: 12px; color: var(--text); margin-top: 2px; }
        .inst-type { font-family: var(--mono); font-size: 10px; color: var(--muted); margin-top: 2px; }

        /* Skeleton */
        .skeleton {
          background: linear-gradient(90deg, var(--border) 25%, var(--border2) 50%, var(--border) 75%);
          background-size: 200% 100%; animation: shimmer 1.2s infinite;
          border-radius: 4px; height: 52px; margin-bottom: 4px;
        }
        @keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }

        /* Main panel */
        .main { display: flex; flex-direction: column; overflow: hidden; min-width: 0; width: 100%; }

        /* Toolbar */
        .toolbar {
          display: flex; align-items: center; gap: 12px;
          padding: 12px 20px; border-bottom: 1px solid var(--border);
          background: var(--surface); flex-shrink: 0;
        }
        .question-input {
          flex: 1; padding: 8px 12px; border-radius: 4px;
          border: 1px solid var(--border2); background: var(--bg);
          color: var(--text); font-family: var(--sans); font-size: 13px;
          outline: none; transition: border 0.15s; min-width: 0;
        }
        .question-input::placeholder { color: var(--muted); }
        .question-input:focus { border-color: var(--accent2); }
        .btn {
          padding: 8px 20px; border-radius: 4px; border: none;
          font-family: var(--mono); font-size: 12px; font-weight: 600;
          cursor: pointer; transition: all 0.15s;
          letter-spacing: 0.05em; white-space: nowrap; flex-shrink: 0;
        }
        .btn-primary { background: var(--accent); color: #000; }
        .btn-primary:hover { background: #00eeff; }
        .btn-primary:disabled { background: var(--muted); cursor: not-allowed; color: var(--bg); }
        .btn-stop  { background: transparent; border: 1px solid var(--red); color: var(--red); }
        .btn-stop:hover { background: rgba(255,68,68,0.1); }
        .btn-clear { background: transparent; border: 1px solid var(--border2); color: var(--muted); }
        .btn-clear:hover { border-color: var(--muted); color: var(--text); }

        /* Output area */
        .output-area {
          flex: 1; overflow-y: auto; padding: 28px 36px;
          font-family: var(--sans); font-size: 14px; line-height: 1.75;
        }
        .output-area::-webkit-scrollbar { width: 6px; }
        .output-area::-webkit-scrollbar-track { background: transparent; }
        .output-area::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 3px; }

        /* Empty state */
        .empty-state {
          display: flex; flex-direction: column; align-items: center;
          justify-content: center; height: 100%; gap: 16px; color: var(--muted);
        }
        .empty-glyph { font-size: 32px; opacity: 0.2; font-family: var(--mono); letter-spacing: 0.1em; }
        .empty-text  { font-family: var(--mono); font-size: 13px; letter-spacing: 0.1em; }
        .empty-hint  { font-size: 12px; opacity: 0.6; }

        /* Error */
        .error-box {
          padding: 16px; border-radius: 6px;
          border: 1px solid var(--red); background: rgba(255,68,68,0.05);
          font-family: var(--mono); font-size: 12px; color: var(--red);
        }

        /* Streaming cursor */
        .cursor {
          display: inline-block; width: 2px; height: 1em; background: var(--accent);
          margin-left: 2px; vertical-align: text-bottom;
          animation: blink 0.8s step-end infinite;
        }
        @keyframes blink { 0%,100% { opacity: 1; } 50% { opacity: 0; } }

        /* Markdown elements */
        .md-h1 {
          font-size: 20px; font-weight: 600; color: var(--accent);
          margin: 28px 0 14px; padding-bottom: 8px;
          border-bottom: 1px solid var(--border2); font-family: var(--sans);
        }
        .md-h1:first-child { margin-top: 0; }
        .md-h2 { font-size: 16px; font-weight: 600; color: var(--text); margin: 22px 0 10px; }
        .md-h3 {
          font-size: 13px; font-weight: 600; color: var(--accent);
          margin: 18px 0 8px; font-family: var(--mono);
          letter-spacing: 0.05em; text-transform: uppercase;
        }
        .md-h4 { font-size: 12px; font-weight: 600; color: var(--amber); margin: 14px 0 6px; font-family: var(--mono); }
        .md-p { margin: 0 0 10px; color: var(--text); }
        .md-strong { color: #e8edf4; font-weight: 600; }
        .md-em { font-style: italic; }
        .md-hr { border: none; border-top: 1px solid var(--border2); margin: 22px 0; }
        .md-ul, .md-ol { padding-left: 22px; margin: 0 0 10px; }
        .md-li { margin: 4px 0; color: var(--text); }
        .md-blockquote {
          border-left: 3px solid var(--accent2); margin: 12px 0; padding: 8px 16px;
          background: rgba(0,212,255,0.04); border-radius: 0 4px 4px 0;
          color: var(--muted); font-style: italic;
        }
        .md-code {
          font-family: var(--mono); font-size: 12px; padding: 2px 7px; border-radius: 3px;
          background: rgba(0,212,255,0.08); color: var(--accent); border: 1px solid rgba(0,212,255,0.15);
        }
        .md-pre {
          background: #07090d; border: 1px solid var(--border2);
          border-radius: 6px; padding: 16px; margin: 12px 0; overflow-x: auto;
        }
        .md-pre code {
          font-family: var(--mono); font-size: 11px; color: #8899bb;
          background: none; border: none; padding: 0;
        }

        /* Tables */
        .md-table-wrap {
          overflow-x: auto; margin: 16px 0;
          border: 1px solid var(--border2); border-radius: 6px;
        }
        .md-table { width: 100%; border-collapse: collapse; font-size: 12px; font-family: var(--mono); }
        .md-thead { background: #0d1017; }
        .md-th {
          padding: 9px 14px; text-align: left; font-size: 10px; font-weight: 600;
          color: var(--muted); letter-spacing: 0.1em; text-transform: uppercase;
          border-bottom: 1px solid var(--border2); white-space: nowrap;
        }
        .md-td {
          padding: 8px 14px; border-bottom: 1px solid var(--border);
          color: var(--text); vertical-align: top;
        }
        .md-tr:last-child .md-td { border-bottom: none; }
        .md-tr:nth-child(even) .md-td { background: rgba(255,255,255,0.01); }
        .md-tr:hover .md-td { background: rgba(0,212,255,0.025); }
      `}</style>

      <div className="app">

        <header className="header">
          <div className="header-logo">EC2 ANALYSIS AGENT</div>
          <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--muted)" }}>
            Groq / llama-3.3-70b-versatile
          </div>
          <div className="header-sep" />
          <div className={`status-dot ${
            status === "streaming" ? "active" :
            status === "error"     ? "error"  :
            status === "done"      ? "done"   : ""
          }`} />
          <div className="status-label">
            {status === "idle"      && "ready"}
            {status === "loading"   && "fetching data..."}
            {status === "streaming" && "generating..."}
            {status === "done"      && "complete"}
            {status === "error"     && "error"}
          </div>
        </header>

        <aside className="sidebar">
          <div className="sidebar-section">
            <div className="sidebar-label">Time Window</div>
            <div className="window-pills">
              {windowOptions.map(w => (
                <button key={w}
                  className={`window-pill ${win === w ? "active" : ""}`}
                  onClick={() => setWin(w)}>
                  {w}d
                </button>
              ))}
            </div>
          </div>

          <div className="sidebar-section">
            <div className="sidebar-label">Analysis Focus</div>
            <div className="focus-list">
              {focusOptions.map(f => (
                <button key={f.id}
                  className={`focus-toggle ${focus.includes(f.id) ? "active" : ""}`}
                  onClick={() => toggleFocus(f.id)}>
                  <span className="focus-icon">{f.symbol}</span>
                  {f.label}
                  <span className="focus-check" />
                </button>
              ))}
            </div>
          </div>

          <div className="sidebar-section" style={{ paddingBottom: 8 }}>
            <div className="sidebar-label">Instances ({instances.length})</div>
          </div>

          <div className="inst-list">
            <button
              className={`inst-all-btn ${selected.length === 0 ? "active" : ""}`}
              onClick={() => setSelected([])}>
              ALL INSTANCES
            </button>
            {loadingInst
              ? [1, 2, 3].map(i => <div key={i} className="skeleton" />)
              : instances.map(inst => (
                  <InstanceCard
                    key={inst.instance_id}
                    inst={inst}
                    selected={selected.includes(inst.instance_id)}
                    onClick={() => toggleSelect(inst.instance_id)}
                  />
                ))
            }
          </div>
        </aside>

        <main className="main">
          <div className="toolbar">
            <input
              className="question-input"
              placeholder="Optional: ask a specific question about your fleet..."
              value={question}
              onChange={e => setQuestion(e.target.value)}
              onKeyDown={e => e.key === "Enter" && !streaming && handleAnalyse()}
            />
            {streaming
              ? <button className="btn btn-stop" onClick={handleStop}>STOP</button>
              : <button className="btn btn-primary"
                  disabled={focus.length === 0}
                  onClick={handleAnalyse}>
                  ANALYSE
                </button>
            }
            {output && !streaming &&
              <button className="btn btn-clear"
                onClick={() => { setOutput(""); setStatus("idle"); }}>
                CLEAR
              </button>
            }
          </div>

          <div className="output-area" ref={outputRef}>
            {error && <div className="error-box">{error}</div>}

            {!output && !error && status !== "loading" && status !== "streaming" && (
              <div className="empty-state">
                <div className="empty-glyph">[ EC2 ]</div>
                <div className="empty-text">EC2 FLEET ANALYSER</div>
                <div className="empty-hint">Select a time window, choose focus areas, and click Analyse</div>
                <div className="empty-hint">
                  {selected.length === 0
                    ? `All ${instances.length} instances will be analysed`
                    : `${selected.length} instance${selected.length > 1 ? "s" : ""} selected`}
                </div>
              </div>
            )}

            { (status === "loading" || (status === "streaming" && !output)) && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                 <div className="skeleton" style={{ height: 32, width: '40%' }} />
                 <div className="skeleton" style={{ height: 20, width: '90%' }} />
                 <div className="skeleton" style={{ height: 20, width: '85%' }} />
                 <div className="skeleton" style={{ height: 20, width: '95%' }} />
                 <div className="skeleton" style={{ height: 100, width: '100%' }} />
              </div>
            )}

            {(output || status === "loading" || status === "streaming") && selected.length === 1 && (
               <TimeSeriesChart instanceId={selected[0]} windowDays={win} />
            )}

            {output && (
              <div>
                <ReactMarkdown
                  remarkPlugins={[remarkGfm]}
                  components={MD_COMPONENTS}
                >
                  {output}
                </ReactMarkdown>
                {streaming && <span className="cursor" />}
              </div>
            )}
          </div>
        </main>

      </div>
    </>
  );
}