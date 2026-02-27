import { useState, useRef, useEffect, useCallback } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

const API = "http://localhost:8000";

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

// ── Stat chip in the top bar ──────────────────────────────────────
function StatChip({ label, value, accent }) {
  return (
    <div className="stat-chip">
      <span className="stat-label">{label}</span>
      <span className={`stat-value ${accent ? "stat-accent" : ""}`}>{value}</span>
    </div>
  );
}

// ── Instance card ─────────────────────────────────────────────────
function InstanceCard({ inst, selected, onClick }) {
  const stateColor = inst.state === "running" ? "#16a34a" : inst.state === "stopped" ? "#dc2626" : "#d97706";
  return (
    <button
      className={`inst-card ${selected ? "inst-card-selected" : ""}`}
      onClick={onClick}
    >
      <div className="inst-card-top">
        <span className="inst-id">{inst.instance_id}</span>
        <span className="inst-state-dot" style={{ background: stateColor }} />
      </div>
      <div className="inst-name">{inst.instance_name || "unnamed"}</div>
      <div className="inst-badges">
        <span className="inst-badge">{inst.instance_type}</span>
        <span className="inst-badge">{inst.az?.split("-").slice(-1)[0]}</span>
        <span className="inst-badge">{inst.platform || "linux"}</span>
      </div>
    </button>
  );
}

// ── Copy button ───────────────────────────────────────────────────
function CopyButton({ text }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };
  return (
    <button className="copy-btn" onClick={handleCopy}>
      {copied ? (
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
          <polyline points="20 6 9 17 4 12"/>
        </svg>
      ) : (
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/>
        </svg>
      )}
      {copied ? "Copied" : "Copy"}
    </button>
  );
}

// ── Main App ──────────────────────────────────────────────────────
export default function App() {
  const [instances, setInstances]       = useState([]);
  const [selected, setSelected]         = useState([]);
  const [win, setWin]                   = useState(30);
  const [focus, setFocus]               = useState(["rightsizing", "risk_warnings", "full_report"]);
  const [question, setQuestion]         = useState("");
  const [output, setOutput]             = useState("");
  const [streaming, setStreaming]       = useState(false);
  const [status, setStatus]             = useState("idle");
  const [error, setError]               = useState(null);
  const [loadingInst, setLoadingInst]   = useState(true);
  const [sidebarOpen, setSidebarOpen]   = useState(true);
  const outputRef = useRef(null);
  const abortRef  = useRef(null);

  useEffect(() => {
    fetch(`${API}/instances`)
      .then(r => r.json())
      .then(d => setInstances(d.instances || []))
      .catch(() => setError("Cannot reach backend. Is uvicorn running on port 8000?"))
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
    { id: "rightsizing",   label: "Rightsizing",   desc: "Instance type recommendations" },
    { id: "risk_warnings", label: "Risk Warnings", desc: "Performance & reliability flags" },
    { id: "full_report",   label: "Full Report",   desc: "Executive summary & action plan" },
  ];

  const windowOptions = [
    { val: 10, label: "10d" },
    { val: 30, label: "30d" },
    { val: 60, label: "60d" },
    { val: 90, label: "90d" },
  ];

  const runningCount = instances.filter(i => i.state === "running").length;
  const analysingCount = selected.length === 0 ? instances.length : selected.length;

  const statusConfig = {
    idle:      { label: "Ready",        color: "#64748b", bg: "#f1f5f9" },
    loading:   { label: "Fetching...",  color: "#d97706", bg: "#fef3c7" },
    streaming: { label: "Generating",   color: "#0891b2", bg: "#e0f2fe" },
    done:      { label: "Complete",     color: "#16a34a", bg: "#dcfce7" },
    error:     { label: "Error",        color: "#dc2626", bg: "#fee2e2" },
  };
  const sc = statusConfig[status] || statusConfig.idle;

  return (
    <>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,400&family=JetBrains+Mono:wght@400;500&display=swap');

        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

        :root {
          --bg:        #f8f7f4;
          --canvas:    #ffffff;
          --surface:   #f1f0ed;
          --border:    #e5e3de;
          --border2:   #d1cec7;
          --text:      #1c1917;
          --text2:     #57534e;
          --muted:     #a8a29e;
          --accent:    #e85d26;
          --accent-lt: #fff4ef;
          --accent2:   #0891b2;
          --accent2-lt:#e0f2fe;
          --green:     #16a34a;
          --green-lt:  #dcfce7;
          --amber:     #d97706;
          --amber-lt:  #fef3c7;
          --red:       #dc2626;
          --red-lt:    #fee2e2;
          --shadow-sm: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
          --shadow-md: 0 4px 12px rgba(0,0,0,0.08), 0 2px 4px rgba(0,0,0,0.04);
          --mono:      'JetBrains Mono', monospace;
          --sans:      'DM Sans', sans-serif;
          --radius:    8px;
        }

        html, body, #root {
          height: 100%; width: 100%; margin: 0; padding: 0;
          overflow: hidden; background: var(--bg);
          color: var(--text); font-family: var(--sans);
          -webkit-font-smoothing: antialiased;
        }

        /* ── Layout grid ── */
        .app {
          display: grid;
          grid-template-rows: auto auto 1fr;
          grid-template-columns: 1fr;
          height: 100vh; width: 100vw; overflow: hidden;
        }

        /* ── Top nav ── */
        .topnav {
          display: flex; align-items: center; gap: 0;
          padding: 0 20px;
          height: 52px;
          background: var(--canvas);
          border-bottom: 1px solid var(--border);
          box-shadow: var(--shadow-sm);
          z-index: 10;
        }
        .nav-logo {
          display: flex; align-items: center; gap: 10px;
          margin-right: 24px;
        }
        .nav-logo-icon {
          width: 28px; height: 28px; border-radius: 7px;
          background: var(--accent);
          display: flex; align-items: center; justify-content: center;
          flex-shrink: 0;
        }
        .nav-logo-icon svg { display: block; }
        .nav-logo-text {
          font-size: 14px; font-weight: 600; color: var(--text);
          letter-spacing: -0.02em;
        }
        .nav-logo-sub {
          font-size: 11px; color: var(--muted); margin-top: 1px;
          font-family: var(--mono); font-weight: 400;
        }
        .nav-divider {
          width: 1px; height: 24px; background: var(--border);
          margin: 0 16px; flex-shrink: 0;
        }
        .nav-stats { display: flex; gap: 4px; flex: 1; }
        .nav-right { display: flex; align-items: center; gap: 10px; margin-left: auto; }

        .stat-chip {
          display: flex; align-items: center; gap: 6px;
          padding: 5px 12px; border-radius: 20px;
          background: var(--surface); border: 1px solid var(--border);
          font-size: 12px;
        }
        .stat-label { color: var(--muted); font-weight: 400; }
        .stat-value { color: var(--text); font-weight: 600; font-family: var(--mono); font-size: 11px; }
        .stat-accent { color: var(--accent); }

        .status-badge {
          display: flex; align-items: center; gap: 6px;
          padding: 5px 12px; border-radius: 20px;
          font-size: 12px; font-weight: 500;
          transition: all 0.2s;
        }
        .status-dot-sm {
          width: 6px; height: 6px; border-radius: 50%;
          background: currentColor; flex-shrink: 0;
        }
        .status-dot-sm.pulse { animation: pulse 1.2s ease-in-out infinite; }
        @keyframes pulse { 0%,100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.5; transform: scale(0.8); } }

        .sidebar-toggle {
          width: 32px; height: 32px; border-radius: 6px;
          border: 1px solid var(--border); background: transparent;
          cursor: pointer; display: flex; align-items: center; justify-content: center;
          color: var(--text2); transition: all 0.15s;
        }
        .sidebar-toggle:hover { background: var(--surface); color: var(--text); }

        /* ── Stats bar ── */
        .statsbar {
          display: flex; align-items: center; gap: 0;
          padding: 0 20px;
          height: 44px;
          background: var(--canvas);
          border-bottom: 1px solid var(--border);
          overflow-x: auto;
        }
        .statsbar::-webkit-scrollbar { display: none; }
        .sbar-item {
          display: flex; align-items: center; gap: 8px;
          padding: 0 16px; height: 100%;
          border-right: 1px solid var(--border);
          flex-shrink: 0;
        }
        .sbar-item:first-child { padding-left: 0; }
        .sbar-label { font-size: 11px; color: var(--muted); font-weight: 400; text-transform: uppercase; letter-spacing: 0.06em; }
        .sbar-value { font-family: var(--mono); font-size: 13px; font-weight: 500; color: var(--text); }
        .sbar-value.green { color: var(--green); }
        .sbar-value.amber { color: var(--amber); }
        .sbar-value.accent { color: var(--accent); }
        .sbar-spacer { flex: 1; }

        /* ── Body row (sidebar + main) ── */
        .body-row {
          display: flex; overflow: hidden; min-height: 0;
        }

        /* ── Sidebar ── */
        .sidebar {
          width: 300px; min-width: 300px;
          background: var(--canvas);
          border-right: 1px solid var(--border);
          display: flex; flex-direction: column;
          overflow: hidden;
          transition: width 0.25s ease, min-width 0.25s ease, opacity 0.2s;
        }
        .sidebar.closed {
          width: 0; min-width: 0; opacity: 0; pointer-events: none; overflow: hidden;
        }
        .sidebar-inner { display: flex; flex-direction: column; height: 100%; min-width: 300px; }

        .sidebar-block { padding: 16px 18px; border-bottom: 1px solid var(--border); }
        .block-title {
          font-size: 10px; font-weight: 600; letter-spacing: 0.1em;
          text-transform: uppercase; color: var(--muted); margin-bottom: 10px;
        }

        /* Window selector */
        .window-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 5px; }
        .window-btn {
          padding: 7px 0; border-radius: 6px;
          border: 1px solid var(--border);
          background: transparent; color: var(--text2);
          font-family: var(--mono); font-size: 12px; font-weight: 500;
          cursor: pointer; transition: all 0.15s; text-align: center;
        }
        .window-btn:hover { border-color: var(--accent); color: var(--accent); background: var(--accent-lt); }
        .window-btn.active {
          background: var(--accent); border-color: var(--accent);
          color: #fff; font-weight: 600;
          box-shadow: 0 2px 8px rgba(232,93,38,0.25);
        }

        /* Focus options */
        .focus-options { display: flex; flex-direction: column; gap: 5px; }
        .focus-row {
          display: flex; align-items: flex-start; gap: 10px;
          padding: 10px 12px; border-radius: 8px;
          border: 1px solid var(--border);
          cursor: pointer; transition: all 0.15s; text-align: left;
          background: transparent;
        }
        .focus-row:hover { border-color: var(--accent); background: var(--accent-lt); }
        .focus-row.active { border-color: var(--accent); background: var(--accent-lt); }
        .focus-checkbox {
          width: 16px; height: 16px; border-radius: 4px; flex-shrink: 0; margin-top: 1px;
          border: 1.5px solid var(--border2);
          display: flex; align-items: center; justify-content: center;
          transition: all 0.15s;
        }
        .focus-row.active .focus-checkbox {
          background: var(--accent); border-color: var(--accent);
        }
        .focus-row.active .focus-checkbox svg { display: block; }
        .focus-checkbox svg { display: none; }
        .focus-text { flex: 1; }
        .focus-name { font-size: 13px; font-weight: 500; color: var(--text); }
        .focus-desc { font-size: 11px; color: var(--muted); margin-top: 2px; }
        .focus-row.active .focus-name { color: var(--accent); }

        /* Instance list */
        .inst-section { flex: 1; overflow: hidden; display: flex; flex-direction: column; }
        .inst-header {
          display: flex; align-items: center; justify-content: space-between;
          padding: 14px 18px 10px;
        }
        .inst-count-badge {
          font-family: var(--mono); font-size: 10px; font-weight: 500;
          padding: 2px 7px; border-radius: 10px;
          background: var(--surface); border: 1px solid var(--border);
          color: var(--text2);
        }
        .inst-all-row {
          margin: 0 18px 8px;
          display: flex; align-items: center; gap: 8px;
          padding: 8px 12px; border-radius: 7px;
          border: 1.5px dashed var(--border2);
          cursor: pointer; transition: all 0.15s;
          background: transparent; font-size: 13px; color: var(--text2); font-weight: 500;
        }
        .inst-all-row:hover { border-color: var(--green); color: var(--green); background: var(--green-lt); }
        .inst-all-row.active { border-color: var(--green); color: var(--green); background: var(--green-lt); font-weight: 600; }
        .inst-scroll { flex: 1; overflow-y: auto; padding: 0 18px 18px; }
        .inst-scroll::-webkit-scrollbar { width: 3px; }
        .inst-scroll::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 2px; }

        .inst-card {
          width: 100%; padding: 10px 12px; margin-bottom: 5px;
          border-radius: 8px; border: 1px solid var(--border);
          background: transparent; text-align: left;
          cursor: pointer; transition: all 0.15s;
        }
        .inst-card:hover { border-color: var(--border2); background: var(--surface); box-shadow: var(--shadow-sm); }
        .inst-card-selected {
          border-color: var(--accent) !important;
          background: var(--accent-lt) !important;
          box-shadow: 0 0 0 3px rgba(232,93,38,0.08) !important;
        }
        .inst-card-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 3px; }
        .inst-id { font-family: var(--mono); font-size: 11px; font-weight: 500; color: var(--accent2); }
        .inst-card-selected .inst-id { color: var(--accent); }
        .inst-state-dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
        .inst-name { font-size: 12px; font-weight: 500; color: var(--text); margin-bottom: 5px; }
        .inst-badges { display: flex; gap: 4px; flex-wrap: wrap; }
        .inst-badge {
          font-family: var(--mono); font-size: 10px; padding: 1px 6px;
          border-radius: 4px; background: var(--surface); border: 1px solid var(--border);
          color: var(--text2);
        }
        .inst-card-selected .inst-badge { background: rgba(232,93,38,0.08); border-color: rgba(232,93,38,0.2); }

        /* Skeleton */
        .skeleton {
          height: 64px; border-radius: 8px; margin-bottom: 5px;
          background: linear-gradient(90deg, var(--surface) 25%, var(--border) 50%, var(--surface) 75%);
          background-size: 200%; animation: shimmer 1.4s infinite;
        }
        @keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }

        /* ── Main panel ── */
        .main { flex: 1; display: flex; flex-direction: column; overflow: hidden; min-width: 0; }

        /* Question bar */
        .qbar {
          display: flex; align-items: center; gap: 10px;
          padding: 12px 20px;
          background: var(--canvas);
          border-bottom: 1px solid var(--border);
          flex-shrink: 0;
        }
        .qbar-icon {
          width: 32px; height: 32px; border-radius: 8px;
          background: var(--surface); border: 1px solid var(--border);
          display: flex; align-items: center; justify-content: center;
          flex-shrink: 0; color: var(--muted);
        }
        .qinput {
          flex: 1; padding: 8px 14px; border-radius: 8px;
          border: 1.5px solid var(--border);
          background: var(--surface); color: var(--text);
          font-family: var(--sans); font-size: 13px;
          outline: none; transition: all 0.15s; min-width: 0;
        }
        .qinput::placeholder { color: var(--muted); }
        .qinput:focus { border-color: var(--accent2); background: var(--canvas); box-shadow: 0 0 0 3px rgba(8,145,178,0.08); }

        .btn {
          padding: 8px 18px; border-radius: 8px;
          font-family: var(--sans); font-size: 13px; font-weight: 600;
          cursor: pointer; transition: all 0.15s;
          white-space: nowrap; flex-shrink: 0; border: none;
          display: flex; align-items: center; gap: 6px;
        }
        .btn-primary {
          background: var(--accent); color: #fff;
          box-shadow: 0 2px 8px rgba(232,93,38,0.3);
        }
        .btn-primary:hover { background: #d44e1a; box-shadow: 0 4px 12px rgba(232,93,38,0.4); }
        .btn-primary:disabled { background: var(--muted); box-shadow: none; cursor: not-allowed; }
        .btn-stop {
          background: var(--red-lt); color: var(--red); border: 1.5px solid var(--red);
        }
        .btn-stop:hover { background: #fecaca; }
        .btn-clear {
          background: var(--surface); color: var(--text2);
          border: 1.5px solid var(--border);
        }
        .btn-clear:hover { background: var(--border); color: var(--text); }

        /* Output area */
        .output-wrap { flex: 1; overflow: hidden; display: flex; flex-direction: column; min-height: 0; }

        .output-toolbar {
          display: flex; align-items: center; justify-content: space-between;
          padding: 8px 20px;
          background: var(--surface);
          border-bottom: 1px solid var(--border);
          flex-shrink: 0;
        }
        .output-toolbar-left { display: flex; align-items: center; gap: 8px; }
        .output-label { font-size: 11px; font-weight: 600; color: var(--muted); letter-spacing: 0.08em; text-transform: uppercase; }
        .output-pill {
          font-family: var(--mono); font-size: 10px; padding: 2px 8px;
          border-radius: 10px; border: 1px solid var(--border);
          color: var(--text2); background: var(--canvas);
        }

        .copy-btn {
          display: flex; align-items: center; gap: 5px;
          padding: 5px 10px; border-radius: 6px;
          border: 1px solid var(--border); background: var(--canvas);
          font-size: 12px; font-weight: 500; color: var(--text2);
          cursor: pointer; transition: all 0.15s;
        }
        .copy-btn:hover { border-color: var(--accent2); color: var(--accent2); background: var(--accent2-lt); }

        .output-area {
          flex: 1; overflow-y: auto; padding: 28px 36px;
          background: var(--canvas);
        }
        .output-area::-webkit-scrollbar { width: 6px; }
        .output-area::-webkit-scrollbar-track { background: transparent; }
        .output-area::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 3px; }

        /* Empty state */
        .empty-state {
          display: flex; flex-direction: column; align-items: center;
          justify-content: center; height: 100%; gap: 12px;
        }
        .empty-icon {
          width: 64px; height: 64px; border-radius: 16px;
          background: var(--surface); border: 1px solid var(--border);
          display: flex; align-items: center; justify-content: center;
          color: var(--muted);
        }
        .empty-title { font-size: 15px; font-weight: 600; color: var(--text); }
        .empty-sub { font-size: 13px; color: var(--muted); text-align: center; max-width: 320px; line-height: 1.5; }
        .empty-chips { display: flex; gap: 6px; flex-wrap: wrap; justify-content: center; margin-top: 4px; }
        .empty-chip {
          font-size: 11px; padding: 4px 10px; border-radius: 20px;
          background: var(--surface); border: 1px solid var(--border);
          color: var(--text2); font-family: var(--mono);
        }

        /* Error */
        .error-box {
          margin: 0 0 16px; padding: 14px 16px; border-radius: 8px;
          border: 1px solid var(--red); background: var(--red-lt);
          font-size: 13px; color: var(--red); display: flex; align-items: flex-start; gap: 8px;
        }

        /* Streaming cursor */
        .cursor {
          display: inline-block; width: 2px; height: 1em;
          background: var(--accent); margin-left: 2px;
          vertical-align: text-bottom;
          animation: blink 0.9s step-end infinite;
          border-radius: 1px;
        }
        @keyframes blink { 0%,100% { opacity: 1; } 50% { opacity: 0; } }

        /* ── Markdown elements ── */
        .md-h1 {
          font-size: 19px; font-weight: 700; color: var(--text);
          margin: 28px 0 14px; padding-bottom: 10px;
          border-bottom: 2px solid var(--border);
          letter-spacing: -0.02em;
        }
        .md-h1:first-child { margin-top: 0; }
        .md-h2 {
          font-size: 15px; font-weight: 700; color: var(--text);
          margin: 22px 0 10px; letter-spacing: -0.01em;
        }
        .md-h3 {
          font-size: 12px; font-weight: 600; color: var(--accent);
          margin: 18px 0 8px; font-family: var(--mono);
          letter-spacing: 0.06em; text-transform: uppercase;
        }
        .md-h4 {
          font-size: 13px; font-weight: 600; color: var(--text2);
          margin: 14px 0 6px;
        }
        .md-p { margin: 0 0 12px; color: var(--text2); line-height: 1.7; font-size: 14px; }
        .md-strong { color: var(--text); font-weight: 600; }
        .md-em { font-style: italic; color: var(--text2); }
        .md-hr { border: none; border-top: 1px solid var(--border); margin: 24px 0; }
        .md-ul, .md-ol { padding-left: 20px; margin: 0 0 12px; }
        .md-li { margin: 5px 0; color: var(--text2); font-size: 14px; line-height: 1.6; }
        .md-blockquote {
          border-left: 3px solid var(--accent);
          margin: 14px 0; padding: 10px 16px;
          background: var(--accent-lt); border-radius: 0 6px 6px 0;
          color: var(--text2);
        }
        .md-code {
          font-family: var(--mono); font-size: 12px; padding: 2px 6px;
          border-radius: 4px; background: var(--surface);
          color: var(--accent); border: 1px solid var(--border);
        }
        .md-pre {
          background: #1c1917; border-radius: 8px; padding: 16px;
          margin: 14px 0; overflow-x: auto;
          box-shadow: var(--shadow-sm);
        }
        .md-pre code {
          font-family: var(--mono); font-size: 12px; color: #d6d3d1;
          background: none; border: none; padding: 0;
        }

        /* Tables */
        .md-table-wrap {
          overflow-x: auto; margin: 16px 0;
          border: 1px solid var(--border); border-radius: 8px;
          box-shadow: var(--shadow-sm);
        }
        .md-table { width: 100%; border-collapse: collapse; font-size: 13px; }
        .md-thead { background: var(--surface); }
        .md-th {
          padding: 10px 14px; text-align: left;
          font-size: 10px; font-weight: 600; font-family: var(--mono);
          color: var(--muted); letter-spacing: 0.1em; text-transform: uppercase;
          border-bottom: 1px solid var(--border); white-space: nowrap;
        }
        .md-td {
          padding: 9px 14px; border-bottom: 1px solid var(--border);
          color: var(--text2); font-size: 13px; vertical-align: top;
          line-height: 1.5;
        }
        .md-tr:last-child .md-td { border-bottom: none; }
        .md-tr:nth-child(even) .md-td { background: var(--surface); }
        .md-tr:hover .md-td { background: var(--accent-lt); }
      `}</style>

      <div className="app">

        {/* ── Top Nav ── */}
        <nav className="topnav">
          <div className="nav-logo">
            <div className="nav-logo-icon">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.5" strokeLinecap="round">
                <path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/>
                <polyline points="9 22 9 12 15 12 15 22"/>
              </svg>
            </div>
            <div>
              <div className="nav-logo-text">EC2 Fleet Analyser</div>
              <div className="nav-logo-sub">powered by Groq</div>
            </div>
          </div>

          <div className="nav-divider" />

          <div className="nav-stats">
            <StatChip label="Instances" value={instances.length} />
            <StatChip label="Running" value={runningCount} accent />
            <StatChip label="Window" value={`${win}d`} />
            <StatChip label="Analysing" value={analysingCount === instances.length ? "All" : analysingCount} />
          </div>

          <div className="nav-right">
            <div className="status-badge" style={{ color: sc.color, background: sc.bg }}>
              <span className={`status-dot-sm ${status === "streaming" ? "pulse" : ""}`} />
              {sc.label}
            </div>
            <button className="sidebar-toggle" onClick={() => setSidebarOpen(o => !o)} title="Toggle sidebar">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                <rect x="3" y="3" width="18" height="18" rx="2"/>
                <path d="M9 3v18"/>
              </svg>
            </button>
          </div>
        </nav>

        {/* ── Stats bar ── */}
        <div className="statsbar">
          <div className="sbar-item">
            <span className="sbar-label">Model</span>
            <span className="sbar-value">llama-3.3-70b</span>
          </div>
          <div className="sbar-item">
            <span className="sbar-label">Focus</span>
            <span className="sbar-value accent">{focus.length} / 3 selected</span>
          </div>
          <div className="sbar-item">
            <span className="sbar-label">Window</span>
            <span className="sbar-value">{win} days</span>
          </div>
          <div className="sbar-item">
            <span className="sbar-label">Scope</span>
            <span className="sbar-value green">
              {selected.length === 0 ? `All ${instances.length} instances` : `${selected.length} selected`}
            </span>
          </div>
          {output && (
            <div className="sbar-item">
              <span className="sbar-label">Output</span>
              <span className="sbar-value">{output.split(" ").length} words</span>
            </div>
          )}
          <div className="sbar-spacer" />
          <div className="sbar-item" style={{ borderRight: "none", borderLeft: "1px solid var(--border)" }}>
            <span className="sbar-label">Provider</span>
            <span className="sbar-value">Groq Cloud</span>
          </div>
        </div>

        {/* ── Body row ── */}
        <div className="body-row">

          {/* Sidebar */}
          <aside className={`sidebar ${sidebarOpen ? "" : "closed"}`}>
            <div className="sidebar-inner">

              {/* Time window */}
              <div className="sidebar-block">
                <div className="block-title">Time Window</div>
                <div className="window-grid">
                  {windowOptions.map(w => (
                    <button key={w.val}
                      className={`window-btn ${win === w.val ? "active" : ""}`}
                      onClick={() => setWin(w.val)}>
                      {w.label}
                    </button>
                  ))}
                </div>
              </div>

              {/* Focus */}
              <div className="sidebar-block">
                <div className="block-title">Analysis Focus</div>
                <div className="focus-options">
                  {focusOptions.map(f => (
                    <button key={f.id}
                      className={`focus-row ${focus.includes(f.id) ? "active" : ""}`}
                      onClick={() => toggleFocus(f.id)}>
                      <span className="focus-checkbox">
                        <svg width="10" height="8" viewBox="0 0 10 8" fill="none">
                          <path d="M1 4l3 3 5-5" stroke="#fff" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
                        </svg>
                      </span>
                      <span className="focus-text">
                        <span className="focus-name">{f.label}</span>
                        <span className="focus-desc">{f.desc}</span>
                      </span>
                    </button>
                  ))}
                </div>
              </div>

              {/* Instances */}
              <div className="inst-section">
                <div className="inst-header">
                  <div className="block-title" style={{ margin: 0 }}>Instances</div>
                  <span className="inst-count-badge">{instances.length}</span>
                </div>
                <button
                  className={`inst-all-row ${selected.length === 0 ? "active" : ""}`}
                  onClick={() => setSelected([])}>
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                    <rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/>
                  </svg>
                  All Instances
                </button>
                <div className="inst-scroll">
                  {loadingInst
                    ? [1, 2, 3, 4].map(i => <div key={i} className="skeleton" />)
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
              </div>

            </div>
          </aside>

          {/* Main */}
          <main className="main">

            {/* Question bar */}
            <div className="qbar">
              <div className="qbar-icon">
                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                  <circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/>
                </svg>
              </div>
              <input
                className="qinput"
                placeholder="Ask a specific question, e.g. 'Which instances can be downsized this week?'"
                value={question}
                onChange={e => setQuestion(e.target.value)}
                onKeyDown={e => e.key === "Enter" && !streaming && handleAnalyse()}
              />
              {streaming
                ? <button className="btn btn-stop" onClick={handleStop}>
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg>
                    Stop
                  </button>
                : <button className="btn btn-primary" disabled={focus.length === 0} onClick={handleAnalyse}>
                    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                      <polygon points="5 3 19 12 5 21 5 3"/>
                    </svg>
                    Analyse
                  </button>
              }
              {output && !streaming &&
                <button className="btn btn-clear"
                  onClick={() => { setOutput(""); setStatus("idle"); }}>
                  Clear
                </button>
              }
            </div>

            {/* Output */}
            <div className="output-wrap">
              {output && (
                <div className="output-toolbar">
                  <div className="output-toolbar-left">
                    <span className="output-label">Analysis Report</span>
                    <span className="output-pill">{win}d window</span>
                    {selected.length > 0 && (
                      <span className="output-pill">{selected.length} instance{selected.length > 1 ? "s" : ""}</span>
                    )}
                    {focus.map(f => (
                      <span key={f} className="output-pill">{f.replace("_", " ")}</span>
                    ))}
                  </div>
                  {!streaming && <CopyButton text={output} />}
                </div>
              )}

              <div className="output-area" ref={outputRef}>
                {error && (
                  <div className="error-box">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" style={{ flexShrink: 0, marginTop: 1 }}>
                      <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
                    </svg>
                    {error}
                  </div>
                )}

                {!output && !error && (
                  <div className="empty-state">
                    <div className="empty-icon">
                      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
                        <path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/>
                        <polyline points="3.27 6.96 12 12.01 20.73 6.96"/>
                        <line x1="12" y1="22.08" x2="12" y2="12"/>
                      </svg>
                    </div>
                    <div className="empty-title">EC2 Fleet Analyser</div>
                    <div className="empty-sub">
                      Configure your analysis using the sidebar, then click Analyse to generate AI-powered rightsizing recommendations.
                    </div>
                    <div className="empty-chips">
                      <span className="empty-chip">{instances.length} instances loaded</span>
                      <span className="empty-chip">{win}d window</span>
                      <span className="empty-chip">{focus.length} focus areas</span>
                    </div>
                  </div>
                )}

                {output && (
                  <div>
                    <ReactMarkdown remarkPlugins={[remarkGfm]} components={MD_COMPONENTS}>
                      {output}
                    </ReactMarkdown>
                    {streaming && <span className="cursor" />}
                  </div>
                )}
              </div>
            </div>

          </main>
        </div>
      </div>
    </>
  );
}