#!/usr/bin/env python3
"""Pingbot - background internet/LAN connectivity monitor with a local web dashboard.

Pings the default gateway (LAN check) and a public host (internet check) on a
fixed interval, classifies each sample, and serves a self-contained dashboard at
http://localhost:<port>/ showing a live graph, health summaries and an outage log.

Stdlib only. The dashboard draws its own charts on a <canvas>, so it keeps working
even while the internet (and any CDN) is down.
"""

import argparse
import json
import os
import platform
import re
import subprocess
import threading
import time
import webbrowser
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

IS_WIN = platform.system().lower().startswith("win")

# ---- Defaults (overridable via CLI) -------------------------------------------------
DEFAULT_PORT = 8787
DEFAULT_INTERVAL = 5.0          # seconds between samples
GATEWAY_TIMEOUT_MS = 1000
INTERNET_TIMEOUT_MS = 1500
PUBLIC_HOSTS = ["1.1.1.1", "8.8.8.8"]   # tried in order; first reply wins
KEEP_SECONDS = 6 * 3600         # how much history to retain in memory
GRAPH_SECONDS = 95 * 60         # how much history to ship to the dashboard

# ---- Shared state -------------------------------------------------------------------
_lock = threading.Lock()
_samples = deque(maxlen=int(KEEP_SECONDS / 1) + 10)  # resized once interval is known
_annotations = {}  # keyed by event start time (float): {"text": str, "timestamp": float}
ANNOTATIONS_FILE = "pingbot_annotations.json"
LOG_FILE = "pingbot_log.json"
LOG_SAVE_INTERVAL = 5 * 60  # save every 5 minutes
START_TIME = time.time()
GATEWAY_IP = None
INTERVAL = DEFAULT_INTERVAL


def load_annotations():
    """Load annotations from file."""
    global _annotations
    if os.path.exists(ANNOTATIONS_FILE):
        try:
            with open(ANNOTATIONS_FILE, 'r') as f:
                _annotations = json.load(f)
        except Exception as e:
            print(f"Warning: could not load annotations: {e}")
            _annotations = {}


def save_annotations():
    """Save annotations to file."""
    try:
        with open(ANNOTATIONS_FILE, 'w') as f:
            json.dump(_annotations, f, indent=2)
    except Exception as e:
        print(f"Warning: could not save annotations: {e}")


def get_annotation(event_start_time):
    """Get annotation for an event by start time."""
    key = str(event_start_time)
    return _annotations.get(key)


def save_log():
    """Save the last 6 hours of samples to disk."""
    try:
        with _lock:
            samples_to_save = [
                [r[0], r[1], r[2], r[3], r[4]]
                for r in _samples
            ]
        with open(LOG_FILE, 'w') as f:
            json.dump(samples_to_save, f)
    except Exception as e:
        print(f"Warning: could not save log: {e}")


def load_log():
    """Load samples from disk and populate the samples deque."""
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, 'r') as f:
                data = json.load(f)
            with _lock:
                for item in data:
                    _samples.append(tuple(item))
            print(f"Loaded {len(_samples)} historical samples from log")
        except Exception as e:
            print(f"Warning: could not load log: {e}")


def log_saver_thread():
    """Periodically save the log."""
    while True:
        time.sleep(LOG_SAVE_INTERVAL)
        save_log()


def ping(host, timeout_ms):
    """Return (ok, latency_ms). ok is True only on a genuine echo reply."""
    if IS_WIN:
        cmd = ["ping", "-n", "1", "-w", str(timeout_ms), host]
    else:
        cmd = ["ping", "-c", "1", "-W", str(max(1, timeout_ms // 1000)), host]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=timeout_ms / 1000 + 2,
        )
    except Exception:
        return False, None
    text = (proc.stdout or "") + (proc.stderr or "")
    if IS_WIN:
        # Windows ping can exit 0 while printing "Destination host unreachable",
        # so require an actual TTL in the reply.
        ok = proc.returncode == 0 and "TTL=" in text
    else:
        ok = proc.returncode == 0 and ("ttl=" in text.lower() or "time=" in text.lower())
    m = re.search(r"time[=<]\s*([0-9]+(?:\.[0-9]+)?)\s*ms", text, re.IGNORECASE)
    latency = float(m.group(1)) if m else None
    return (ok, latency if ok else None)


def get_default_gateway():
    """Best-effort default gateway IPv4 detection."""
    if IS_WIN:
        try:
            out = subprocess.run(["route", "print", "-4"],
                                 capture_output=True, text=True, timeout=5).stdout
            for line in out.splitlines():
                parts = line.split()
                if len(parts) >= 3 and parts[0] == "0.0.0.0" and parts[1] == "0.0.0.0":
                    gw = parts[2]
                    if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", gw) and gw != "0.0.0.0":
                        return gw
        except Exception:
            pass
        try:
            out = subprocess.run(["ipconfig"], capture_output=True, text=True, timeout=5).stdout
            for line in out.splitlines():
                if "Default Gateway" in line:
                    m = re.search(r"(\d+\.\d+\.\d+\.\d+)", line)
                    if m and m.group(1) != "0.0.0.0":
                        return m.group(1)
        except Exception:
            pass
    else:
        try:
            out = subprocess.run(["ip", "route"], capture_output=True, text=True, timeout=5).stdout
            m = re.search(r"default via (\d+\.\d+\.\d+\.\d+)", out)
            if m:
                return m.group(1)
        except Exception:
            pass
        try:
            out = subprocess.run(["netstat", "-rn"], capture_output=True, text=True, timeout=5).stdout
            for line in out.splitlines():
                parts = line.split()
                if parts and parts[0] in ("default", "0.0.0.0") and len(parts) >= 2:
                    if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", parts[1]):
                        return parts[1]
        except Exception:
            pass
    return None


def monitor_loop():
    global GATEWAY_IP
    while True:
        cycle_start = time.time()

        if not GATEWAY_IP:
            GATEWAY_IP = get_default_gateway()

        g_ok, g_lat = (False, None)
        if GATEWAY_IP:
            g_ok, g_lat = ping(GATEWAY_IP, GATEWAY_TIMEOUT_MS)

        i_ok, i_lat = (False, None)
        for host in PUBLIC_HOSTS:
            i_ok, i_lat = ping(host, INTERNET_TIMEOUT_MS)
            if i_ok:
                break

        # If the gateway stopped responding, it may have changed (reconnect / new DHCP).
        if not g_ok:
            new_gw = get_default_gateway()
            if new_gw and new_gw != GATEWAY_IP:
                GATEWAY_IP = new_gw

        with _lock:
            _samples.append((time.time(), 1 if g_ok else 0, 1 if i_ok else 0, g_lat, i_lat))

        time.sleep(max(0.2, INTERVAL - (time.time() - cycle_start)))


# ---- Analysis -----------------------------------------------------------------------
def _lan_ok(row):
    # If the public internet is reachable the LAN path is fine, even when the
    # router itself declines to answer pings. LAN is only "down" when both fail.
    return row[1] == 1 or row[2] == 1


def summarize(rows):
    n = len(rows)
    if n == 0:
        return {
            "samples": 0, "internet_uptime": None, "lan_uptime": None,
            "internet_outages": 0, "lan_outages": 0,
            "internet_longest_sec": 0, "lan_longest_sec": 0,
            "internet_downtime_sec": 0, "lan_downtime_sec": 0,
        }
    internet_up = sum(r[2] for r in rows)
    lan_up = sum(1 for r in rows if _lan_ok(r))

    def runs(is_down):
        count = longest = cur = down = 0
        for r in rows:
            if is_down(r):
                cur += 1
                down += 1
                if cur == 1:
                    count += 1
                longest = max(longest, cur)
            else:
                cur = 0
        return count, longest, down

    i_count, i_long, i_down = runs(lambda r: r[2] == 0)
    l_count, l_long, l_down = runs(lambda r: not _lan_ok(r))
    return {
        "samples": n,
        "internet_uptime": internet_up / n * 100,
        "lan_uptime": lan_up / n * 100,
        "internet_outages": i_count,
        "lan_outages": l_count,
        "internet_longest_sec": i_long * INTERVAL,
        "lan_longest_sec": l_long * INTERVAL,
        "internet_downtime_sec": i_down * INTERVAL,
        "lan_downtime_sec": l_down * INTERVAL,
    }


def sample_state(row):
    g, i = row[1], row[2]
    if i == 1 and g == 1:
        return "ok"
    if i == 1 and g == 0:
        return "gw"          # internet works but router won't answer pings
    if i == 0 and g == 1:
        return "internet"    # LAN fine, ISP/internet down
    return "lan"             # nothing reachable -> local network problem


def build_events(rows):
    """Collapse consecutive non-ok samples into outage events (most recent first)."""
    events = []
    cur = None
    for r in rows:
        st = sample_state(r)
        if st == "ok":
            if cur:
                cur["end"] = r[0]
                events.append(cur)
                cur = None
            continue
        if cur and cur["type"] == st:
            cur["end"] = r[0]
        else:
            if cur:
                events.append(cur)
            cur = {"type": st, "start": r[0], "end": r[0]}
    if cur:
        cur["end"] = None  # ongoing
        events.append(cur)
    events.reverse()
    
    # Attach annotations
    for event in events:
        annotation = get_annotation(event["start"])
        if annotation:
            event["annotation"] = annotation
    
    return events[:50]


def build_state_payload():
    now = time.time()
    with _lock:
        all_rows = list(_samples)
    graph_rows = [r for r in all_rows if r[0] >= now - GRAPH_SECONDS]

    def window(minutes):
        cutoff = now - minutes * 60
        return summarize([r for r in all_rows if r[0] >= cutoff])

    current = None
    if all_rows:
        last = all_rows[-1]
        current = {
            "t": last[0], "gateway_ok": bool(last[1]), "internet_ok": bool(last[2]),
            "gateway_latency": last[3], "internet_latency": last[4],
            "state": sample_state(last),
        }

    return {
        "now": now,
        "start_time": START_TIME,
        "interval": INTERVAL,
        "gateway_ip": GATEWAY_IP,
        "public_hosts": PUBLIC_HOSTS,
        "current": current,
        "summary": {
            "since_start": summarize(all_rows),
            "15": window(15), "30": window(30), "60": window(60), "90": window(90),
        },
        "events": build_events(graph_rows),
        # compact: [t, lan_ok, internet_ok, gateway_latency, internet_latency]
        "samples": [[round(r[0], 1), r[1], r[2], r[3], r[4]] for r in graph_rows],
    }


# ---- HTTP server --------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass  # quiet

    def _send(self, code, body, ctype):
        data = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/api/state"):
            self._send(200, json.dumps(build_state_payload()), "application/json")
        elif self.path.startswith("/api/annotations"):
            with _lock:
                self._send(200, json.dumps(_annotations), "application/json")
        elif self.path == "/" or self.path.startswith("/index"):
            self._send(200, INDEX_HTML, "text/html; charset=utf-8")
        else:
            self._send(404, "not found", "text/plain")

    def do_POST(self):
        if self.path == "/api/annotate":
            content_len = int(self.headers.get("Content-Length", 0))
            try:
                body = self.rfile.read(content_len).decode("utf-8")
                data = json.loads(body)
                event_start = data.get("event_start")
                text = data.get("text", "").strip()
                
                if event_start is None:
                    self._send(400, json.dumps({"error": "Missing event_start"}), "application/json")
                    return
                
                with _lock:
                    key = str(event_start)
                    if text:
                        _annotations[key] = {
                            "text": text,
                            "timestamp": time.time()
                        }
                    elif key in _annotations:
                        del _annotations[key]
                    save_annotations()
                
                self._send(200, json.dumps({"success": True}), "application/json")
            except Exception as e:
                self._send(400, json.dumps({"error": str(e)}), "application/json")
        else:
            self._send(404, "not found", "text/plain")


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pingbot - connectivity monitor</title>
<style>
  :root{
    --bg:#0f1419; --panel:#1a2129; --panel2:#222c36; --line:#2c3742;
    --text:#e6edf3; --muted:#8b98a5;
    --ok:#16a34a; --gw:#f59e0b; --internet:#dc2626; --lan:#7f1d1d; --nodata:#39424d;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);
       font:14px/1.45 system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
  header{padding:16px 20px;border-bottom:1px solid var(--line);
         display:flex;align-items:center;gap:18px;flex-wrap:wrap}
  h1{font-size:18px;margin:0;font-weight:600;letter-spacing:.3px}
  .badge{padding:8px 16px;border-radius:8px;font-weight:700;font-size:15px;
         display:flex;align-items:center;gap:9px}
  .dot{width:11px;height:11px;border-radius:50%;display:inline-block}
  .meta{color:var(--muted);font-size:12.5px;display:flex;gap:18px;flex-wrap:wrap}
  .meta b{color:var(--text);font-weight:600}
  main{padding:18px 20px;display:grid;gap:18px;max-width:1200px}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px}
  .panel h2{margin:0 0 12px;font-size:13px;text-transform:uppercase;
            letter-spacing:.6px;color:var(--muted);font-weight:600}
  .chartbar{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
  .wins button{background:var(--panel2);color:var(--text);border:1px solid var(--line);
               padding:5px 12px;border-radius:6px;cursor:pointer;font-size:12.5px;margin-left:6px}
  .wins button.active{background:#2563eb;border-color:#2563eb;color:#fff}
  canvas{width:100%;display:block;border-radius:6px}
  .legend{display:flex;gap:16px;flex-wrap:wrap;margin-top:10px;font-size:12px;color:var(--muted)}
  .legend span{display:flex;align-items:center;gap:6px}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{text-align:right;padding:8px 10px;border-bottom:1px solid var(--line)}
  th:first-child,td:first-child{text-align:left}
  thead th{color:var(--muted);font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:.4px}
  td.good{color:#4ade80}td.warn{color:#fbbf24}td.bad{color:#f87171}
  .events{display:flex;flex-direction:column;gap:6px;max-height:280px;overflow:auto}
  .evt{display:flex;align-items:center;gap:10px;padding:7px 10px;background:var(--panel2);
       border-radius:6px;font-size:13px}
  .evt .tag{font-weight:700;padding:2px 8px;border-radius:4px;font-size:11px;color:#fff}
  .evt .when{color:var(--muted);font-size:12px;margin-left:auto;white-space:nowrap}
  .evt-btn{background:transparent;border:none;color:var(--muted);cursor:pointer;font-size:16px;padding:0 4px;margin-left:4px}
  .evt-btn:hover{color:var(--text)}
  .annot{color:var(--muted);font-size:12px;padding:6px 20px;font-style:italic;border-left:3px solid var(--line)}
  .empty{color:var(--muted);font-style:italic;padding:8px 2px}
  .grid2{display:grid;grid-template-columns:1.4fr 1fr;gap:18px}
  @media(max-width:860px){.grid2{grid-template-columns:1fr}}
</style>
</head>
<body>
<header>
  <h1>Pingbot</h1>
  <div id="badge" class="badge" style="background:var(--nodata)">
    <span class="dot" style="background:#aaa"></span><span id="badgeText">Starting…</span>
  </div>
  <div class="meta">
    <span>Gateway: <b id="gwip">—</b></span>
    <span>Internet ping: <b id="ilat">—</b></span>
    <span>LAN ping: <b id="glat">—</b></span>
    <span>Monitoring for <b id="uptime">—</b></span>
    <span>Updated <b id="updated">—</b></span>
  </div>
</header>

<main>
  <div class="panel">
    <div class="chartbar">
      <h2 style="margin:0">Live timeline</h2>
      <div class="wins" id="wins">
        <button data-w="15">15m</button>
        <button data-w="30" class="active">30m</button>
        <button data-w="60">60m</button>
        <button data-w="90">90m</button>
      </div>
    </div>
    <canvas id="chart" height="260"></canvas>
    <div class="legend">
      <span><span class="dot" style="background:var(--ok)"></span>OK</span>
      <span><span class="dot" style="background:var(--internet)"></span>Internet down (ISP)</span>
      <span><span class="dot" style="background:var(--lan)"></span>LAN down (local network)</span>
      <span><span class="dot" style="background:var(--gw)"></span>Router not answering (internet OK)</span>
      <span><span class="dot" style="background:var(--nodata)"></span>No data</span>
      <span>— line = internet latency (ms)</span>
    </div>
  </div>

  <div class="grid2">
    <div class="panel">
      <h2>Health summary</h2>
      <table>
        <thead><tr>
          <th>Window</th><th>Internet up</th><th>LAN up</th>
          <th>Net outages</th><th>LAN outages</th><th>Worst net</th><th>Worst LAN</th>
        </tr></thead>
        <tbody id="summaryBody"></tbody>
      </table>
    </div>
    <div class="panel">
      <h2>Recent outages (last 90m)</h2>
      <div class="events" id="events"><div class="empty">No outages recorded.</div></div>
    </div>
  </div>
</main>

<script>
const COLORS={ok:"#16a34a",gw:"#f59e0b",internet:"#dc2626",lan:"#7f1d1d",nodata:"#39424d"};
const LABELS={ok:"OK",gw:"Router not answering",internet:"Internet down (ISP)",lan:"LAN down"};
let windowMin=30, latest=null;

document.querySelectorAll('#wins button').forEach(b=>{
  b.onclick=()=>{
    document.querySelectorAll('#wins button').forEach(x=>x.classList.remove('active'));
    b.classList.add('active'); windowMin=+b.dataset.w; if(latest) render(latest);
  };
});

function fmtDur(s){
  s=Math.round(s); if(s<60) return s+"s";
  const m=Math.floor(s/60), r=s%60;
  if(m<60) return m+"m "+String(r).padStart(2,"0")+"s";
  const h=Math.floor(m/60); return h+"h "+String(m%60).padStart(2,"0")+"m";
}
function fmtClock(t){const d=new Date(t*1000);return d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'});}
function pct(v){return v==null?"—":v.toFixed(v>=99.95?2:1)+"%";}
function stateOf(g,i){if(i&&g)return"ok";if(i&&!g)return"gw";if(!i&&g)return"internet";return"lan";}

async function poll(){
  try{
    const r=await fetch('/api/state',{cache:'no-store'});
    latest=await r.json(); render(latest);
  }catch(e){
    document.getElementById('badgeText').textContent="Dashboard can't reach monitor";
  }
}

function render(d){
  const c=d.current;
  const badge=document.getElementById('badge'), bt=document.getElementById('badgeText');
  if(c){
    const st=c.state, col=COLORS[st];
    badge.style.background=col;
    badge.querySelector('.dot').style.background="#fff";
    bt.textContent = st==="ok"?"All systems OK":LABELS[st];
    document.getElementById('ilat').textContent=c.internet_ok?(c.internet_latency!=null?c.internet_latency+" ms":"reply"):"no reply";
    document.getElementById('glat').textContent=c.gateway_ok?(c.gateway_latency!=null?c.gateway_latency+" ms":"reply"):"no reply";
  }
  document.getElementById('gwip').textContent=d.gateway_ip||"unknown";
  document.getElementById('uptime').textContent=fmtDur(d.now-d.start_time);
  document.getElementById('updated').textContent=c?fmtClock(c.t):"—";

  renderSummary(d.summary);
  renderEvents(d.events,d.now);
  drawChart(d);
}

function cell(v,type){
  let cls="";
  if(type==="up"&&v!=null){cls=v>=99.9?"good":v>=98?"warn":"bad";}
  if(type==="out"){cls=v>0?"bad":"good";}
  return `<td class="${cls}">`;
}
function renderSummary(s){
  const rows=[["Since start","since_start"],["Last 15m","15"],["Last 30m","30"],["Last 60m","60"],["Last 90m","90"]];
  let html="";
  for(const [label,key] of rows){
    const w=s[key];
    html+="<tr><td>"+label+"</td>"+
      cell(w.internet_uptime,"up")+pct(w.internet_uptime)+"</td>"+
      cell(w.lan_uptime,"up")+pct(w.lan_uptime)+"</td>"+
      cell(w.internet_outages,"out")+w.internet_outages+"</td>"+
      cell(w.lan_outages,"out")+w.lan_outages+"</td>"+
      "<td>"+(w.internet_longest_sec?fmtDur(w.internet_longest_sec):"—")+"</td>"+
      "<td>"+(w.lan_longest_sec?fmtDur(w.lan_longest_sec):"—")+"</td></tr>";
  }
  document.getElementById('summaryBody').innerHTML=html;
}

function renderEvents(events,now){
  const el=document.getElementById('events');
  if(!events||!events.length){el.innerHTML='<div class="empty">No outages recorded.</div>';return;}
  let html="";
  for(const e of events){
    const ongoing=e.end==null;
    const end=ongoing?now:e.end;
    const dur=fmtDur(end-e.start);
    const col=COLORS[e.type]||"#666";
    const annot=e.annotation?.text||"";
    const annotText=annot?`<div class="annot">${escapeHtml(annot)}</div>`:"";
    html+=`<div class="evt" data-start="${e.start}"><span class="tag" style="background:${col}">${LABELS[e.type]||e.type}</span>`+
          `<span>${fmtClock(e.start)} → ${ongoing?'<b>ongoing</b>':fmtClock(e.end)} &nbsp;(${dur})</span>`+
          `<span class="when">${ongoing?'now':timeAgo(now-e.start)+' ago'}</span>`+
          `<button class="evt-btn" onclick="openAnnotateDialog(${e.start})">📝</button>`+
          `</div>${annotText}`;
  }
  el.innerHTML=html;
}
function timeAgo(s){return fmtDur(s);}
function escapeHtml(txt){
  const el=document.createElement('div');el.textContent=txt;return el.innerHTML;
}

function openAnnotateDialog(eventStart){
  const existingAnnot=document.querySelector(`.evt[data-start="${eventStart}"]`);
  const currentText=existingAnnot?.nextElementSibling?.innerText||"";
  const newText=prompt("Annotation for this outage:",currentText);
  if(newText!==null){
    fetch('/api/annotate',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({event_start:eventStart,text:newText})
    }).then(()=>{poll();}).catch(e=>alert("Error saving annotation: "+e));
  }
}

function drawChart(d){
  const canvas=document.getElementById('chart');
  const dpr=window.devicePixelRatio||1;
  const cssW=canvas.clientWidth, cssH=260;
  canvas.width=cssW*dpr; canvas.height=cssH*dpr;
  const ctx=canvas.getContext('2d'); ctx.setTransform(dpr,0,0,dpr,0,0);
  ctx.clearRect(0,0,cssW,cssH);

  const now=d.now, span=windowMin*60, t0=now-span;
  const padL=46,padR=12,padT=14,padB=22,stripH=26,gap=8;
  const plotX=padL, plotW=cssW-padL-padR;
  const stripY=padT, plotY=stripY+stripH+gap, plotH=cssH-plotY-padB;
  const xOf=t=>plotX+((t-t0)/span)*plotW;

  const rows=d.samples.filter(r=>r[0]>=t0);

  // status strip: width-per-sample based on interval so gaps (no data) show through
  const colW=Math.max(2,(plotW/span)*d.interval);
  ctx.fillStyle=COLORS.nodata; ctx.fillRect(plotX,stripY,plotW,stripH);
  for(const r of rows){
    const st=stateOf(r[1],r[2]);
    ctx.fillStyle=COLORS[st];
    const x=xOf(r[0]);
    ctx.fillRect(x-colW/2,stripY,colW+0.6,stripH);
  }
  ctx.strokeStyle="#0f1419"; ctx.lineWidth=1; ctx.strokeRect(plotX+.5,stripY+.5,plotW-1,stripH-1);

  // latency scale
  let maxLat=50;
  for(const r of rows){if(r[4]!=null&&r[4]>maxLat)maxLat=r[4];}
  maxLat=Math.ceil(maxLat/25)*25;
  const yOf=v=>plotY+plotH-(v/maxLat)*plotH;

  // gridlines + y labels
  ctx.font="11px system-ui"; ctx.textBaseline="middle";
  ctx.strokeStyle="#2c3742"; ctx.fillStyle="#8b98a5"; ctx.lineWidth=1;
  for(let i=0;i<=4;i++){
    const v=maxLat*i/4, y=yOf(v);
    ctx.beginPath();ctx.moveTo(plotX,y);ctx.lineTo(plotX+plotW,y);ctx.stroke();
    ctx.textAlign="right"; ctx.fillText(Math.round(v)+"ms",plotX-6,y);
  }
  // x time labels
  ctx.textAlign="center"; ctx.textBaseline="top";
  const ticks=6;
  for(let i=0;i<=ticks;i++){
    const t=t0+span*i/ticks, x=xOf(t);
    ctx.fillText(fmtClock(t),Math.min(Math.max(x,plotX+14),plotX+plotW-14),plotY+plotH+6);
  }

  // internet latency line (break across gaps and down samples)
  ctx.strokeStyle="#38bdf8"; ctx.lineWidth=1.6; ctx.beginPath();
  let started=false, prevT=null;
  for(const r of rows){
    const gapped = prevT!=null && (r[0]-prevT) > d.interval*2.2;
    if(r[2]===1 && r[4]!=null && !gapped){
      const x=xOf(r[0]), y=yOf(Math.min(r[4],maxLat));
      if(started)ctx.lineTo(x,y); else {ctx.moveTo(x,y);started=true;}
    } else {started=false;}
    prevT=r[0];
  }
  ctx.stroke();
}

window.addEventListener('resize',()=>{if(latest)drawChart(latest);});
poll(); setInterval(poll,3000);
</script>
</body>
</html>
"""


def main():
    global INTERVAL, _samples, PUBLIC_HOSTS
    parser = argparse.ArgumentParser(description="Background internet/LAN connectivity monitor.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL,
                        help="seconds between connectivity samples")
    parser.add_argument("--hosts", default=",".join(PUBLIC_HOSTS),
                        help="comma-separated public hosts for the internet check")
    parser.add_argument("--no-open", action="store_true", help="don't open the browser on start")
    args = parser.parse_args()

    INTERVAL = max(1.0, args.interval)
    PUBLIC_HOSTS = [h.strip() for h in args.hosts.split(",") if h.strip()]
    _samples = deque(maxlen=int(KEEP_SECONDS / INTERVAL) + 10)
    
    load_annotations()
    load_log()

    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=log_saver_thread, daemon=True).start()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://localhost:{args.port}/"
    print(f"Pingbot monitoring every {INTERVAL:g}s — internet hosts: {', '.join(PUBLIC_HOSTS)}")
    print(f"Dashboard: {url}  (Ctrl+C to stop)")
    if not args.no_open:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping.")
        save_log()
        server.shutdown()


if __name__ == "__main__":
    main()
