/* Shared-scan web console.
 *
 * Vanilla JS, no build step. Talks to the stdlib HTTP server and opens one
 * EventSource (SSE) per query so each subscriber's records stream live.
 */
"use strict";

const $ = (id) => document.getElementById(id);
const esc = (s) =>
  String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const fmtBytes = (n) => {
  if (n == null) return "–";
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB"];
  let v = n, u = -1;
  do { v /= 1024; u++; } while (v >= 1024 && u < units.length - 1);
  return `${v.toFixed(v < 10 ? 2 : v < 100 ? 1 : 0)} ${units[u]}`;
};
const fmtTime = (ts) => new Date(ts * 1000).toLocaleTimeString("zh-CN",
  { hour12: false }) + "." + String(Math.floor((ts * 1000) % 1000)).padStart(3, "0");

async function api(path, payload) {
  const opt = payload === undefined
    ? {}
    : { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload) };
  const resp = await fetch(path, opt);
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(data.error || `${resp.status} ${resp.statusText}`);
  return data;
}

/* ============================================================ global state */
const state = {
  files: [],
  // subId -> live card controller
  cards: new Map(),
  lastEventTs: 0,
  pollFailures: 0,
};

/* ============================================================ filter form */
function readFilter() {
  const kind = $("filter-kind").value;
  if (kind === "all") return { kind: "all" };
  if (kind === "contains" || kind === "regex")
    return { kind, pattern: $("filter-pattern").value };
  // json
  const raw = $("json-value").value;
  let value = raw;
  if (raw === "true") value = true;
  else if (raw === "false") value = false;
  else if (raw !== "" && !Number.isNaN(Number(raw))) value = Number(raw);
  return { kind: "json", key: $("json-key").value,
           op: $("json-op").value, value };
}

$("filter-kind").addEventListener("change", () => {
  const kind = $("filter-kind").value;
  $("wrap-pattern").classList.toggle("hidden", !(kind === "contains" || kind === "regex"));
  $("wrap-json").classList.toggle("hidden", kind !== "json");
});

/* ============================================================ files */
async function refreshFiles() {
  const { files } = await api("/api/files");
  state.files = files;
  renderFileList(files);
  renderFileSelect(files);
}

function renderFileList(files) {
  const el = $("file-list");
  if (!files.length) { el.innerHTML = '<p class="empty">尚无文件，先生成一个。</p>'; return; }
  el.innerHTML = files.map((f) => {
    const cur = f.versions.find((v) => v.version_id === f.current) || f.versions[0];
    const chips = f.versions.slice().reverse().map((v) =>
      `<span class="ver-chip ${v.version_id === f.current ? "current" : ""}"
              title="点击按此历史版本查询" data-vid="${esc(v.version_id)}" data-file="${esc(f.name)}">
        v${v.seq} · ${esc(v.version_id.slice(0, 10))}</span>`).join("");
    return `<div class="file-item">
      <div class="fname">${esc(f.name)}</div>
      <div class="fmeta">当前 v${cur.seq} · ${fmtBytes(cur.size)} · ${f.versions.length} 个版本
        · hash ${esc(cur.version_id.slice(0, 18))}…</div>
      <div>${chips}</div>
    </div>`;
  }).join("");
  el.querySelectorAll(".ver-chip").forEach((chip) =>
    chip.addEventListener("click", () => pinVersion(chip.dataset.file, chip.dataset.vid)));
}

let pinnedVersion = null; // {file, versionId}
function pinVersion(file, vid) {
  pinnedVersion = { file, versionId: vid };
  $("query-file").value = file;
  flashCrashNote(`已固定历史版本 ${vid.slice(0, 10)}…，新查询将读取该不可变内容而非最新版。再次点击当前版本可取消。`);
}

function renderFileSelect(files) {
  const sel = $("query-file");
  const prev = sel.value;
  sel.innerHTML = files.map((f) => `<option>${esc(f.name)}</option>`).join("");
  if (files.some((f) => f.name === prev)) sel.value = prev;
}

$("btn-generate").addEventListener("click", async () => {
  try {
    const name = $("file-name").value.trim();
    const r = await api("/api/files/generate", {
      name, lines: Number($("gen-lines").value), seed: Number($("gen-seed").value),
    });
    flashCrashNote(`已生成 ${name}，${fmtBytes(r.generated_bytes)}（内容寻址，相同参数不会产生新版本）。`);
    await refreshFiles();
  } catch (e) { alert(e.message); }
});

$("btn-upload").addEventListener("click", async () => {
  try {
    const name = $("file-name").value.trim();
    const content = $("upload-content").value;
    if (!content) return alert("内容为空");
    await api("/api/files", { name, content });
    flashCrashNote(`已上传 ${name} 的新版本——新查询不会接到任何旧扫描。`);
    $("upload-content").value = "";
    await refreshFiles();
  } catch (e) { alert(e.message); }
});

/* ============================================================ queries / SSE */
async function startQuery(opts = {}) {
  const fileName = $("query-file").value;
  if (!fileName) return alert("请先生成或选择一个文件");
  const body = {
    file: fileName,
    filter: readFilter(),
    label: opts.label || `q${state.cards.size + 1}`,
    max_buffer_items: Number($("buf-items").value),
    offer_timeout: Number($("offer-timeout").value),
    slow: $("born-slow").checked || opts.slow || false,
  };
  if (pinnedVersion && pinnedVersion.file === fileName)
    body.version_id = pinnedVersion.version_id;
  const q = await api("/api/query", body);
  openCard(q, body.filter);
  return q;
}

$("btn-query").addEventListener("click", () =>
  startQuery().catch((e) => alert(e.message)));
$("btn-query-twice").addEventListener("click", async () => {
  try {
    // Back-to-back: the second query must attach to the first's in-flight scan.
    const q1 = await startQuery({ label: "shared-A" });
    const q2 = await startQuery({ label: "shared-B" });
    if (q1.session_id === q2.session_id)
      flashCrashNote("两个查询共用同一会话 —— 第二个为共享附加，物理扫描仍只读一次。");
  } catch (e) { alert(e.message); }
});

function openCard(q, filterSpec) {
  const subId = q.subscriber_id;
  if (state.cards.has(subId)) return;
  const wrap = document.createElement("div");
  wrap.className = "sub-card running";
  wrap.id = `card-${subId}`;
  wrap.innerHTML = `
    <div class="sub-head">
      <div><span class="sub-title">${esc(q.label || subId)}</span>
        <span class="sub-id">${esc(subId)}</span>
        <span class="mode-tag">${esc(q.shared_mode)}</span></div>
      <span class="badge running" data-role="state">running</span>
    </div>
    <div class="sub-meta">
      <span>会话 <b data-role="session">${esc(q.session_id)}</b></span>
      <span>命中 <b data-role="matched">0</b></span>
      <span>已投递 <b data-role="delivered">0</b></span>
      <span>缓冲 <b data-role="buf">0/0</b></span>
      <span data-role="slowflag" style="display:none;color:var(--warn)">🐢 慢</span>
    </div>
    <div class="bufbar" data-role="bufbar"><i style="width:0%"></i></div>
    <div class="sub-actions">
      <button class="btn small" data-act="slow">调慢 / 恢复</button>
      <button class="btn small" data-act="cancel">取消</button>
      <button class="btn small" data-act="resume" disabled>恢复</button>
      <button class="btn small" data-act="compare">与独立查询核对</button>
      <button class="btn small" data-act="clear">清空记录</button>
    </div>
    <div data-role="compare" class="compare pending">尚未核对</div>
    <div class="records" data-role="records">
      <div class="rec-head">实时 SSE 记录（行号 · 分片 · 内容）</div>
    </div>`;
  $("subscribers").querySelector(".empty")?.remove();
  $("subscribers").prepend(wrap);

  const card = {
    subId, sessionId: q.session_id, filterSpec,
    el: wrap, records: [], lineNos: new Set(),
    source: null, slow: !!q.slow, compareDone: false,
  };
  state.cards.set(subId, card);

  wrap.querySelector('[data-act="slow"]').onclick = () => toggleSlow(card);
  wrap.querySelector('[data-act="cancel"]').onclick = () => cancel(card);
  wrap.querySelector('[data-act="resume"]').onclick = () => resume(card);
  wrap.querySelector('[data-act="compare"]').onclick = () => compare(card);
  wrap.querySelector('[data-act="clear"]').onclick = () => {
    card.records = []; card.lineNos.clear();
    wrap.querySelector('[data-role="records"]').innerHTML =
      '<div class="rec-head">实时 SSE 记录（行号 · 分片 · 内容）</div>';
  };

  connectSSE(card);
}

function connectSSE(card) {
  // EventSource handles reconnect/framing; the server ends the stream with an
  // `end` event and closes the HTTP/1.0 connection.
  const src = new EventSource(`/api/subscribe/${card.subId}/stream`);
  card.source = src;
  const recEl = card.el.querySelector('[data-role="records"]');

  src.addEventListener("ready", () => {});
  src.addEventListener("heartbeat", () => {});
  src.addEventListener("record", (ev) => {
    const rec = JSON.parse(ev.data);
    card.records.push(rec);
    card.lineNos.add(rec.line_no);
    if (recEl.querySelectorAll(".rec").length < 200) {
      const div = document.createElement("div");
      div.className = "rec" + (rec.shard_index < 0 ? " catchup" : "");
      div.innerHTML = `<span class="ln">#${rec.line_no}</span><span class="ln">[s${rec.shard_index}]</span>${esc(rec.text)}`;
      recEl.appendChild(div);
      recEl.scrollTop = recEl.scrollHeight;
    }
    card.el.querySelector('[data-role="delivered"]').textContent = card.records.length;
  });
  src.addEventListener("end", (ev) => {
    const data = JSON.parse(ev.data);
    card.endState = data.state;
    card.endSnapshot = data.snapshot;
    setCardState(card, data.state, data.error);
    src.close();
    card.source = null;
  });
  src.onerror = () => {
    // The server deliberately closes after `end`; a closed source is expected.
    if (src.readyState === EventSource.CLOSED) return;
  };
}

function setCardState(card, st, error) {
  card.el.className = `sub-card ${st}`;
  const badge = card.el.querySelector('[data-role="state"]');
  badge.className = `badge ${st}`;
  badge.textContent = st + (error ? `: ${error}` : "");
  const isTerminal = ["done", "canceled", "overflow", "timeout", "error"].includes(st);
  card.el.querySelector('[data-act="resume"]').disabled = !isTerminal;
  card.el.querySelector('[data-act="cancel"]').disabled = isTerminal;
  card.el.querySelector('[data-act="slow"]').disabled = isTerminal;
  if (isTerminal && card.source) { card.source.close(); card.source = null; }
}

async function toggleSlow(card) {
  card.slow = !card.slow;
  try {
    await api(`/api/subscribe/${card.subId}/slow`, { slow: card.slow });
    card.el.querySelector('[data-role="slowflag"]').style.display =
      card.slow ? "inline" : "none";
  } catch (e) { alert(e.message); }
}

async function cancel(card) {
  try {
    await api(`/api/subscribe/${card.subId}/cancel`, {});
    setCardState(card, "canceled");
  } catch (e) { alert(e.message); }
}

async function resume(card) {
  try {
    const r = await api(`/api/subscribe/${card.subId}/resume`, {});
    // Resume returns a NEW subscriber id with its own stream.
    state.cards.delete(card.subId);
    card.el.remove();
    openCard({ ...r, label: `${card.el.querySelector(".sub-title").textContent}-resumed` },
             card.filterSpec);
  } catch (e) { alert(e.message); }
}

async function compare(card) {
  const box = card.el.querySelector('[data-role="compare"]');
  box.className = "compare pending";
  box.textContent = "独立全扫核对中…";
  try {
    const body = { file: $("query-file").value, filter: card.filterSpec };
    const truth = await api("/api/independent", body);
    const want = new Set(truth.records.map((r) => r.line_no));
    const got = card.lineNos;
    const missing = [...want].filter((x) => !got.has(x));
    const extra = [...got].filter((x) => !want.has(x));
    if (!missing.length && !extra.length && got.size === want.size) {
      box.className = "compare ok";
      box.textContent = `✓ 核对一致：${got.size} 条命中，与独立全扫（读取 ${fmtBytes(truth.physical_bytes_read)}）逐行相同，无重复无丢失。`;
    } else {
      box.className = "compare bad";
      box.textContent = `✗ 不一致：缺 ${missing.length} 条 / 多 ${extra.length} 条（独立命中 ${want.size}，本地 ${got.size}）。`;
    }
    card.compareDone = true;
  } catch (e) {
    box.className = "compare bad";
    box.textContent = "核对失败：" + e.message;
  }
}

/* ============================================================ debug */
$("btn-crash").addEventListener("click", async () => {
  if (!confirm("将 SIGKILL 硬杀服务器进程。需要外部重启（或运行 crash_recovery.py）。继续？")) return;
  try {
    await fetch("/api/debug/crash", { method: "POST" });
  } catch { /* the process is dead — the poller shows offline */ }
  flashCrashNote("进程已被 SIGKILL。重启服务器后，轮询会自动恢复并显示 recovered 会话。");
});

$("btn-corrupt").addEventListener("click", async () => {
  const first = state.lastSnapshot?.sessions?.[0];
  if (!first) return alert("当前没有会话可污染");
  try {
    await api("/api/debug/corrupt", { session_id: first.session_id });
    flashCrashNote(`已向 ${first.session_id} 的日志注入越界分片；下次重启恢复时该日志会被隔离（quarantine）。`);
  } catch (e) { alert(e.message); }
});

function flashCrashNote(msg) { $("crash-note").textContent = msg; }

/* ============================================================ polling render */
function renderTotals(t) {
  $("totals").innerHTML = `
    <div class="stat"><div class="k">会话 运行/暂停/完成</div>
      <div class="v">${t.running_sessions}/${t.paused_sessions}/${t.finished_sessions}</div></div>
    <div class="stat"><div class="k">活跃订阅者</div>
      <div class="v">${t.active_subscribers}</div></div>
    <div class="stat full"><div class="k">共享扫描物理读取（worker，一次/文件）</div>
      <div class="v share">${fmtBytes(t.shared_physical_bytes_read)}</div></div>
    <div class="stat"><div class="k">catch-up 补读</div>
      <div class="v catch">${fmtBytes(t.catchup_physical_bytes_read)}</div></div>
    <div class="stat"><div class="k">独立全扫 ×${t.independent_scan_count}</div>
      <div class="v indep">${fmtBytes(t.independent_physical_bytes_read)}</div></div>
    <div class="stat full"><div class="k">总真实物理读取</div>
      <div class="v total">${fmtBytes(t.total_physical_bytes_read)}</div></div>`;
}

function renderSessions(sessions) {
  const el = $("sessions");
  if (!sessions.length) { el.innerHTML = '<p class="empty">无会话</p>'; return; }
  el.innerHTML = sessions.map((s) => {
    const done = s.completed_shards.length;
    const pct = s.shard_count ? Math.round((done / s.shard_count) * 100) : 0;
    return `<div class="sess">
      <div class="sid">${esc(s.session_id)}
        <span class="badge ${s.state}" style="margin-left:6px">${s.state}${s.recovered ? " · recovered" : ""}</span></div>
      <div class="smeta">
        <span>${esc(s.file)} · 分片 ${done}/${s.shard_count} · 订阅者 ${s.subscribers.length}</span>
        <span>worker ${fmtBytes(s.worker_bytes_read)} · catch-up ${fmtBytes(s.catchup_bytes_read)}
          · 共 ${fmtBytes(s.physical_bytes_read)} / 文件 ${fmtBytes(s.file_size)}</span>
        <span class="hash">${esc(s.version_full)}</span>
      </div>
      <div class="shardbar"><i style="width:${pct}%"></i></div>
    </div>`;
  }).join("");
}

function renderEvents(events) {
  const el = $("events");
  const fresh = events.filter((e) => e.ts > state.lastEventTs);
  if (!fresh.length) return;
  state.lastEventTs = events[events.length - 1].ts;
  for (const e of fresh) {
    const div = document.createElement("div");
    div.className = `ev ${e.kind}`;
    div.innerHTML = `<span class="t">${fmtTime(e.ts)}</span><b>${esc(e.kind)}</b> ${esc(e.message)}`;
    el.appendChild(div);
  }
  el.scrollTop = el.scrollHeight;
}

// Update live per-subscriber buffer occupancy / counters from the authoritative
// server snapshot (SSE carries records; this carries backpressure state).
function renderSubscriberState(sessions) {
  for (const s of sessions) {
    for (const snap of s.subscribers) {
      const card = state.cards.get(snap.sub_id);
      if (!card) continue;
      card.el.querySelector('[data-role="matched"]').textContent = snap.matched_records;
      card.el.querySelector('[data-role="session"]').textContent = s.session_id;
      if (!card.endState) {
        card.el.querySelector('[data-role="delivered"]').textContent =
          snap.delivered_records;
      }
      card.el.querySelector('[data-role="buf"]').textContent =
        `${snap.queued_items}/${snap.max_items} 条 · ${fmtBytes(snap.queued_bytes)}`;
      const bar = card.el.querySelector('[data-role="bufbar"]');
      const pct = Math.min(100, (snap.queued_items / snap.max_items) * 100);
      bar.firstElementChild.style.width = pct + "%";
      bar.className = "bufbar" + (pct >= 100 ? " full" : pct >= 70 ? " hot" : "");
      card.el.querySelector('[data-role="slowflag"]').style.display =
        snap.slow ? "inline" : "none";
      card.slow = snap.slow;
      // Reflect server-driven terminal transitions (overflow/timeout) even if
      // the SSE end event was missed.
      if (["overflow", "timeout", "error"].includes(snap.state) && !card.endState)
        setCardState(card, snap.state, snap.error);
    }
  }
}

async function poll() {
  try {
    const snap = await api("/api/state");
    state.lastSnapshot = snap;
    state.pollFailures = 0;
    $("conn-dot").className = "dot online";
    $("conn-text").textContent = "已连接";
    renderTotals(snap.totals);
    renderSessions(snap.sessions);
    renderEvents(snap.events);
    renderSubscriberState(snap.sessions);
  } catch {
    state.pollFailures++;
    $("conn-dot").className = "dot offline";
    $("conn-text").textContent = state.pollFailures > 2 ? "服务器离线（等待重启恢复）" : "重连中…";
  }
}

/* ============================================================ boot */
async function boot() {
  await refreshFiles().catch(() => {});
  poll();
  setInterval(poll, 1000);
  setInterval(refreshFiles, 5000);
}
boot();
