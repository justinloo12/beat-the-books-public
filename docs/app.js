/* Beat the Books — quant research dashboard.
   Vanilla JS, no build step, no CDN dependencies. Charts are hand-rolled SVG. */

"use strict";

/* ────────────────────────── formatters ────────────────────────── */

const fmt = {
  pct: (v, d = 1) => (v == null ? "—" : `${(v * 100).toFixed(d)}%`),
  spct: (v, d = 1) => (v == null ? "—" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(d)}%`),
  pp: (v, d = 2) => (v == null ? "—" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(d)} pp`),
  units: (v, d = 2) => (v == null ? "—" : `${v >= 0 ? "+" : ""}${v.toFixed(d)}u`),
  odds: (v) => (v == null ? "—" : `${v > 0 ? "+" : ""}${v}`),
  num: (v, d = 2) => (v == null ? "—" : (+v).toFixed(d)),
  date: (v) =>
    v == null
      ? "—"
      : new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" }).format(
          new Date(`${v}T12:00:00`)
        ),
  dateLong: (v) =>
    v == null
      ? "—"
      : new Intl.DateTimeFormat("en-US", { weekday: "long", month: "long", day: "numeric", year: "numeric" }).format(
          new Date(`${v}T12:00:00`)
        ),
};

const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const el = (id) => document.getElementById(id);

function nBadge(n, reliable) {
  const cls = reliable ? "n-ok" : "n-small";
  return `<span class="n-badge ${cls}" title="${reliable ? "Sample size" : "Small sample — treat as noise"}">n=${n}</span>`;
}

function signClass(v) {
  if (v == null || v === 0) return "flat";
  return v > 0 ? "pos" : "neg";
}

/* ────────────────────────── SVG line chart ────────────────────────── */

function lineChart({ points, yLabel, yFormat = (v) => v.toFixed(2), zeroLine = true, width = 640, height = 240 }) {
  if (!points || points.length === 0) {
    return `<div class="empty">No data yet.</div>`;
  }
  const pad = { top: 14, right: 14, bottom: 26, left: 56 };
  const w = width - pad.left - pad.right;
  const h = height - pad.top - pad.bottom;

  const ys = points.map((p) => p.y);
  let yMin = Math.min(...ys, zeroLine ? 0 : Infinity);
  let yMax = Math.max(...ys, zeroLine ? 0 : -Infinity);
  if (yMin === yMax) {
    yMin -= 1;
    yMax += 1;
  }
  const spanPad = (yMax - yMin) * 0.12;
  yMin -= spanPad;
  yMax += spanPad;

  const x = (i) => pad.left + (points.length === 1 ? w / 2 : (i / (points.length - 1)) * w);
  const y = (v) => pad.top + h - ((v - yMin) / (yMax - yMin)) * h;

  let grid = "";
  const ticks = 4;
  for (let t = 0; t <= ticks; t++) {
    const v = yMin + ((yMax - yMin) * t) / ticks;
    const yy = y(v);
    grid += `<line x1="${pad.left}" y1="${yy}" x2="${pad.left + w}" y2="${yy}" class="grid-line"/>`;
    grid += `<text x="${pad.left - 8}" y="${yy + 4}" class="tick-label" text-anchor="end">${yFormat(v)}</text>`;
  }
  if (zeroLine && yMin < 0 && yMax > 0) {
    grid += `<line x1="${pad.left}" y1="${y(0)}" x2="${pad.left + w}" y2="${y(0)}" class="zero-line"/>`;
  }

  const path = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(p.y).toFixed(1)}`).join(" ");
  const last = points[points.length - 1];
  const lastCls = signClass(last.y);

  const baseY = y(Math.max(Math.min(0, yMax), yMin));
  const area =
    `M${x(0).toFixed(1)},${baseY.toFixed(1)} ` +
    points.map((p, i) => `L${x(i).toFixed(1)},${y(p.y).toFixed(1)}`).join(" ") +
    ` L${x(points.length - 1).toFixed(1)},${baseY.toFixed(1)} Z`;

  const dots =
    points.length <= 60
      ? points
          .map(
            (p, i) =>
              `<circle cx="${x(i).toFixed(1)}" cy="${y(p.y).toFixed(1)}" r="2.6" class="dot"><title>${esc(
                p.label
              )}: ${yFormat(p.y)}</title></circle>`
          )
          .join("")
      : "";

  const xFirst = points[0].label || "";
  const xLast = last.label || "";

  return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(yLabel)}" preserveAspectRatio="xMidYMid meet">
    ${grid}
    <path d="${area}" class="area ${lastCls}"/>
    <path d="${path}" class="line ${lastCls}"/>
    ${dots}
    <text x="${pad.left}" y="${height - 6}" class="tick-label">${esc(xFirst)}</text>
    <text x="${pad.left + w}" y="${height - 6}" class="tick-label" text-anchor="end">${esc(xLast)}</text>
  </svg>`;
}

/* ────────────────────────── performance section ────────────────────────── */

function renderPerformance(metrics) {
  const overall = metrics.overall || {};
  const clv = metrics.clv || {};
  const threshold = metrics.small_sample_threshold ?? 100;

  const warn = el("sample-warning");
  if ((overall.n ?? 0) < threshold) {
    warn.classList.remove("hidden");
    warn.innerHTML =
      `<strong>Small-sample warning:</strong> only ${overall.n ?? 0} graded entries so far ` +
      `(threshold for meaning: ${threshold}). Every number below is statistically indistinguishable ` +
      `from luck. This page exists to accumulate evidence, not to claim an edge.`;
  }

  const record = `${overall.wins ?? 0}–${overall.losses ?? 0}${overall.pushes ? `–${overall.pushes}` : ""}`;
  const stats = [
    { label: "Graded entries", value: String(overall.n ?? 0), sub: "picks + leans", cls: "flat" },
    { label: "Record", value: record, sub: `hit rate ${fmt.pct(overall.hit_rate)}`, cls: "flat" },
    { label: "P&L (flat 1u)", value: fmt.units(overall.profit_units), sub: `ROI ${fmt.spct(overall.roi)}`, cls: signClass(overall.profit_units) },
    { label: "Avg CLV", value: fmt.pp(clv.mean_clv), sub: `${clv.n ?? 0} priced vs close`, cls: signClass(clv.mean_clv) },
  ];
  el("perf-stats").innerHTML = stats
    .map(
      (s) => `<div class="stat-card">
        <div class="stat-label">${esc(s.label)}</div>
        <div class="stat-value ${s.cls}">${esc(s.value)}</div>
        <div class="stat-sub">${esc(s.sub)}</div>
      </div>`
    )
    .join("");

  el("clv-n").innerHTML = nBadge(clv.n ?? 0, clv.reliable);
  const clvPoints = (clv.cumulative || []).map((c) => ({ y: c.cum_avg_clv * 100, label: c.date }));
  el("clv-chart").innerHTML =
    clvPoints.length >= 2
      ? lineChart({ points: clvPoints, yLabel: "Cumulative average CLV", yFormat: (v) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}pp` })
      : `<div class="empty">CLV accumulates as intraday odds snapshots land — ${clv.n ?? 0} sample(s) so far.</div>`;
  el("clv-note").textContent = clv.proxy_note || "";

  const daily = metrics.daily || [];
  el("pnl-n").innerHTML = nBadge(overall.n ?? 0, overall.reliable);
  const pnlPoints = daily.map((d) => ({ y: d.cum_units, label: d.date }));
  el("pnl-chart").innerHTML =
    pnlPoints.length >= 2
      ? lineChart({ points: pnlPoints, yLabel: "Cumulative units", yFormat: (v) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}u` })
      : `<div class="empty">Not enough graded days to chart.</div>`;

  renderRecordTable(el("tier-table"), metrics.by_tier || {}, "Tier");
  renderRecordTable(el("market-table"), metrics.by_market || {}, "Market");
  renderCalibration(metrics.calibration || {});
  renderMetaModel(metrics.meta_model || {});
}

function renderRecordTable(table, groups, label) {
  const names = Object.keys(groups);
  if (names.length === 0) {
    table.innerHTML = `<tbody><tr><td class="empty-cell">No graded entries yet.</td></tr></tbody>`;
    return;
  }
  const rows = names
    .map((name) => {
      const g = groups[name];
      return `<tr>
        <td class="mono">${esc(name)}</td>
        <td>${g.wins}–${g.losses}${g.pushes ? `–${g.pushes}` : ""}</td>
        <td>${fmt.pct(g.hit_rate)}</td>
        <td class="${signClass(g.profit_units)}">${fmt.units(g.profit_units)}</td>
        <td class="${signClass(g.roi)}">${fmt.spct(g.roi)}</td>
        <td>${nBadge(g.n, g.reliable)}</td>
      </tr>`;
    })
    .join("");
  table.innerHTML = `<thead><tr><th>${esc(label)}</th><th>W–L</th><th>Hit</th><th>P&amp;L</th><th>ROI</th><th>Sample</th></tr></thead><tbody>${rows}</tbody>`;
}

function renderCalibration(cal) {
  el("cal-n").innerHTML = nBadge(cal.n ?? 0, cal.reliable);
  const rows = (cal.rows || [])
    .map((r) => {
      if (!r.n) {
        return `<tr class="dim"><td class="mono">${esc(r.bucket)}</td><td>0</td><td>—</td><td>—</td><td>—</td></tr>`;
      }
      return `<tr>
        <td class="mono">${esc(r.bucket)}</td>
        <td>${r.n}</td>
        <td>${fmt.pct(r.avg_predicted)}</td>
        <td>${fmt.pct(r.win_rate)}</td>
        <td class="${signClass(r.gap)}">${fmt.pp(r.gap, 1)}</td>
      </tr>`;
    })
    .join("");
  el("calibration-table").innerHTML =
    `<thead><tr><th>Model prob</th><th>N</th><th>Predicted</th><th>Realized</th><th>Gap</th></tr></thead><tbody>${rows}</tbody>`;
}

function renderMetaModel(meta) {
  const trained = meta.state === "trained";
  let coefRows = "";
  if (trained && meta.coefficients) {
    coefRows = `<div class="table-scroll"><table class="coef-table"><thead><tr><th>Feature</th><th>Coefficient</th></tr></thead><tbody>${Object.entries(
      meta.coefficients
    )
      .map(
        ([k, v]) =>
          `<tr><td class="mono">${esc(k)}</td><td class="${signClass(v)} mono">${v >= 0 ? "+" : ""}${(+v).toFixed(4)}</td></tr>`
      )
      .join("")}</tbody></table></div>`;
  }
  el("meta-model-card").innerHTML = `
    <div class="meta-status ${trained ? "meta-trained" : "meta-fallback"}">
      <span class="meta-dot"></span>
      <span class="meta-state">${trained ? "TRAINED" : "FALLBACK"}</span>
      <span class="meta-msg">${esc(meta.message || "status unavailable")}</span>
    </div>
    <p class="panel-note">
      A logistic regression stacking the raw module signals with the market's no-vig probability.
      It stays inert until ${esc(String(meta.threshold ?? 150))}+ graded picks with stored signals exist —
      fitting a meta-model on a couple dozen bets would be curve-fitting noise. Until then the
      published probabilities are the hand-set config blend, untouched.
    </p>
    ${coefRows}
    ${trained ? `<p class="panel-note">Trained ${esc(meta.trained_at || "")} · in-sample accuracy ${fmt.pct(meta.train_accuracy)}</p>` : ""}
  `;
}

/* ────────────────────────── today's board ────────────────────────── */

function pickCard(p, kind) {
  const edgeCls = signClass(p.edge);
  return `<div class="pick-card">
    <div class="pick-top">
      <span class="pick-market mono">${esc(p.market_type)}</span>
      <span class="pick-tier tier-${esc((p.tier || "").toLowerCase())}">${esc(p.tier || "")}</span>
    </div>
    <div class="pick-main">${esc(p.pick)} ${p.market_type === "game_total" ? esc(String(p.line)) : ""} <span class="mono">${fmt.odds(p.american_odds)}</span></div>
    <div class="pick-matchup">${esc(p.matchup)}${p.start_time ? ` · ${esc(p.start_time)}` : ""}</div>
    <div class="pick-nums mono">
      <span>model ${fmt.pct(p.model_probability)}</span>
      <span>market ${fmt.pct(p.no_vig_probability)}</span>
      <span class="${edgeCls}">edge ${fmt.spct(p.edge)}</span>
    </div>
    ${kind === "lean" ? `<div class="pick-flag">lean — not a bet</div>` : ""}
  </div>`;
}

function renderBoard(latest) {
  el("board-date").textContent = latest?.date
    ? `${fmt.dateLong(latest.date)} — picks lock at first publication and are graded next morning.`
    : "No board available.";

  const picks = latest?.daily?.picks || [];
  const leans = latest?.daily?.leans || [];

  el("board-picks").innerHTML = picks.length
    ? picks.map((p) => pickCard(p, "pick")).join("")
    : `<div class="empty">No qualifying edges today — the model passes when the mid-band (3–6% edge) is empty. Passing is a position.</div>`;

  if (leans.length) {
    el("board-leans-wrap").classList.remove("hidden");
    el("board-leans").innerHTML = leans.map((p) => pickCard(p, "lean")).join("");
  }

  renderSlate(latest?.daily?.lineup_cards || []);
}

function renderSlate(cards) {
  if (!cards.length) {
    el("slate-table").innerHTML = `<tbody><tr><td class="empty-cell">No games on the current board.</td></tr></tbody>`;
    return;
  }
  const rows = cards
    .map((c) => {
      const marketTotal = (c.top_game_picks || []).find((q) => q.market_type === "game_total");
      const modelTotal = c.simulated_total ?? c.projected_total;
      const diff = marketTotal && modelTotal != null ? modelTotal - marketTotal.line : null;
      return `<tr>
        <td>
          <div class="slate-matchup">${esc(c.matchup)}</div>
          <div class="slate-sub">${esc(c.away_pitcher?.name || "TBD")} vs ${esc(c.home_pitcher?.name || "TBD")}</div>
        </td>
        <td class="mono">${c.start_time ? esc(c.start_time) : "—"}</td>
        <td class="mono">${marketTotal ? fmt.num(marketTotal.line, 1) : "—"}</td>
        <td class="mono">${fmt.num(modelTotal, 2)}</td>
        <td class="mono ${signClass(diff)}">${diff == null ? "—" : (diff >= 0 ? "+" : "") + diff.toFixed(2)}</td>
        <td class="mono">${fmt.pct(c.home_win_prob, 0)}</td>
        <td><span class="lineup-flag ${c.lineup_status === "confirmed" ? "ok" : ""}">${esc(c.lineup_status || "projected")}</span></td>
      </tr>`;
    })
    .join("");
  el("slate-table").innerHTML =
    `<thead><tr><th>Game</th><th>First pitch</th><th>Mkt total</th><th>Model</th><th>Δ</th><th>Home win</th><th>Lineups</th></tr></thead><tbody>${rows}</tbody>`;
}

/* ────────────────────────── archive ────────────────────────── */

function renderArchiveDates(index) {
  const dates = index?.dates || [];
  if (!dates.length) {
    el("archive-dates").innerHTML = `<div class="empty">No archived days yet.</div>`;
    return;
  }
  el("archive-dates").innerHTML = dates
    .map(
      (d) => `<button class="archive-date" data-date="${esc(d.date)}">
        <span class="mono">${esc(d.date)}</span>
        <span class="archive-meta">${d.picks} pick${d.picks === 1 ? "" : "s"} · ${d.leans ?? 0} lean${(d.leans ?? 0) === 1 ? "" : "s"} · ${d.games} games</span>
      </button>`
    )
    .join("");
  el("archive-dates")
    .querySelectorAll(".archive-date")
    .forEach((btn) => btn.addEventListener("click", () => loadArchiveDay(btn.dataset.date, btn)));
}

async function loadArchiveDay(date, btn) {
  el("archive-dates").querySelectorAll(".archive-date").forEach((b) => b.classList.remove("active"));
  if (btn) btn.classList.add("active");
  const detail = el("archive-detail");
  detail.innerHTML = `<div class="empty">Loading ${esc(date)}…</div>`;
  try {
    const day = await fetchJSON(`data/${date}.json`);
    const picks = day?.daily?.picks || [];
    const leans = day?.daily?.leans || [];
    const all = [...picks.map((p) => ({ ...p, _kind: "pick" })), ...leans.map((p) => ({ ...p, _kind: "lean" }))];
    detail.innerHTML = `
      <h3 class="subhead">${fmt.dateLong(date)}</h3>
      ${all.length ? `<div class="cards">${all.map((p) => pickCard(p, p._kind)).join("")}</div>` : `<div class="empty">No picks or leans published that day.</div>`}
    `;
  } catch {
    detail.innerHTML = `<div class="empty">Could not load ${esc(date)}.</div>`;
  }
}

/* ────────────────────────── history ────────────────────────── */

function renderHistory(history) {
  const entries = (history || []).filter((e) => e.result);
  el("history-sub").textContent = entries.length
    ? `${entries.length} graded entries · flat 1u stakes at archived odds · CLV vs pre-first-pitch snapshot.`
    : "No graded entries yet.";
  if (!entries.length) {
    el("history-table").innerHTML = `<tbody><tr><td class="empty-cell">Nothing graded yet.</td></tr></tbody>`;
    return;
  }
  const rows = entries
    .map((e) => {
      const resCls = e.result === "win" ? "pos" : e.result === "loss" ? "neg" : "flat";
      return `<tr>
        <td class="mono">${esc(e.date)}</td>
        <td>${esc(e.matchup)}</td>
        <td class="mono">${esc(e.market_type)}</td>
        <td>${esc(e.pick)}${e.market_type === "game_total" ? ` ${esc(String(e.line))}` : ""}</td>
        <td class="mono">${fmt.odds(e.american_odds)}</td>
        <td class="mono">${fmt.spct(e.edge)}</td>
        <td class="mono ${e.clv == null ? "dim" : signClass(e.clv)}">${e.clv == null ? "—" : fmt.pp(e.clv)}</td>
        <td class="${resCls}">${esc(e.result)}</td>
        <td class="mono ${signClass(e.pnl)}">${e.pnl == null ? "—" : fmt.units(e.pnl / 100)}</td>
      </tr>`;
    })
    .join("");
  el("history-table").innerHTML =
    `<thead><tr><th>Date</th><th>Matchup</th><th>Market</th><th>Pick</th><th>Odds</th><th>Edge</th><th>CLV</th><th>Result</th><th>P&amp;L</th></tr></thead><tbody>${rows}</tbody>`;
}

/* ────────────────────────── nav highlighting ────────────────────────── */

function initNav() {
  const links = Array.from(document.querySelectorAll(".nav-links a"));
  const sections = links.map((a) => document.querySelector(a.getAttribute("href"))).filter(Boolean);
  const activate = () => {
    let current = sections[0];
    for (const s of sections) {
      if (s.getBoundingClientRect().top <= 120) current = s;
    }
    links.forEach((a) => a.classList.toggle("active", a.getAttribute("href") === `#${current.id}`));
  };
  document.addEventListener("scroll", activate, { passive: true });
  activate();
}

/* ────────────────────────── boot ────────────────────────── */

async function fetchJSON(path) {
  const resp = await fetch(path, { cache: "no-store" });
  if (!resp.ok) throw new Error(`${resp.status} for ${path}`);
  return resp.json();
}

async function boot() {
  initNav();

  const [metrics, latest, archive, pickHistory] = await Promise.all([
    fetchJSON("data/metrics.json").catch(() => null),
    fetchJSON("data/latest.json").catch(() => null),
    fetchJSON("data/archive_index.json").catch(() => null),
    fetchJSON("data/pick_history.json").catch(() => null),
  ]);

  if (metrics) {
    renderPerformance(metrics);
    el("perf-asof").textContent =
      `Every metric carries its sample size — below n=${metrics.small_sample_threshold} the honest label is "noise". ` +
      `Updated ${metrics.generated_at ? metrics.generated_at.slice(0, 16).replace("T", " ") + " UTC" : "—"}.`;
  } else if (latest?.meta_model) {
    el("perf-asof").textContent = "metrics.json not found — run export_site to generate performance metrics.";
    renderMetaModel(latest.meta_model);
  } else {
    el("perf-asof").textContent = "No data payloads found.";
  }

  if (latest) {
    renderBoard(latest);
    el("footer-asof").textContent = latest.as_of ? `Data as of ${latest.as_of.slice(0, 16).replace("T", " ")} UTC` : "";
  }
  // pick_history.json is the grading source of truth and updates independently
  // of board exports; latest.json's embedded history is the fallback.
  const historyEntries = pickHistory ? [...pickHistory].reverse() : latest?.history || [];
  renderHistory(historyEntries);
  if (archive) renderArchiveDates(archive);
}

boot();
