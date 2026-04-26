/* ── State ── */
let currentCards = [];
let activeMatchup = null;
let activeArsenalSplit = {}; // matchup -> "overall"|"vs_l"|"vs_r"

/* ── DOM refs ── */
const $summaryGrid = document.getElementById("summary-grid");
const $dailyPicks  = document.getElementById("daily-picks");
const $dailyMeta   = document.getElementById("daily-meta");
const $historyTbl  = document.getElementById("history-table");
const $gameTabs    = document.getElementById("game-tabs");
const $gameDetail  = document.getElementById("game-detail");
const $skippedList = document.getElementById("skipped-list");
const $heroPills   = document.getElementById("hero-pills");
const $heroMeta    = document.getElementById("hero-meta");

/* ── Formatters ── */
const fmt = {
  pct:  (v, d=1) => v == null ? "—" : `${(+v*100).toFixed(d)}%`,
  pctS: (v, d=1) => `${+(v||0)>=0?"+":""}${(+(v||0)*100).toFixed(d)}%`,
  num:  (v, d=2) => v == null ? "—" : `${(+v).toFixed(d)}`,
  sign: (v, d=2) => `${+(v||0)>=0?"+":""}${(+(v||0)).toFixed(d)}`,
  money: (v, d=2) => v == null ? "—" : `${+(v||0)>=0?"+":""}$${Math.abs(+(v||0)).toFixed(d)}`,
  odds: (v) => `${+(v||0)>0?"+":""}${+(v||0)}`,
  date: (v) => new Intl.DateTimeFormat("en-US",{weekday:"long",month:"long",day:"numeric",year:"numeric"}).format(new Date(`${v}T12:00:00`)),
  time: (v) => new Intl.DateTimeFormat("en-US",{hour:"numeric",minute:"2-digit",timeZone:"America/New_York"}).format(new Date(v)),
};

function windLabel(weather) {
  const spd = +(weather?.wind_speed_mph||0);
  const dirs = ["N","NE","E","SE","S","SW","W","NW"];
  const dir = weather?.wind_direction != null
    ? dirs[Math.round(+weather.wind_direction/45)%8]
    : "calm";
  return spd < 2 ? "Calm" : `${spd.toFixed(0)} mph ${dir}`;
}

function tierBadge(tier) {
  const map = { strong:"strong", moderate:"moderate", monitor:"monitor", pass:"pass", block:"block" };
  const cls = map[tier?.toLowerCase()] || "neutral";
  return `<span class="badge badge-${cls}">${tier||"—"}</span>`;
}

function matchupScoreBadge(score) {
  const n = +(score||0);
  const cls = n>=62?"mscore-high": n>=48?"mscore-mid": n>=36?"mscore-low":"mscore-neutral";
  return `<span class="matchup-score-badge ${cls}">${n.toFixed(1)}</span>`;
}

function pitchMatchClass(score) {
  const n = +(score||0);
  return n>=62?"pm-good": n>=48?"pm-mid":"pm-bad";
}

function valClass(v, goodHigh=true, goodThr=0.36, badThr=0.44) {
  const n = +(v||0);
  if (goodHigh) return n >= goodThr ? "val-good" : n >= badThr * 0.85 ? "val-warn" : "val-bad";
  return n <= goodThr ? "val-good" : n <= badThr ? "val-warn" : "val-bad";
}

function stuffBadgeClass(sp) {
  if (sp == null) return "badge-neutral";
  if (sp >= 120) return "badge-elite";
  if (sp >= 108) return "badge-strong";
  if (sp >= 93)  return "badge-moderate";
  return "badge-pass";
}

function stuffCellClass(sp) {
  if (sp == null) return "";
  if (sp >= 120) return "val-good";
  if (sp >= 108) return "val-good";
  if (sp >= 93)  return "val-warn";
  return "val-bad";
}

/* ── Data Fetch ── */
async function fetchPayload() {
  try {
    const r = await fetch("/api/site/today");
    if (r.ok) return r.json();
    throw new Error("api");
  } catch {
    const r = await fetch(`data/latest.json?ts=${Date.now()}`);
    if (!r.ok) throw new Error("no snapshot");
    return r.json();
  }
}

/* ── Render Hero ── */
function renderHero(payload) {
  const cards = payload.daily.lineup_cards || [];
  const picks = payload.daily.picks || [];
  const strongWind = cards.slice().sort((a,b)=>+(b.weather?.wind_speed_mph||0)-+(a.weather?.wind_speed_mph||0))[0];
  const strongCount = picks.filter(p=>p.tier==="strong").length;

  $heroPills.innerHTML = [
    `<span class="pill accent">${cards.length} games on board</span>`,
    `<span class="pill">${picks.length} picks posted</span>`,
    strongCount ? `<span class="pill">${strongCount} strong edge${strongCount>1?"s":""}</span>` : "",
    strongWind ? `<span class="pill">${strongWind.matchup}: ${windLabel(strongWind.weather)}</span>` : "",
  ].filter(Boolean).join("");

  $heroMeta.innerHTML = `
    <div class="hero-date">${fmt.date(payload.date)}</div>
    <div class="hero-updated">Updated ${fmt.time(payload.as_of)} ET</div>
  `;
}

/* ── Render Summary ── */
function renderSummary(s) {
  const dollarsWon = (+(s.units_profit || 0)) * 100;
  const dollarsRisked = (+(s.units_risked || 0)) * 100;
  const cards = [
    ["Won/Lost",    fmt.money(dollarsWon),            `${fmt.money(dollarsRisked,0).replace("+","") } risked at $100 per bet`],
    ["ROI",         fmt.pct(s.roi),                   `${fmt.sign(s.units_profit)} units`],
    ["Tracked",     s.tracked_bets,                   `${s.wins}-${s.losses}-${s.pushes} W-L-P`],
    ["Hit Rate",    fmt.pct(s.hit_rate),               "graded picks"],
    ["CLV 50",      s.clv_last_50==null?"n/a":fmt.sign(s.clv_last_50,3), "last 50"],
    ["Today",       s.lineup_card_count,               `${s.daily_pick_count} pick${s.daily_pick_count!==1?"s":""}`],
  ];
  $summaryGrid.innerHTML = cards.map(([l,v,d])=>`
    <div class="stat-card">
      <div class="label">${l}</div>
      <div class="value">${v}</div>
      <div class="detail">${d}</div>
    </div>
  `).join("");
}

/* ── Render Picks ── */
function renderPicks(picks) {
  if (!picks.length) {
    $dailyPicks.innerHTML = `<div class="empty-state">No picks cleared the threshold today.</div>`;
    $dailyMeta.textContent = "";
    return;
  }
  $dailyMeta.textContent = `${picks.length} simulation-screened pick${picks.length!==1?"s":""} from today's full slate`;

  $dailyPicks.innerHTML = picks.map((pick, i) => {
    const tier = (pick.tier||"").toLowerCase();
    return `
    <article class="pick-card tier-${tier}">
      <div class="pick-header">
        <div class="pick-rank">#${i+1}</div>
        <div class="pick-main">
          <div class="pick-matchup">${pick.matchup}</div>
          <div class="pick-meta">
            ${tierBadge(pick.tier)}
            <span class="pick-market">${fmtMarketType(pick.market_type)} · ${pick.pick} ${pick.line??""} · ${pick.start_time??"TBD"}</span>
          </div>
        </div>
        <div class="pick-right">
          <div class="edge-value">${fmt.pctS(pick.edge)}</div>
          <div class="pick-odds-line">${fmt.odds(pick.american_odds)} · ${(+(pick.simulation_trials||0)).toLocaleString()} trials</div>
        </div>
      </div>
      <div class="pick-body">
        <div class="pick-stat">
          <div class="label">Model %</div>
          <div class="val">${fmt.pct(pick.model_probability)}</div>
        </div>
        <div class="pick-stat">
          <div class="label">No-vig %</div>
          <div class="val">${fmt.pct(pick.no_vig_probability)}</div>
        </div>
        <div class="pick-stat">
          <div class="label">Edge</div>
          <div class="val" style="color:${tier==="strong"?"var(--green)":tier==="moderate"?"var(--gold)":"var(--blue)"}">${fmt.pctS(pick.edge)}</div>
        </div>
        <div class="pick-stat">
          <div class="label">Stake</div>
          <div class="val">${fmt.pct(pick.bankroll_fraction)}</div>
        </div>
        <div class="pick-stat">
          <div class="label">Lineup</div>
          <div class="val">${pick.lineup_status??"—"}</div>
        </div>
      </div>
      <div class="pick-reasoning">${pickBlurb(pick)}</div>
    </article>
    `;
  }).join("");
}

function fmtMarketType(t) {
  const m = { game_total:"Total", first_five_total:"F5 Total", moneyline:"ML", runline:"RL", team_total:"Team Total" };
  return m[t] || t;
}

function pickBlurb(pick) {
  if (pick.specific_blurb) return pick.specific_blurb;
  const card = currentCards.find(c => c.matchup === pick.matchup);
  if (card) {
    const homeAvg = avgMatchup(card.home_lineup?.players || []);
    const awayAvg = avgMatchup(card.away_lineup?.players || []);
    const homeTop = topPlayers(card.home_lineup?.players || []);
    const awayTop = topPlayers(card.away_lineup?.players || []);
    const weatherNote = +(card.weather?.wind_speed_mph || 0) >= 12 ? ` Wind is up at ${windLabel(card.weather)}.` : "";

    if (pick.market_type === "game_total" && pick.pick === "Over") {
      const attackingHome = homeAvg >= awayAvg;
      const attackPlayers = attackingHome ? homeTop : awayTop;
      const targetPitcher = attackingHome ? card.away_pitcher : card.home_pitcher;
      return `${joinPlayers(attackPlayers)} rate as the strongest hitter-pitcher matchups on this board against ${targetPitcher?.name || "the opposing starter"}, who is allowing ${fmt.num(targetPitcher?.xba,3)} xBA and ${fmt.pct(targetPitcher?.hard_hit_pct,0)} hard-hit contact. The sim sits at ${fmt.num(card.simulated_total,1)} versus ${pick.line}.${weatherNote}`;
    }

    if (pick.market_type === "game_total" && pick.pick === "Under") {
      const suppressHome = lowPlayers(card.home_lineup?.players || []);
      const suppressAway = lowPlayers(card.away_lineup?.players || []);
      return `${card.away_pitcher?.name || "Away starter"} and ${card.home_pitcher?.name || "Home starter"} both project to limit clean contact early, and the weakest matchup clusters belong to ${joinPlayers(suppressHome)} and ${joinPlayers(suppressAway)}. The sim lands at ${fmt.num(card.simulated_total,1)} against ${pick.line}.${weatherNote}`;
    }

    if (pick.market_type === "moneyline") {
      const teamSide = pick.pick === card.home_pitcher?.team || pick.pick === card.home_lineup?.team;
      const teamPlayers = teamSide ? homeTop : awayTop;
      const starter = teamSide ? card.home_pitcher : card.away_pitcher;
      const oppStarter = teamSide ? card.away_pitcher : card.home_pitcher;
      const teamRuns = teamSide ? card.simulated_home_runs : card.simulated_away_runs;
      const oppRuns = teamSide ? card.simulated_away_runs : card.simulated_home_runs;
      return `${starter?.name || pick.pick} gives this side the cleaner starter setup over ${oppStarter?.name || "the opponent"}, while ${joinPlayers(teamPlayers)} own the best arsenal matchups in the lineup. The sim has it ${fmt.num(teamRuns,1)} to ${fmt.num(oppRuns,1)}.${weatherNote}`;
    }
  }
  const market = fmtMarketType(pick.market_type);
  const lineup = pick.lineup_status === "confirmed" ? "confirmed lineups" : "projected lineups";
  const top = (pick.top_features || [])[0];
  const featureText = top ? `${top.feature} is a key driver` : "the simulation is pricing this side above market";
  return `${market} ${pick.pick}${pick.line != null ? ` ${pick.line}` : ""} cleared the board with a ${fmt.pctS(pick.edge)} edge. ${featureText}, and this card is still using ${lineup}.`;
}

function avgMatchup(players) {
  if (!players.length) return 0;
  return players.reduce((sum, player) => sum + (+(player.matchup_score || 0)), 0) / players.length;
}

function topPlayers(players) {
  return players.slice().sort((a, b) => (+(b.matchup_score || 0)) - (+(a.matchup_score || 0))).slice(0, 2);
}

function lowPlayers(players) {
  return players.slice().sort((a, b) => (+(a.matchup_score || 0)) - (+(b.matchup_score || 0))).slice(0, 2);
}

function joinPlayers(players) {
  if (!players.length) return "this lineup";
  if (players.length === 1) return `${players[0].name} (${fmt.num(players[0].matchup_score,0)})`;
  return `${players[0].name} and ${players[1].name} (${fmt.num(players[0].matchup_score,0)}/${fmt.num(players[1].matchup_score,0)})`;
}

/* ── Render Game Tabs ── */
function renderGames(cards) {
  currentCards = cards;
  if (!cards.length) {
    $gameTabs.innerHTML = `<div class="empty-state">No games on the board yet.</div>`;
    $gameDetail.innerHTML = "";
    return;
  }
  if (!cards.find(c=>c.matchup===activeMatchup)) activeMatchup = cards[0].matchup;

  $gameTabs.innerHTML = cards.map(c => {
    const total = +(c.simulated_total||c.projected_total||0);
    return `
    <button class="game-tab-btn ${c.matchup===activeMatchup?"active":""}" data-matchup="${c.matchup}">
      <span class="game-tab-label">${c.matchup}</span>
      <span class="game-tab-sub">${c.start_time??"TBD"} · ${total.toFixed(1)} sim total</span>
    </button>
    `;
  }).join("");

  $gameDetail.innerHTML = renderGameCard(cards.find(c=>c.matchup===activeMatchup));

  document.querySelectorAll(".game-tab-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      activeMatchup = btn.dataset.matchup;
      renderGames(currentCards);
    });
  });

  // wire arsenal tab buttons
  document.querySelectorAll(".arsenal-tab-btn").forEach(btn => {
    btn.addEventListener("click", e => {
      const panel = btn.closest("[data-pitcher-panel]");
      const split = btn.dataset.split;
      const key = panel?.dataset.pitcherPanel || "home";
      if (!activeArsenalSplit[activeMatchup]) activeArsenalSplit[activeMatchup] = {};
      activeArsenalSplit[activeMatchup][key] = split;
      // re-render just this panel's table
      const tableWrap = panel?.querySelector(".arsenal-table-wrap");
      if (tableWrap) {
        const pitcher = split==="vs_l"
          ? (panel._pitcher.arsenal_vs_l||[])
          : split==="vs_r"
            ? (panel._pitcher.arsenal_vs_r||[])
            : (panel._pitcher.arsenal||[]);
        tableWrap.innerHTML = arsenalTable(pitcher);
      }
      panel?.querySelectorAll(".arsenal-tab-btn").forEach(b => b.classList.toggle("active", b===btn));
    });
  });
}

/* ── Game Card ── */
function renderGameCard(card) {
  if (!card) return "";
  const weather = card.weather || {};
  const windHigh = +(weather.wind_speed_mph||0) >= 12;
  const topPicks = card.top_game_picks || [];
  const hp = card.home_pitcher;
  const ap = card.away_pitcher;

  return `
  <div class="game-card">
    <div class="game-card-header">
      <div>
        <div class="game-card-title">${card.matchup}</div>
        <div class="game-card-subtitle">${card.start_time??"TBD"} · ${card.venue} · ${card.lineup_status==="confirmed"?"✓ confirmed lineups":"projected lineup"}</div>
      </div>
      <div class="game-stats-bar">
        <div class="game-stat-item">
          <div class="label">Projected Total</div>
          <div class="val">${fmt.num(card.projected_total,2)}</div>
        </div>
        <div class="game-stat-item">
          <div class="label">Sim Total</div>
          <div class="val">${fmt.num(card.simulated_total,2)}</div>
        </div>
        <div class="game-stat-item">
          <div class="label">${ap?.team??""} Win</div>
          <div class="val">${fmt.pct(card.away_win_prob)}</div>
        </div>
        <div class="game-stat-item">
          <div class="label">${hp?.team??""} Win</div>
          <div class="val">${fmt.pct(card.home_win_prob)}</div>
        </div>
      </div>
    </div>

    <div class="weather-block">
      <div class="weather-item">
        <span class="wlabel">Temp</span>
        <span class="wval">${fmt.num(weather.temperature_f,0)}°F</span>
      </div>
      <div class="weather-item">
        <span class="wlabel">Wind</span>
        <span class="wval ${windHigh?"weather-wind-high":""}">${windLabel(weather)}</span>
      </div>
      <div class="weather-item">
        <span class="wlabel">Humidity</span>
        <span class="wval">${fmt.num(weather.humidity,0)}%</span>
      </div>
    </div>

    ${topPicks.length ? `
    <div class="game-picks-bar">
      ${topPicks.map(p=>`
        <div class="inline-pick tier-${(p.tier||"").toLowerCase()}">
          <span class="ip-pick">${fmtMarketType(p.market_type)} ${p.line??""} ${p.pick}</span>
          <span class="ip-meta">${fmt.odds(p.american_odds)}</span>
          ${tierBadge(p.tier)}
          <span class="ip-edge">${fmt.pctS(p.edge)}</span>
        </div>
      `).join("")}
    </div>
    ` : ""}

    <div class="pitcher-section">
      ${renderPitcherPanel(ap, "away")}
      ${renderPitcherPanel(hp, "home")}
    </div>

    <div class="lineup-grid">
      ${renderTeamSection(card.away_lineup)}
      ${renderTeamSection(card.home_lineup)}
    </div>
  </div>
  `;
}

/* ── Pitcher Panel ── */
function renderPitcherPanel(pitcher, side) {
  if (!pitcher) return "";
  const arsenal = pitcher.arsenal || [];
  const vsL = pitcher.arsenal_vs_l || [];
  const vsR = pitcher.arsenal_vs_r || [];
  const hand = pitcher.handedness || "?";
  const flag = pitcher.vulnerability_flag || "";
  const flagBadge = flag==="Elite"?"badge-elite": flag==="Low"?"badge-strong": flag==="Medium"?"badge-moderate":"badge-pass";

  return `
  <div class="pitcher-panel" data-pitcher-panel="${side}">
    <div class="pitcher-panel-head">
      <div>
        <div class="pitcher-name">${pitcher.name || "TBD"}</div>
        <div class="pitcher-team-label">${pitcher.team}</div>
      </div>
      <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
        <span class="badge ${hand==="L"?"badge-lhb":"badge-rhb"}">${hand}HP</span>
        <span class="badge ${flagBadge}">${flag||"—"}</span>
        <span class="badge badge-neutral">Qlty ${fmt.num(pitcher.quality_score,1)}</span>
        ${pitcher.stuff_plus!=null?`<span class="badge ${stuffBadgeClass(pitcher.stuff_plus)}">Stuff+ ${pitcher.stuff_plus}</span>`:""}
      </div>
    </div>

    <div class="pitcher-overall">
      <div class="pitcher-stat"><div class="label">xBA</div><div class="val ${valClass(pitcher.xba,false,0.240,0.270)}">${fmt.num(pitcher.xba,3)}</div></div>
      <div class="pitcher-stat"><div class="label">HH%</div><div class="val ${valClass(pitcher.hard_hit_pct,false,0.35,0.42)}">${fmt.pct(pitcher.hard_hit_pct)}</div></div>
      <div class="pitcher-stat"><div class="label">Barrel%</div><div class="val ${valClass(pitcher.barrel_pct,false,0.07,0.12)}">${fmt.pct(pitcher.barrel_pct)}</div></div>
      <div class="pitcher-stat"><div class="label">EV50</div><div class="val">${fmt.num(pitcher.ev50,1)}</div></div>
      <div class="pitcher-stat"><div class="label">K%</div><div class="val ${valClass(pitcher.weighted_k_pct,true,0.22,0.18)}">${fmt.pct(pitcher.weighted_k_pct)}</div></div>
      <div class="pitcher-stat"><div class="label">BB%</div><div class="val ${valClass(pitcher.weighted_bb_pct,false,0.08,0.12)}">${fmt.pct(pitcher.weighted_bb_pct)}</div></div>
      <div class="pitcher-stat"><div class="label">Ext</div><div class="val">${fmt.num(pitcher.extension,1)} ft</div></div>
      <div class="pitcher-stat"><div class="label">Run Val</div><div class="val">${fmt.sign(pitcher.weighted_run_value,3)}</div></div>
    </div>

    <div class="arsenal-tabs">
      <button class="arsenal-tab-btn active" data-split="overall">Overall</button>
      ${vsL.length ? `<button class="arsenal-tab-btn" data-split="vs_l">vs LHB</button>` : ""}
      ${vsR.length ? `<button class="arsenal-tab-btn" data-split="vs_r">vs RHB</button>` : ""}
    </div>
    <div class="arsenal-table-wrap">${arsenalTable(arsenal)}</div>
  </div>
  `;
}

function arsenalTable(arsenal) {
  if (!arsenal.length) return `<div class="muted" style="padding:8px;font-size:0.8rem;">No pitch data available.</div>`;
  const hasStuff = arsenal.some(p => p.pitch_quality != null);
  return `
  <table class="arsenal-table">
    <thead>
      <tr>
        <th>Pitch</th>
        <th>Use%</th>
        ${hasStuff ? "<th>Stuff+</th><th>Whiff%</th>" : ""}
        <th>xBA</th>
        <th>K%</th>
        <th>BB%</th>
        <th>RV/100</th>
      </tr>
    </thead>
    <tbody>
      ${arsenal.map(p=>`
      <tr>
        <td><span class="pitch-type-badge" title="${p.pitch_name||p.pitch_type}">${p.pitch_type}</span></td>
        <td>${fmt.pct(p.usage_pct,0)}</td>
        ${hasStuff ? `
        <td class="${stuffCellClass(p.pitch_quality)}">${p.pitch_quality != null ? p.pitch_quality : "—"}</td>
        <td>${p.whiff_pct != null ? p.whiff_pct.toFixed(1)+"%" : "—"}</td>
        ` : ""}
        <td class="${valClass(p.xba,false,0.240,0.270)}">${fmt.num(p.xba,3)}</td>
        <td class="${valClass(p.k_pct,true,0.22,0.18)}">${fmt.pct(p.k_pct)}</td>
        <td class="${valClass(p.bb_pct,false,0.08,0.13)}">${fmt.pct(p.bb_pct)}</td>
        <td class="${p.run_value_per_100!=null?(+(p.run_value_per_100)<=0?"val-good":"val-bad"):(+(p.run_value||0)>=0?"val-good":"val-bad")}">${p.run_value_per_100!=null?fmt.sign(p.run_value_per_100,1):fmt.sign(p.run_value,3)}</td>
      </tr>
      `).join("")}
    </tbody>
  </table>
  `;
}

/* ── Team / Batter Section ── */
function renderTeamSection(lineupCard) {
  if (!lineupCard) return "";
  const players = (lineupCard.players || []).slice().sort((a, b) => (b.matchup_score || 0) - (a.matchup_score || 0));
  return `
  <div class="team-section">
    <div class="team-head">
      <div>
        <div class="team-name">${lineupCard.team}</div>
        <div class="team-label">${lineupCard.label}</div>
      </div>
      ${!lineupCard.confirmed ? `<span class="badge badge-pass">Projected</span>` : `<span class="badge badge-strong">Confirmed</span>`}
    </div>
    <div class="player-list">
      ${players.map(renderPlayerRow).join("")}
    </div>
  </div>
  `;
}

function matchupBlurb(player) {
  const pm = player.pitch_matchup || {};
  const xwoba = pm.matchup_xwoba;
  if (!xwoba) return "";
  const kRisk = pm.matchup_k_risk   || 0;
  const bbUp  = pm.matchup_bb_upside || 0;
  const hh    = pm.matchup_hard_hit_pct || 0;
  const pHand = player.pitcher_hand || "?";
  const bHand = player.handedness   || "?";
  const hasPlatoon = bHand !== "?" && pHand !== "?" && bHand !== pHand;

  let label, cls;
  if      (xwoba >= 0.370) { label = "Strong batter advantage"; cls = "blurb-strong"; }
  else if (xwoba >= 0.340) { label = "Slight batter edge";      cls = "blurb-edge";   }
  else if (xwoba <= 0.270) { label = "Pitcher dominant";        cls = "blurb-weak";   }
  else if (xwoba <= 0.295) { label = "Slight pitcher edge";     cls = "blurb-pitcher";}
  else                     { label = "Neutral matchup";         cls = "blurb-neutral";}

  const bits = [];
  if (hasPlatoon)       bits.push(`${bHand} vs ${pHand}HP platoon advantage`);
  if (hh >= 0.50)       bits.push(`elite hard contact (${fmt.pct(hh,0)} HH)`);
  else if (hh >= 0.42)  bits.push(`above-avg hard contact (${fmt.pct(hh,0)} HH)`);
  else if (hh > 0 && hh <= 0.28) bits.push(`weak contact (${fmt.pct(hh,0)} HH)`);
  if (kRisk >= 0.30)    bits.push(`high K risk (${fmt.pct(kRisk,0)} K)`);
  else if (kRisk > 0 && kRisk <= 0.17) bits.push(`low K risk (${fmt.pct(kRisk,0)} K)`);
  if (bbUp >= 0.12)     bits.push(`strong walk upside (${fmt.pct(bbUp,0)} BB)`);

  const detail = bits.length ? ` · ${bits.join(", ")}` : "";
  return `<div class="matchup-blurb"><span class="blurb-label ${cls}">${label}</span><span class="blurb-detail">${fmt.num(xwoba,3)} xwOBA vs pitch mix${detail}.</span></div>`;
}

function renderPitchVsStarter(player) {
  const pitches = player.pitch_vs_starter || [];
  if (!pitches.length) {
    const tags = (player.pitch_scores || []).slice(0, 3).map(ps => {
      return `<span class="pitch-match-tag ${pitchMatchClass(ps.pitch_score)}">${ps.pitch_type} ${fmt.num(ps.pitch_score, 0)}</span>`;
    }).join("");
    return tags ? `<div class="pitch-matches">${tags}</div>` : "";
  }
  const rows = pitches.map(p => {
    const xwobaCls = p.batter_xwoba != null ? valClass(p.batter_xwoba, true, 0.330, 0.290) : "";
    const kCls = p.batter_k_pct != null ? (p.batter_k_pct >= 0.28 ? "val-bad" : p.batter_k_pct <= 0.18 ? "val-good" : "") : "";
    return `
    <div class="pitch-vs-row">
      <span class="pitch-type-badge">${p.pitch_type}</span>
      <span class="pitch-vs-use">${fmt.pct(p.usage_pct, 0)}</span>
      ${p.has_batter_data ? `
        <span class="pitch-vs-stat"><span class="pv-label">xwOBA</span> <span class="pv-val ${xwobaCls}">${fmt.num(p.batter_xwoba, 3)}</span></span>
        <span class="pitch-vs-stat"><span class="pv-label">K%</span> <span class="pv-val ${kCls}">${fmt.pct(p.batter_k_pct)}</span></span>
        <span class="pitch-vs-stat"><span class="pv-label">BB%</span> <span class="pv-val">${fmt.pct(p.batter_bb_pct)}</span></span>
      ` : `
        <span class="pv-no-data">no batter data · P xBA ${fmt.num(p.pitcher_xba, 3)} · P K% ${fmt.pct(p.pitcher_k_pct)}</span>
      `}
    </div>`;
  }).join("");
  return `<div class="pitch-vs-wrap"><div class="pitch-vs-header">vs starter's top pitches</div>${rows}</div>`;
}

function renderPlayerRow(player) {
  const sim = player.simulation || {};
  const pitchScores = player.pitch_scores || player.best_pitch_matches || [];
  const hand = player.handedness || "?";
  const pHand = player.pitcher_hand || "?";
  const platoonNote = hand !== "?" && pHand !== "?" ? (hand !== pHand ? "opp-hand" : "same-hand") : "";
  const platoonBadge = platoonNote === "opp-hand"
    ? `<span class="badge badge-strong" title="Opposite hand matchup">OPP</span>`
    : platoonNote === "same-hand"
      ? `<span class="badge badge-pass" title="Same hand matchup">SAME</span>`
      : "";

  const topPitchTags = pitchScores.slice(0,3).map(ps => {
    const cls = pitchMatchClass(ps.pitch_score);
    return `<span class="pitch-match-tag ${cls}">${ps.pitch_type} ${fmt.num(ps.pitch_score,0)}</span>`;
  }).join("");

  const hasSim = sim.pa > 0;

  // K% coloring: high K is bad for batter
  const kCls  = player.k_pct  == null ? "" : player.k_pct  >= 0.28 ? "val-bad"  : player.k_pct  <= 0.18 ? "val-good" : "";
  const bbCls = player.bb_pct == null ? "" : player.bb_pct >= 0.12  ? "val-good" : player.bb_pct <= 0.06  ? "val-bad"  : "";
  const hhCls = player.hard_hit_pct == null ? "" : player.hard_hit_pct >= 0.42 ? "val-good" : player.hard_hit_pct <= 0.30 ? "val-bad" : "";
  const srcTag = player.has_pitch_matchup
    ? `<span class="player-src-tag">vs arsenal</span>`
    : `<span class="player-src-tag player-src-season">season avg</span>`;

  return `
  <article class="player-row">
    <div class="player-header">
      <div class="player-name-block">
        <span class="player-slot">${player.slot}</span>
        <span class="player-name">${player.name}</span>
        <span class="badge ${hand==="L"?"badge-lhb":"badge-rhb"}">${hand}</span>
        ${platoonBadge}
        ${srcTag}
      </div>
      ${matchupScoreBadge(player.matchup_score)}
    </div>

    <div class="player-stats">
      <div class="player-stat">
        <div class="ps-label">xwOBA</div>
        <div class="ps-val ${valClass(player.xwoba,true,0.330,0.290)}">${fmt.num(player.xwoba,3)}</div>
      </div>
      <div class="player-stat">
        <div class="ps-label">HH%</div>
        <div class="ps-val ${hhCls}">${fmt.pct(player.hard_hit_pct)}</div>
      </div>
      <div class="player-stat">
        <div class="ps-label">K%</div>
        <div class="ps-val ${kCls}">${fmt.pct(player.k_pct)}</div>
      </div>
      <div class="player-stat">
        <div class="ps-label">BB%</div>
        <div class="ps-val ${bbCls}">${fmt.pct(player.bb_pct)}</div>
      </div>
    </div>

    ${renderPitchVsStarter(player)}

    ${matchupBlurb(player)}

    ${hasSim ? `
    <div class="sim-row">
      <div class="sim-divider"></div>
      <div class="sim-stat sim-featured"><div class="sim-lbl">R</div><div class="sim-val">${fmt.num(sim.runs,2)}</div></div>
      <div class="sim-stat sim-featured"><div class="sim-lbl">RBI</div><div class="sim-val">${fmt.num(sim.rbi,2)}</div></div>
      <div class="sim-stat sim-featured"><div class="sim-lbl">H</div><div class="sim-val">${fmt.num(sim.hits,2)}</div></div>
      <div class="sim-divider"></div>
      <div class="sim-stat"><div class="sim-lbl">HR</div><div class="sim-val">${fmt.num(sim.hr,2)}</div></div>
      <div class="sim-stat"><div class="sim-lbl">TB</div><div class="sim-val">${fmt.num(sim.tb,2)}</div></div>
      <div class="sim-stat"><div class="sim-lbl">BB</div><div class="sim-val">${fmt.num(sim.bb,2)}</div></div>
      <div class="sim-stat"><div class="sim-lbl">K</div><div class="sim-val">${fmt.num(sim.k,2)}</div></div>
      <div class="sim-stat"><div class="sim-lbl">PA</div><div class="sim-val">${fmt.num(sim.pa,2)}</div></div>
      <div class="sim-divider"></div>
      <div class="sim-stat"><div class="sim-lbl">H%</div><div class="sim-val">${fmt.pct(sim.hit_prob,0)}</div></div>
      <div class="sim-stat"><div class="sim-lbl">HR%</div><div class="sim-val">${fmt.pct(sim.hr_prob,0)}</div></div>
    </div>
    ` : ""}
  </article>
  `;
}

/* ── History Table ── */
function renderHistory(history) {
  const $summary = document.getElementById("history-summary");
  if (!history.length) {
    $historyTbl.innerHTML = `<tr><td colspan="8" class="empty-state">No graded picks yet — check back after games complete.</td></tr>`;
    if ($summary) $summary.innerHTML = "";
    return;
  }

  // Summary strip
  const graded = history.filter(p => p.result && p.result !== "pending" && p.result !== "no_result");
  const wins   = graded.filter(p => p.result === "win").length;
  const losses = graded.filter(p => p.result === "loss").length;
  const pushes = graded.filter(p => p.result === "push").length;
  const totalPnl = graded.reduce((s, p) => s + (p.pnl || 0), 0);
  const roi = graded.length ? totalPnl / (graded.length * 100) : 0;
  const pnlCls = totalPnl >= 0 ? "val-good" : "val-bad";

  if ($summary) {
    $summary.innerHTML = `
      <div class="history-stat"><span class="hs-val">${graded.length}</span><span class="hs-lbl">Graded</span></div>
      <div class="history-stat"><span class="hs-val val-good">${wins}</span><span class="hs-lbl">Wins</span></div>
      <div class="history-stat"><span class="hs-val val-bad">${losses}</span><span class="hs-lbl">Losses</span></div>
      <div class="history-stat"><span class="hs-val">${pushes}</span><span class="hs-lbl">Pushes</span></div>
      <div class="history-stat"><span class="hs-val ${pnlCls}">${totalPnl>=0?"+":""}$${totalPnl.toFixed(2)}</span><span class="hs-lbl">Total P&L</span></div>
      <div class="history-stat"><span class="hs-val ${pnlCls}">${roi>=0?"+":""}${(roi*100).toFixed(1)}%</span><span class="hs-lbl">ROI</span></div>
    `;
  }

  $historyTbl.innerHTML = history.map(p => {
    const result = p.result || "pending";
    const resultCls = result==="win"?"result-win": result==="loss"?"result-loss": result==="push"?"result-push":"result-open";
    const pnlStr = p.pnl == null ? "—" : `${p.pnl>=0?"+":""}$${(+p.pnl).toFixed(2)}`;
    const pnlCls2 = p.pnl == null ? "" : p.pnl > 0 ? "val-good" : p.pnl < 0 ? "val-bad" : "";
    const dateStr = p.date || (p.placed_at||"").slice(0,10);
    const pickSide = p.pick || p.pick_side || "—";
    const line = p.line != null ? ` ${p.line}` : "";
    return `
    <tr>
      <td>${dateStr}</td>
      <td>${p.matchup||"—"}</td>
      <td>${fmtMarketType(p.market_type)}</td>
      <td>${pickSide}${line}</td>
      <td>${fmt.odds(p.american_odds)}</td>
      <td>${fmt.pctS(p.edge)}</td>
      <td class="${resultCls}">${result}</td>
      <td class="${pnlCls2}">${pnlStr}</td>
    </tr>
    `;
  }).join("");
}

/* ── Skipped ── */
function renderSkipped(skipped) {
  if (!skipped.length) {
    $skippedList.innerHTML = `<li>No skipped games.</li>`;
    return;
  }
  $skippedList.innerHTML = skipped.map(s=>`<li><strong>${s.matchup}</strong>: ${s.reason}</li>`).join("");
}

/* ── Main ── */
async function loadBoard() {
  try {
    const payload = await fetchPayload();
    currentCards = payload.daily.lineup_cards || [];
    renderHero(payload);
    renderSummary(payload.summary || {});
    renderPicks(payload.daily.picks || []);
    if ($historyTbl) renderHistory(payload.history || []);
    renderGames(currentCards);

    // after DOM is built, store pitcher references for arsenal tab switching
    document.querySelectorAll("[data-pitcher-panel]").forEach(panel => {
      const side = panel.dataset.pitcherPanel;
      const card = currentCards.find(c=>c.matchup===activeMatchup);
      if (card) panel._pitcher = side==="home" ? card.home_pitcher : card.away_pitcher;
    });
  } catch (err) {
    console.error("loadBoard failed:", err);
  }
}

/* ── Sidebar Navigation ── */
document.querySelectorAll(".nav-item[data-section]").forEach(link => {
  link.addEventListener("click", e => {
    e.preventDefault();
    const section = link.dataset.section;
    document.querySelectorAll(".nav-item").forEach(l => l.classList.remove("active"));
    link.classList.add("active");
    const titleEl = document.getElementById("page-title");
    const titles = { dashboard:"Dashboard", gameboard:"Game Board", archive:"Daily Archive", history:"History" };
    if (titleEl) titleEl.textContent = titles[section] || section;

    // Show/hide summary grid
    const sg = document.getElementById("summary-grid");
    if (sg) sg.style.display = (section === "dashboard") ? "" : "none";

    // Show/hide sections
    ["dashboard","gameboard","archive","history"].forEach(id => {
      const el = document.getElementById(`section-${id}`);
      if (el) el.classList.toggle("hidden", id !== section);
    });

    if (section === "archive") loadArchive();
  });
});

/* ── Daily Archive ── */
let archiveLoaded = false;

async function loadArchive() {
  if (archiveLoaded) return;
  archiveLoaded = true;
  const $dates = document.getElementById("archive-dates");
  const $picks = document.getElementById("archive-picks");
  if (!$dates) return;

  try {
    const r = await fetch(`data/archive_index.json?ts=${Date.now()}`);
    const idx = await r.json();
    const dates = (idx.dates || []).filter(d => d.picks > 0);

    if (!dates.length) {
      $dates.innerHTML = `<div class="empty-state">No archived days yet.</div>`;
      return;
    }

    $dates.innerHTML = dates.map(d => `
      <button class="archive-date-btn" data-date="${d.date}">
        <div class="archive-date-label">${fmt.date(d.date)}</div>
        <div class="archive-date-meta">
          <span class="archive-picks-count">${d.picks} pick${d.picks!==1?"s":""}</span>
          ${d.strong ? `<span class="badge badge-strong">${d.strong} strong</span>` : ""}
          <span class="archive-games-count">${d.games} games</span>
        </div>
      </button>
    `).join("");

    $dates.querySelectorAll(".archive-date-btn").forEach(btn => {
      btn.addEventListener("click", () => {
        $dates.querySelectorAll(".archive-date-btn").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        loadArchiveDay(btn.dataset.date, $picks);
      });
    });

    // Auto-load the most recent day
    $dates.querySelector(".archive-date-btn")?.click();
  } catch (err) {
    $dates.innerHTML = `<div class="empty-state">Could not load archive.</div>`;
  }
}

async function loadArchiveDay(dateStr, $picks) {
  $picks.innerHTML = `<div class="empty-state">Loading…</div>`;
  try {
    const r = await fetch(`data/${dateStr}.json?ts=${Date.now()}`);
    const payload = await r.json();
    const picks = payload.daily?.picks || [];
    if (!picks.length) {
      $picks.innerHTML = `<div class="empty-state">No picks recorded for ${dateStr}.</div>`;
      return;
    }
    $picks.innerHTML = `
      <div class="archive-picks-header">
        <span class="archive-picks-title">Top picks — ${fmt.date(dateStr)}</span>
        <span class="archive-picks-sub">${picks.length} model pick${picks.length!==1?"s":""} · ${(+(picks[0]?.simulation_trials||0)).toLocaleString()} trials</span>
      </div>
      <div class="picks-grid">${picks.map((pick, i) => {
        const tier = (pick.tier||"").toLowerCase();
        return `
        <article class="pick-card tier-${tier}">
          <div class="pick-header">
            <div class="pick-rank">#${i+1}</div>
            <div class="pick-main">
              <div class="pick-matchup">${pick.matchup}</div>
              <div class="pick-meta">
                ${tierBadge(pick.tier)}
                <span class="pick-market">${fmtMarketType(pick.market_type)} · ${pick.pick} ${pick.line??""} · ${pick.start_time??"TBD"}</span>
              </div>
            </div>
            <div class="pick-right">
              <div class="edge-value">${fmt.pctS(pick.edge)}</div>
              <div class="pick-odds-line">${fmt.odds(pick.american_odds)}</div>
            </div>
          </div>
          <div class="pick-body">
            <div class="pick-stat"><div class="label">Model %</div><div class="val">${fmt.pct(pick.model_probability)}</div></div>
            <div class="pick-stat"><div class="label">No-vig %</div><div class="val">${fmt.pct(pick.no_vig_probability)}</div></div>
            <div class="pick-stat"><div class="label">Edge</div><div class="val" style="color:${tier==="strong"?"var(--teal)":tier==="moderate"?"var(--gold)":"var(--blue)"}">${fmt.pctS(pick.edge)}</div></div>
            <div class="pick-stat"><div class="label">Stake</div><div class="val">${fmt.pct(pick.bankroll_fraction)}</div></div>
            <div class="pick-stat"><div class="label">Lineup</div><div class="val">${pick.lineup_status??"—"}</div></div>
          </div>
          ${pick.reasoning ? `<div class="pick-reasoning">${pick.reasoning}</div>` : ""}
        </article>`;
      }).join("")}</div>`;
  } catch (err) {
    $picks.innerHTML = `<div class="empty-state">Could not load picks for ${dateStr}.</div>`;
  }
}

loadBoard();
setInterval(loadBoard, 15 * 60 * 1000);
