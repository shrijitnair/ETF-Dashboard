const RANGE_OPTIONS = [
  { key: "1M", months: 1 },
  { key: "3M", months: 3 },
  { key: "6M", months: 6 },
  { key: "1Y", years: 1 },
  { key: "3Y", years: 3 },
  { key: "5Y", years: 5 },
];
const DATA_REFRESH_INTERVAL_MS = 10 * 60 * 1000;
const INDEX_COMPARISON_TAB_ID = "index-performance";
const INDEX_COMPARISON_SERIES = [
  { group: "indices", ticker: "NIFTY50", label: "Nifty 50", color: "#f5b84b" },
  { group: "indices", ticker: "NIFTYNEXT50", label: "Nifty Next 50", color: "#48c8f0" },
  { group: "indices", ticker: "NIFTYMIDCAP150", label: "Nifty Midcap 150", color: "#a78bfa" },
  { group: "indices", ticker: "SP500", label: "S&P 500", color: "#52d39b" },
  { group: "indices", ticker: "NASDAQ100", label: "NASDAQ-100", color: "#ff7f9f" },
  { group: "commodities", ticker: "GOLD", label: "Gold / oz", color: "#fde047" },
  { group: "commodities", ticker: "SILVER", label: "Silver / oz", color: "#cbd5e1" },
  { group: "commodities", ticker: "BRENT", label: "Brent / bbl", color: "#2dd4bf" },
];
const INDEX_COMPARISON_RANGES = [
  { key: "1W", label: "1 week", days: 7 },
  { key: "1M", label: "1 month", months: 1 },
  { key: "1Y", label: "1 year", years: 1 },
  { key: "YTD", label: "Year to date", ytd: true },
];
const SAVED_THEME = window.localStorage.getItem("dashboard-theme") || "dark";
document.documentElement.dataset.theme = SAVED_THEME;

const state = {
  dashboard: null,
  snapshot: null,
  history: null,
  meta: null,
  activeTabId: null,
  sortStateByTab: {},
  selectedItemByTab: {},
  chartRangeByTab: {},
  comparisonRange: "1Y",
  filterText: "",
  displayCurrency: "INR",
  theme: SAVED_THEME,
};

const elements = {
  tablist: document.getElementById("tablist"),
  tablePanel: document.getElementById("table-panel"),
  comparisonPanel: document.getElementById("comparison-panel"),
  dashboardLayout: document.querySelector(".dashboard"),
  detailPanel: document.getElementById("detail-panel"),
  builtAt: document.getElementById("built-at"),
  instrumentCount: document.getElementById("instrument-count"),
  detailTabLabel: document.getElementById("detail-tab-label"),
  detailTitle: document.getElementById("detail-title"),
  detailSubtitle: document.getElementById("detail-subtitle"),
  chartLast: document.getElementById("chart-last"),
  chartChange: document.getElementById("chart-change"),
  chartStart: document.getElementById("chart-start"),
  chartEnd: document.getElementById("chart-end"),
  chart: document.getElementById("price-chart"),
  detailStats: document.getElementById("detail-stats"),
  rangeToggle: document.getElementById("range-toggle"),
  searchInput: document.getElementById("search-input"),
  currencySelect: document.getElementById("currency-select"),
  mobileSortSelect: document.getElementById("mobile-sort-select"),
  mobileSortDirection: document.getElementById("mobile-sort-direction"),
  emptyTemplate: document.getElementById("empty-state-template"),
  returnBasisNote: document.getElementById("return-basis-note"),
  chartChangeLabel: document.getElementById("chart-change-label"),
  themeToggle: document.getElementById("theme-toggle"),
};

init();

async function init() {
  try {
    bindPersistentEvents();
    await reloadDashboard();
    window.setInterval(() => {
      reloadDashboard().catch(() => {
        // Keep the last successful snapshot visible if a refresh fails.
      });
    }, DATA_REFRESH_INTERVAL_MS);
  } catch (error) {
    renderAppError(error.message || "Failed to load dashboard data.");
  }
}

function bindPersistentEvents() {
  elements.themeToggle.addEventListener("click", () => {
    state.theme = state.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = state.theme;
    window.localStorage.setItem("dashboard-theme", state.theme);
    updateThemeToggle();
  });
  elements.currencySelect.addEventListener("change", (event) => {
    state.displayCurrency = event.target.value;
    render();
  });
  elements.searchInput.addEventListener("input", (event) => {
    state.filterText = event.target.value.trim().toLowerCase();
    syncSelectedRow();
    render();
  });
  elements.mobileSortSelect.addEventListener("change", (event) => {
    const tab = getActiveTab();
    if (!tab) return;
    const key = event.target.value;
    state.sortStateByTab[tab.id] = {
      key,
      direction: key === "ticker" || key === "name" ? "asc" : "desc",
    };
    renderTable();
  });
  elements.mobileSortDirection.addEventListener("click", () => {
    const tab = getActiveTab();
    if (!tab) return;
    const sortState = state.sortStateByTab[tab.id];
    sortState.direction = sortState.direction === "asc" ? "desc" : "asc";
    renderTable();
  });
  updateThemeToggle();
}

function updateThemeToggle() {
  const nextTheme = state.theme === "dark" ? "light" : "dark";
  elements.themeToggle.textContent = `${nextTheme[0].toUpperCase()}${nextTheme.slice(1)} theme`;
  elements.themeToggle.setAttribute("aria-label", `Switch to ${nextTheme} theme`);
}

async function reloadDashboard(options = {}) {
  const previousTabId = options.preferredTabId || state.activeTabId;
  const dashboard = await fetchJson("data/dashboard.json");

  state.dashboard = dashboard;
  state.snapshot = dashboard.snapshot;
  state.history = dashboard.history;
  state.meta = dashboard.meta;

  const nextTabId = previousTabId === INDEX_COMPARISON_TAB_ID || state.snapshot.tabs.some((tab) => tab.id === previousTabId)
    ? previousTabId
    : state.snapshot.tabs[0]?.id || null;
  state.activeTabId = nextTabId;

  state.snapshot.tabs.forEach((tab) => {
    if (!state.sortStateByTab[tab.id]) {
      state.sortStateByTab[tab.id] = tab.asset_type === "etf"
        ? { key: "aum", direction: "desc" }
        : { key: "three_month_pct", direction: "desc" };
    }
    if (!state.chartRangeByTab[tab.id]) {
      state.chartRangeByTab[tab.id] = "1Y";
    }
    if (!state.selectedItemByTab[tab.id]) {
      state.selectedItemByTab[tab.id] = getFirstRowForTab(tab)?.item_id || null;
    }
  });

  if (options.preferredItemId && state.activeTabId) {
    state.selectedItemByTab[state.activeTabId] = options.preferredItemId;
  }

  syncSelectedRow();
  render();
}

function renderAppError(message) {
  elements.tablePanel.innerHTML = `
    <div class="empty-state">
      <h3>Failed to load dashboard</h3>
      <p>${escapeHtml(message)}</p>
    </div>
  `;
}

function render() {
  if (!state.snapshot) {
    return;
  }
  renderHeader();
  renderTabs();
  const isComparisonTab = state.activeTabId === INDEX_COMPARISON_TAB_ID;
  elements.dashboardLayout.classList.toggle("comparison-mode", isComparisonTab);
  elements.tablePanel.hidden = isComparisonTab;
  elements.comparisonPanel.hidden = !isComparisonTab;
  elements.detailPanel.hidden = isComparisonTab;
  if (isComparisonTab) {
    renderIndexComparison();
    return;
  }
  renderReturnBasisUi();
  renderRangeButtons();
  renderTable();
  renderDetail();
}

function renderHeader() {
  const builtAt = state.dashboard?.built_at ? new Date(state.dashboard.built_at) : null;
  const count = state.snapshot?.tabs.reduce((sum, tab) => {
    return sum + tab.groups.reduce((groupSum, group) => groupSum + group.rows.length, 0);
  }, 0) || 0;

  elements.builtAt.textContent = builtAt ? formatDateTime(builtAt) : "--";
  elements.instrumentCount.textContent = String(count);
}

function renderTabs() {
  const activeTabId = state.activeTabId;
  const tabs = [
    ...state.snapshot.tabs,
    { id: INDEX_COMPARISON_TAB_ID, label: "Index Performance" },
  ];
  elements.tablist.innerHTML = tabs.map((tab) => {
    const isActive = tab.id === activeTabId;
    return `
      <button
        class="tab-button ${isActive ? "active" : ""}"
        data-tab-id="${tab.id}"
        role="tab"
        aria-selected="${isActive}"
        type="button"
      >
        ${escapeHtml(tab.label)}
      </button>
    `;
  }).join("");

  elements.tablist.querySelectorAll("[data-tab-id]").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeTabId = button.dataset.tabId;
      syncSelectedRow();
      render();
    });
  });
}

function renderReturnBasisUi() {
  const returnBasisLabel = state.displayCurrency === "INR" ? "INR-adjusted" : "USD";
  const tab = getActiveTab();
  const sourceNote = tab?.asset_type === "index"
    ? " Yahoo Finance data may be delayed."
    : tab?.asset_type === "mutual_fund"
      ? " Mutual fund NAVs update once per business day."
      : "";
  elements.returnBasisNote.textContent = `Returns shown in ${returnBasisLabel} performance.${sourceNote}`;
  elements.chartChangeLabel.textContent = tab?.asset_type === "mutual_fund"
    ? "Range Change"
    : `Range Change (${state.displayCurrency})`;
}

function renderRangeButtons() {
  const tabId = state.activeTabId;
  const activeRange = state.chartRangeByTab[tabId];
  elements.rangeToggle.innerHTML = RANGE_OPTIONS.map((range) => `
    <button
      type="button"
      class="range-button ${range.key === activeRange ? "active" : ""}"
      data-range="${range.key}"
    >
      ${range.key}
    </button>
  `).join("");

  elements.rangeToggle.querySelectorAll("[data-range]").forEach((button) => {
    button.addEventListener("click", () => {
      state.chartRangeByTab[tabId] = button.dataset.range;
      renderRangeButtons();
      renderDetail();
    });
  });
}

function renderIndexComparison() {
  const range = INDEX_COMPARISON_RANGES.find((entry) => entry.key === state.comparisonRange) || INDEX_COMPARISON_RANGES[2];
  const sections = [
    { id: "indices", title: "Equity indices" },
    { id: "commodities", title: "Commodities" },
  ].map((section) => ({
    ...section,
    comparison: getIndexComparisonData(range, INDEX_COMPARISON_SERIES.filter((entry) => entry.group === section.id)),
  }));
  const currency = state.displayCurrency;
  elements.returnBasisNote.textContent = `Indexed performance comparison • ${currency === "INR" ? "INR-adjusted" : "USD"} returns`;

  if (sections.some((section) => !section.comparison.series.length)) {
    elements.comparisonPanel.innerHTML = `
      <div class="comparison-empty">
        <span class="comparison-eyebrow">Market comparison</span>
        <h2>Index data is unavailable</h2>
        <p>Refresh the dashboard data and try again.</p>
      </div>
    `;
    return;
  }

  elements.comparisonPanel.innerHTML = `
    <div class="comparison-heading">
      <div>
        <span class="comparison-eyebrow">Market comparison · ${escapeHtml(currency)}</span>
        <h2>How markets and commodities moved</h2>
        <p>Each line starts at 100, so percentage performance is easy to compare.</p>
      </div>
      <div class="comparison-ranges" role="group" aria-label="Comparison period">
        ${INDEX_COMPARISON_RANGES.map((option) => `
          <button
            class="comparison-range-button ${option.key === range.key ? "active" : ""}"
            type="button"
            data-comparison-range="${option.key}"
            aria-pressed="${option.key === range.key}"
          >${option.key}</button>
        `).join("")}
      </div>
    </div>
    ${sections.map((section) => renderComparisonSection(section, range)).join("")}
    <p class="comparison-footnote">NASDAQ-100 is used for the Nasdaq line. Gold and silver are futures per troy ounce; Brent is futures per barrel, so returns can differ from spot prices. Cross-market returns are converted to ${escapeHtml(currency)}.</p>
  `;

  elements.comparisonPanel.querySelectorAll("[data-comparison-range]").forEach((button) => {
    button.addEventListener("click", () => {
      state.comparisonRange = button.dataset.comparisonRange;
      renderIndexComparison();
    });
  });
}

function renderComparisonSection(section, range) {
  const { comparison } = section;
  const zoomFactor = section.id === "indices" ? 0.5 : 1;
  return `
    <section class="comparison-section" aria-label="${escapeHtml(section.title)}">
      <h3>${escapeHtml(section.title)}</h3>
      <div class="comparison-legend" aria-label="Latest performance by ${escapeHtml(section.title.toLowerCase())}">
        ${comparison.series.map((entry) => `
          <div class="comparison-legend-item">
            <span class="comparison-legend-name"><i style="--series-color:${entry.color}"></i>${escapeHtml(entry.label)}</span>
            <strong class="${getChangeClass(entry.change)}">${escapeHtml(formatPercent(entry.change, true))}</strong>
          </div>
        `).join("")}
      </div>
      <div class="comparison-chart-card">
        <div class="comparison-chart-meta">
          <span>Indexed value <b>100 = period start</b>${zoomFactor < 1 ? " · Zoomed to half range; extremes may be clipped" : ""}</span>
          <span>${escapeHtml(range.label)} <i>·</i> through ${escapeHtml(comparison.endDate)}</span>
        </div>
        ${renderComparisonSvg(comparison.series, comparison.startDate, comparison.endDate, zoomFactor)}
      </div>
    </section>
  `;
}

function getIndexComparisonData(range, definitions = INDEX_COMPARISON_SERIES) {
  const indexTab = state.snapshot.tabs.find((tab) => tab.id === "indices");
  const rowsByTicker = new Map((indexTab?.groups || []).flatMap((group) => group.rows).map((row) => [row.ticker, row]));
  const rawSeries = definitions.map((definition) => {
    const row = rowsByTicker.get(definition.ticker);
    const history = row ? state.history.series[row.item_id]?.points || [] : [];
    const points = history.map((point) => ({
      date: point.date,
      value: state.displayCurrency === "INR" ? point.inr_close : point.close,
    })).filter((point) => point.value != null && Number.isFinite(Number(point.value)))
      .sort((left, right) => left.date.localeCompare(right.date));
    return { ...definition, points };
  });

  if (rawSeries.some((entry) => entry.points.length < 2)) {
    return { series: [], startDate: "", endDate: "" };
  }

  const endDate = rawSeries.reduce((latest, entry) => {
    const lastDate = entry.points[entry.points.length - 1].date;
    return lastDate < latest ? lastDate : latest;
  }, rawSeries[0].points[rawSeries[0].points.length - 1].date);
  const end = parseIsoDateUtc(endDate);
  let anchor;
  if (range.ytd) {
    anchor = new Date(Date.UTC(end.getUTCFullYear(), 0, 1));
  } else if (range.days) {
    anchor = new Date(end.getTime() - range.days * 24 * 60 * 60 * 1000);
  } else {
    anchor = subtractCalendarOffsetUtc(end, range);
  }
  const anchorDate = anchor.toISOString().slice(0, 10);

  const series = rawSeries.map((entry) => {
    const eligible = entry.points.filter((point) => point.date <= endDate);
    const baselineIndex = eligible.reduce((found, point, index) => {
      const isBaseline = range.ytd ? point.date < anchorDate : point.date <= anchorDate;
      return isBaseline ? index : found;
    }, -1);
    const firstAfterAnchor = eligible.findIndex((point) => point.date >= anchorDate);
    const basePoint = baselineIndex >= 0
      ? eligible[baselineIndex]
      : eligible[Math.max(firstAfterAnchor, 0)];
    if (!basePoint || !basePoint.value) return null;
    const periodPoints = eligible.filter((point) => point.date >= anchorDate && point.date <= endDate);
    const lastPoint = periodPoints[periodPoints.length - 1] || basePoint;
    const normalizedPoints = [
      { date: anchorDate, value: 100 },
      ...periodPoints.filter((point) => point.date > anchorDate).map((point) => ({
        date: point.date,
        value: (Number(point.value) / Number(basePoint.value)) * 100,
      })),
    ];
    return {
      ...entry,
      change: ((Number(lastPoint.value) / Number(basePoint.value)) - 1) * 100,
      points: normalizedPoints,
    };
  }).filter(Boolean);

  return { series, startDate: anchorDate, endDate };
}

function renderComparisonSvg(series, startDate, endDate, zoomFactor = 1) {
  const width = 1000;
  const height = 470;
  const padding = { top: 24, right: 28, bottom: 28, left: 76 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const startTime = parseIsoDateUtc(startDate).getTime();
  const endTime = parseIsoDateUtc(endDate).getTime();
  const timeSpan = Math.max(endTime - startTime, 1);
  const values = series.flatMap((entry) => entry.points.map((point) => point.value));
  const dataMin = Math.min(100, ...values);
  const dataMax = Math.max(100, ...values);
  const spread = Math.max(dataMax - dataMin, 1.5);
  const fullMin = dataMin - spread * 0.08;
  const fullMax = dataMax + spread * 0.08;
  const halfRange = ((fullMax - fullMin) * zoomFactor) / 2;
  const min = zoomFactor < 1 ? 100 - halfRange : fullMin;
  const max = zoomFactor < 1 ? 100 + halfRange : fullMax;
  const clipId = `comparison-plot-${series.map((entry) => entry.ticker).join("-").toLowerCase()}`;
  const yFor = (value) => padding.top + ((max - value) / (max - min)) * plotHeight;
  const xFor = (date) => padding.left + ((parseIsoDateUtc(date).getTime() - startTime) / timeSpan) * plotWidth;
  const ticks = Array.from({ length: 5 }, (_, index) => min + ((max - min) * index) / 4);
  const hasZeroTick = ticks.some((value) => Math.abs(value - 100) < 0.01);
  const xTicks = Array.from({ length: 4 }, (_, index) => {
    const ratio = index / 3;
    const date = new Date(startTime + timeSpan * ratio).toISOString().slice(0, 10);
    return { ratio, date };
  });

  return `
    <svg class="comparison-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Normalized performance of ${escapeHtml(series.map((entry) => entry.label).join(", "))}">
      <defs><clipPath id="${clipId}"><rect x="${padding.left}" y="${padding.top}" width="${plotWidth}" height="${plotHeight}"></rect></clipPath></defs>
      ${ticks.map((value) => {
        const y = yFor(value);
        const isZero = Math.abs(value - 100) < 0.01;
        return `
          <line class="${isZero ? "comparison-baseline" : "comparison-gridline"}" x1="${padding.left}" y1="${y.toFixed(1)}" x2="${width - padding.right}" y2="${y.toFixed(1)}"></line>
          <text class="comparison-y-label ${isZero ? "comparison-zero-label" : ""}" x="${padding.left - 12}" y="${(y + 4).toFixed(1)}" text-anchor="end">${isZero ? "0%" : `${value > 100 ? "+" : ""}${(value - 100).toFixed(0)}%`}</text>
        `;
      }).join("")}
      ${hasZeroTick ? "" : `
        <line class="comparison-baseline" x1="${padding.left}" y1="${yFor(100).toFixed(1)}" x2="${width - padding.right}" y2="${yFor(100).toFixed(1)}"></line>
        <text class="comparison-y-label comparison-zero-label" x="${padding.left - 12}" y="${(yFor(100) + 4).toFixed(1)}" text-anchor="end">0%</text>
      `}
      ${xTicks.map(({ ratio, date }) => {
        const x = padding.left + plotWidth * ratio;
        return `<line class="comparison-x-tick" x1="${x.toFixed(1)}" y1="${padding.top}" x2="${x.toFixed(1)}" y2="${height - padding.bottom}"></line>`;
      }).join("")}
      <g clip-path="url(#${clipId})">${series.map((entry) => {
        const points = entry.points.map((point) => ({ x: xFor(point.date), y: yFor(point.value) }));
        const end = points[points.length - 1];
        return `
          <path class="comparison-line comparison-line-glow" d="${smoothLinePath(points)}" stroke="${entry.color}"></path>
          <path class="comparison-line" d="${smoothLinePath(points)}" stroke="${entry.color}"></path>
          <circle class="comparison-endpoint-halo" cx="${end.x.toFixed(1)}" cy="${end.y.toFixed(1)}" r="8" fill="${entry.color}"></circle>
          <circle class="comparison-endpoint" cx="${end.x.toFixed(1)}" cy="${end.y.toFixed(1)}" r="3.5" fill="${entry.color}"></circle>
        `;
      }).join("")}</g>
      ${xTicks.map(({ ratio, date }) => {
        const x = padding.left + plotWidth * ratio;
        const label = ratio === 0 ? startDate : ratio === 1 ? endDate : date.slice(5);
        return `<text class="comparison-x-label" x="${x.toFixed(1)}" y="${height - 4}" text-anchor="${ratio === 0 ? "start" : ratio === 1 ? "end" : "middle"}">${escapeHtml(label)}</text>`;
      }).join("")}
    </svg>
  `;
}

function smoothLinePath(points) {
  if (!points.length) return "";
  if (points.length === 1) return `M ${points[0].x.toFixed(1)} ${points[0].y.toFixed(1)}`;
  let path = `M ${points[0].x.toFixed(1)} ${points[0].y.toFixed(1)}`;
  for (let index = 0; index < points.length - 1; index += 1) {
    const previous = points[Math.max(0, index - 1)];
    const current = points[index];
    const next = points[index + 1];
    const following = points[Math.min(points.length - 1, index + 2)];
    const controlOne = {
      x: current.x + (next.x - previous.x) / 6,
      y: current.y + (next.y - previous.y) / 6,
    };
    const controlTwo = {
      x: next.x - (following.x - current.x) / 6,
      y: next.y - (following.y - current.y) / 6,
    };
    path += ` C ${controlOne.x.toFixed(1)} ${controlOne.y.toFixed(1)}, ${controlTwo.x.toFixed(1)} ${controlTwo.y.toFixed(1)}, ${next.x.toFixed(1)} ${next.y.toFixed(1)}`;
  }
  return path;
}

function renderTable() {
  const tab = getActiveTab();
  if (!tab) {
    elements.tablePanel.innerHTML = "";
    return;
  }

  const columns = getColumnsForTab(tab);
  renderMobileSort(tab, columns);
  const visibleGroups = tab.groups.map((group) => ({
    ...group,
    rows: getSortedFilteredRows(tab, group.rows),
  })).filter((group) => group.rows.length > 0);

  if (!visibleGroups.length) {
    elements.tablePanel.innerHTML = "";
    elements.tablePanel.appendChild(elements.emptyTemplate.content.cloneNode(true));
    return;
  }

  const sortState = state.sortStateByTab[tab.id];
  const headerHtml = columns.map((column) => {
    const isSorted = sortState.key === column.key;
    const arrow = isSorted ? (sortState.direction === "asc" ? " ↑" : " ↓") : "";
    return `
      <th
        class="${column.sortable ? "sortable" : ""} ${isSorted ? "sorted" : ""}"
        data-sort-key="${column.sortable ? column.key : ""}"
      >
        ${escapeHtml(column.label)}${arrow}
      </th>
    `;
  }).join("");

  elements.tablePanel.innerHTML = visibleGroups.map((group) => `
    <section class="group-block">
      <div class="group-heading">
        <div>
          <span class="group-label">${escapeHtml(tab.label)}</span>
          <h3>${escapeHtml(group.label)}</h3>
        </div>
        <span class="subdued">${group.rows.length} instruments</span>
      </div>
      <div class="table-shell">
        <table>
          <thead>
            <tr>${headerHtml}</tr>
          </thead>
          <tbody>
            ${group.rows.map((row) => renderRow(row, columns, tab.id)).join("")}
          </tbody>
        </table>
      </div>
      <div class="mobile-cards">
        ${group.rows.map((row) => renderMobileCard(row, columns, tab.id)).join("")}
      </div>
    </section>
  `).join("");

  elements.tablePanel.querySelectorAll("[data-sort-key]").forEach((header) => {
    if (!header.dataset.sortKey) {
      return;
    }
    header.addEventListener("click", () => handleSort(tab.id, header.dataset.sortKey));
  });

  elements.tablePanel.querySelectorAll("[data-item-id]").forEach((row) => {
    row.addEventListener("click", () => {
      state.selectedItemByTab[tab.id] = row.dataset.itemId;
      renderTable();
      renderDetail();
    });
  });

  elements.tablePanel.querySelectorAll("[data-mobile-item-id]").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedItemByTab[tab.id] = button.dataset.mobileItemId;
      renderTable();
      renderDetail();
    });
  });
}

function renderMobileSort(tab, columns) {
  const sortState = state.sortStateByTab[tab.id];
  elements.mobileSortSelect.innerHTML = columns
    .filter((column) => column.sortable)
    .map((column) => `<option value="${escapeHtml(column.key)}">${escapeHtml(column.label)}</option>`)
    .join("");
  elements.mobileSortSelect.value = sortState.key;
  elements.mobileSortDirection.textContent = sortState.direction === "asc" ? "↑" : "↓";
  elements.mobileSortDirection.setAttribute(
    "aria-label",
    `Sort ${sortState.direction === "asc" ? "descending" : "ascending"}`,
  );
}

function renderMobileCard(row, columns, tabId) {
  const selected = state.selectedItemByTab[tabId] === row.item_id;
  const summaryKeys = new Set(["daily_pct", "five_day_pct", "one_month_pct", "ytd_pct", "one_year_pct"]);
  const summaryColumns = columns.filter((column) => summaryKeys.has(column.key));
  const detailColumns = columns.filter((column) => ![
    "ticker", "name", "last_price", ...summaryKeys,
  ].includes(column.key));
  const instrumentMeta = row.asset_type === "mutual_fund" || row.asset_type === "stock"
    ? ""
    : `${row.exchange}${row.currency ? ` • ${row.currency}` : ""}`;

  return `
    <article class="instrument-card ${selected ? "selected" : ""}">
      <button
        class="instrument-card-main"
        type="button"
        data-mobile-item-id="${escapeHtml(row.item_id)}"
        aria-pressed="${selected}"
      >
        <span class="instrument-card-heading">
          <span class="instrument-card-identity">
            ${row.asset_type === "mutual_fund" || row.asset_type === "stock" ? "" : `<span class="ticker-pill">${escapeHtml(row.ticker)}</span>`}
            <strong class="${row.asset_type === "stock" ? "stock-card-name" : ""}">${escapeHtml(getDisplayName(row))}</strong>
            ${instrumentMeta ? `<span class="subdued instrument-card-meta">${escapeHtml(instrumentMeta)}</span>` : ""}
          </span>
          <span class="instrument-card-price">
            <span class="metric-label">Last${row.asset_type === "mutual_fund" ? "" : ` ${escapeHtml(state.displayCurrency)}`}</span>
            <span>${renderCell(row, { key: "last_price" })}</span>
          </span>
        </span>
        <span class="mobile-metrics">
          ${summaryColumns.map((column) => {
            const value = getDisplayMetric(row, column.key);
            return `
              <span class="mobile-metric">
                <span>${escapeHtml(column.label)}</span>
                <strong class="${getChangeClass(value)}">${escapeHtml(formatPercent(value, true))}</strong>
              </span>
            `;
          }).join("")}
        </span>
      </button>
      <details class="instrument-card-more">
        <summary>More metrics and details</summary>
        <div class="mobile-detail-metrics">
          ${detailColumns.map((column) => `
            <span class="mobile-detail-metric">
              <span>${escapeHtml(column.label)}</span>
              <strong>${renderCell(row, column)}</strong>
            </span>
          `).join("")}
          ${row.asset_type === "mutual_fund" ? `
            <span class="mobile-detail-metric"><span>Exchange</span><strong>${escapeHtml(row.exchange || "N/A")}</strong></span>
            <span class="mobile-detail-metric"><span>Scheme code</span><strong>${escapeHtml(row.scheme_code || "N/A")}</strong></span>
            <span class="mobile-detail-metric"><span>NAV date</span><strong>${escapeHtml(row.data_date || "N/A")}</strong></span>
            <span class="mobile-detail-metric"><span>Data source</span><strong>${escapeHtml(row.data_source || "MFAPI / AMFI NAV")}</strong></span>
            <span class="mobile-detail-metric"><span>Status</span><strong>${escapeHtml(row.data_status || "Unknown")}</strong></span>
          ` : ""}
          ${row.asset_type === "index" ? `
            <span class="mobile-detail-metric"><span>Data source</span><strong>${escapeHtml(row.data_source || "Yahoo Finance via yfinance")}</strong></span>
            <span class="mobile-detail-metric"><span>Status</span><strong>${escapeHtml(row.data_status || "Unknown")}${row.data_date ? ` • ${escapeHtml(row.data_date)}` : ""}</strong></span>
          ` : ""}
        </div>
      </details>
    </article>
  `;
}

function renderRow(row, columns, tabId) {
  const isSelected = state.selectedItemByTab[tabId] === row.item_id;
  return `
    <tr class="${isSelected ? "active" : ""}" data-item-id="${row.item_id}">
      ${columns.map((column) => `<td class="${column.className || ""}">${renderCell(row, column)}</td>`).join("")}
    </tr>
  `;
}

function renderCell(row, column) {
  switch (column.key) {
    case "ticker":
      return `<span class="ticker-pill">${escapeHtml(row.ticker)}</span>`;
    case "name":
      return `
        <div class="name-cell ${row.asset_type === "stock" ? "stock-name-cell" : ""}">
          <strong>${escapeHtml(getDisplayName(row))}</strong>
          ${row.asset_type === "mutual_fund" || row.asset_type === "stock" ? "" : `<div class="subdued">${escapeHtml(row.exchange)}${row.currency ? " • " + escapeHtml(row.currency) : ""}</div>`}
        </div>
      `;
    case "last_price":
      return `<strong>${formatRowPrice(row)}</strong>`;
    case "aum":
      return row.aum ? `<span class="aum-pill">${escapeHtml(row.aum_display)}</span>` : `<span class="subdued">N/A</span>`;
    case "ter":
      return `<span>${escapeHtml(formatTer(row.ter))}</span>`;
    default:
      const value = getDisplayMetric(row, column.key);
      return `<span class="${getChangeClass(value)}">${escapeHtml(formatPercent(value, true))}</span>`;
  }
}

function renderDetail() {
  const tab = getActiveTab();
  const row = getSelectedRow(tab);
  if (!tab || !row) {
    elements.detailTabLabel.textContent = "No Selection";
    elements.detailTitle.textContent = "Market Snapshot";
    elements.detailSubtitle.textContent = "Select a row to inspect its chart and summary.";
    elements.detailStats.innerHTML = "";
    elements.chart.innerHTML = "";
    elements.chartLast.textContent = "--";
    elements.chartChange.textContent = "--";
    elements.chartStart.textContent = "--";
    elements.chartEnd.textContent = "--";
    return;
  }

  elements.detailTabLabel.textContent = tab.label;
  elements.detailTitle.textContent = row.asset_type === "stock" || row.asset_type === "mutual_fund"
    ? getDisplayName(row)
    : row.ticker;
  elements.detailSubtitle.textContent = row.asset_type === "mutual_fund"
    ? `ISIN ${row.isin} • Scheme ${row.scheme_code} • ${row.exchange}`
    : `${row.name} • ${row.exchange}${row.currency ? " • " + row.currency : ""}`;

  const series = state.history.series[row.item_id]?.points || [];
  const rangeKey = state.chartRangeByTab[tab.id];
  const rangeSlice = sliceSeriesByRange(series, rangeKey);
  const rangedSeries = rangeSlice.points;
  renderChart(rangedSeries);

  const startValue = rangeSlice.hasSufficientHistory ? getRangeChangeValue(rangedSeries[0]) : null;
  const endValue = rangeSlice.hasSufficientHistory ? getRangeChangeValue(rangedSeries[rangedSeries.length - 1]) : null;
  const rangeChange = (startValue != null && endValue != null && startValue !== 0)
    ? ((endValue / startValue) - 1) * 100
    : null;

  elements.chartLast.textContent = formatRowPrice(row);
  elements.chartChange.textContent = formatPercent(rangeChange, true);
  elements.chartChange.className = getChangeClass(rangeChange);
  elements.chartStart.textContent = rangedSeries[0]?.date || "--";
  elements.chartEnd.textContent = rangedSeries[rangedSeries.length - 1]?.date || "--";

  const stats = [
    makeMetricCard(row, "daily_pct", "1D"),
    makeMetricCard(row, "five_day_pct", "5D"),
    makeMetricCard(row, "one_month_pct", "1M"),
    makeMetricCard(row, "three_month_pct", "3M"),
    makeMetricCard(row, "ytd_pct", "YTD"),
    makeMetricCard(row, "one_year_pct", "1Y"),
    makeMetricCard(row, "three_year_pct", "3Y CAGR"),
    makeMetricCard(row, "five_year_pct", "5Y CAGR"),
    { label: "Asset Type", value: row.asset_type === "etf" ? "ETF" : row.asset_type === "index" ? "Index" : row.asset_type === "mutual_fund" ? "Mutual Fund" : "Stock", className: "" },
  ];

  if (row.asset_type === "index") {
    stats.push(makeMetricCard(row, "since_2024_09_26_pct", "Since 26 Sep 2024"));
  }

  if (row.asset_type === "etf") {
    stats.push({ label: "TER", value: formatTer(row.ter), className: "" });
    stats.push({ label: "AUM", value: row.aum_display || "N/A", className: "" });
  }

  if (row.asset_type === "index") {
    stats.push({ label: "Data Source", value: row.data_source || "Yahoo Finance via yfinance", className: "" });
    stats.push({ label: "Data Status", value: `${row.data_status || "Unknown"}${row.data_date ? ` • ${row.data_date}` : ""}`, className: "" });
  }

  if (row.asset_type === "mutual_fund") {
    stats.push({ label: "ISIN", value: row.isin || "N/A", className: "" });
    stats.push({ label: "Scheme Code", value: row.scheme_code || "N/A", className: "" });
    stats.push({ label: "NAV Date", value: row.data_date || "N/A", className: "" });
    stats.push({ label: "Data Source", value: row.data_source || "MFAPI / AMFI NAV", className: "" });
    stats.push({ label: "Data Status", value: row.data_status || "Unknown", className: "" });
  }

  elements.detailStats.innerHTML = stats.map((stat) => `
    <div class="stat-card">
      <span class="metric-label">${escapeHtml(stat.label)}</span>
      <strong class="${stat.className || ""}">${escapeHtml(stat.value)}</strong>
    </div>
  `).join("");
}

function makeMetricCard(row, key, fallbackLabel) {
  const value = getDisplayMetric(row, key);
  return {
    label: getMetricLabel(row, key, fallbackLabel),
    value: formatPercent(value, true),
    className: getChangeClass(value),
  };
}

function getDisplayMetric(row, key) {
  if (state.displayCurrency === "USD") {
    const usdKey = key.replace("_pct", "_usd_pct");
    return row[usdKey] ?? row[key];
  }
  return row[key];
}

function getDisplayLastPrice(row) {
  return state.displayCurrency === "INR"
    ? (row.last_price_inr ?? row.last_price)
    : row.last_price;
}

function renderChart(points) {
  if (!points.length) {
    elements.chart.innerHTML = "";
    return;
  }

  const width = 800;
  const height = 320;
  const padding = { top: 22, right: 18, bottom: 28, left: 18 };
  const values = points.map((point) => point.close);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(max - min, max * 0.02, 1);
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;

  const coords = points.map((point, index) => {
    const x = padding.left + (innerWidth * index) / Math.max(points.length - 1, 1);
    const normalized = (point.close - (min - span * 0.08)) / (span * 1.16);
    const y = height - padding.bottom - normalized * innerHeight;
    return { x, y };
  });

  const linePath = coords.map((coord, index) => `${index === 0 ? "M" : "L"} ${coord.x.toFixed(2)} ${coord.y.toFixed(2)}`).join(" ");
  const areaPath = `${linePath} L ${coords[coords.length - 1].x.toFixed(2)} ${(height - padding.bottom).toFixed(2)} L ${coords[0].x.toFixed(2)} ${(height - padding.bottom).toFixed(2)} Z`;
  const lastPoint = coords[coords.length - 1];

  elements.chart.innerHTML = `
    <defs>
      <linearGradient id="chartGradient" x1="0%" y1="0%" x2="0%" y2="100%">
        <stop offset="0%" stop-color="#f3b93f" stop-opacity="0.42"></stop>
        <stop offset="100%" stop-color="#f3b93f" stop-opacity="0.02"></stop>
      </linearGradient>
    </defs>
    <line class="chart-grid" x1="${padding.left}" y1="${padding.top}" x2="${width - padding.right}" y2="${padding.top}"></line>
    <line class="chart-grid" x1="${padding.left}" y1="${height / 2}" x2="${width - padding.right}" y2="${height / 2}"></line>
    <line class="chart-grid" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line>
    <path class="chart-area" d="${areaPath}"></path>
    <path class="chart-line" d="${linePath}"></path>
    <circle class="chart-dot" cx="${lastPoint.x.toFixed(2)}" cy="${lastPoint.y.toFixed(2)}" r="5"></circle>
  `;
}

function handleSort(tabId, key) {
  const current = state.sortStateByTab[tabId];
  if (current.key === key) {
    current.direction = current.direction === "asc" ? "desc" : "asc";
  } else {
    current.key = key;
    current.direction = key === "ticker" || key === "name" ? "asc" : "desc";
  }
  renderTable();
}

function getColumnsForTab(tab) {
  const metricLabel = (key, fallback) => tab.asset_type === "mutual_fund"
    ? getMetaColumnLabel(key, fallback).replace(/\s+(INR|USD)$/, "")
    : getCurrencyLabel(key, fallback);
  const columns = tab.asset_type === "mutual_fund"
    ? [
        { key: "name", label: "Scheme", sortable: true },
      ]
    : tab.asset_type === "stock"
      ? [{ key: "name", label: "Company", sortable: true }]
    : [{ key: "ticker", label: getMetaColumnLabel("ticker", "Ticker"), sortable: true, className: "ticker-cell" }];
  columns.push(
    { key: "last_price", label: tab.asset_type === "mutual_fund" ? "Last" : `Last ${state.displayCurrency}`, sortable: true },
    { key: "daily_pct", label: metricLabel("daily_pct", "1D"), sortable: true },
    { key: "five_day_pct", label: metricLabel("five_day_pct", "5D"), sortable: true },
    { key: "one_month_pct", label: metricLabel("one_month_pct", "1M"), sortable: true },
    { key: "three_month_pct", label: metricLabel("three_month_pct", "3M"), sortable: true },
    { key: "ytd_pct", label: metricLabel("ytd_pct", "YTD"), sortable: true },
    { key: "one_year_pct", label: metricLabel("one_year_pct", "1Y"), sortable: true },
    { key: "three_year_pct", label: metricLabel("three_year_pct", "3Y CAGR"), sortable: true },
    { key: "five_year_pct", label: metricLabel("five_year_pct", "5Y CAGR"), sortable: true },
  );

  if (tab.asset_type === "etf") {
    columns.push({ key: "ter", label: "TER", sortable: true });
    columns.push({ key: "aum", label: getMetaColumnLabel("aum", "AUM"), sortable: true });
  }
  if (tab.asset_type === "index") {
    columns.push({ key: "since_2024_09_26_pct", label: getCurrencyLabel("since_2024_09_26_pct", "Since 26 Sep 2024"), sortable: true });
  }
  return columns;
}

function getDisplayName(row) {
  if (row.asset_type !== "mutual_fund") {
    return row.name || row.ticker;
  }
  return (row.name || "")
    .replace(/\s*-\s*Direct Plan\b.*$/i, "")
    .replace(/\s+Fund$/i, "")
    .trim();
}

function getMetricLabel(row, key, fallbackLabel) {
  return row.asset_type === "mutual_fund"
    ? getMetaColumnLabel(key, fallbackLabel).replace(/\s+(INR|USD)$/, "")
    : getCurrencyLabel(key, fallbackLabel);
}

function getSortedFilteredRows(tab, rows) {
  const sortState = state.sortStateByTab[tab.id];
  const filteredRows = rows.filter((row) => {
    if (!state.filterText) {
      return true;
    }
    const haystack = `${row.ticker} ${row.name}`.toLowerCase();
    return haystack.includes(state.filterText);
  });

  return filteredRows.slice().sort((left, right) => compareRows(left, right, sortState));
}

function compareRows(left, right, sortState) {
  const leftValue = left[sortState.key];
  const rightValue = right[sortState.key];
  const direction = sortState.direction === "asc" ? 1 : -1;

  if (leftValue == null && rightValue == null) {
    return left.ticker.localeCompare(right.ticker);
  }
  if (leftValue == null) {
    return 1;
  }
  if (rightValue == null) {
    return -1;
  }

  if (typeof leftValue === "string" || typeof rightValue === "string") {
    return leftValue.toString().localeCompare(rightValue.toString()) * direction;
  }
  return (leftValue - rightValue) * direction;
}

function getFirstRowForTab(tab) {
  for (const group of tab.groups) {
    if (group.rows.length) {
      return group.rows[0];
    }
  }
  return null;
}

function getActiveTab() {
  return state.snapshot?.tabs.find((tab) => tab.id === state.activeTabId) || null;
}

function getMetaColumnLabel(key, fallback) {
  const column = state.meta?.columns?.find((entry) => entry.key === key);
  return column?.label || fallback;
}

function getCurrencyLabel(key, fallback) {
  const baseLabel = getMetaColumnLabel(key, fallback).replace(/\s+(INR|USD)$/, "");
  return `${baseLabel} ${state.displayCurrency}`;
}

function getSelectedRow(tab) {
  if (!tab) {
    return null;
  }
  const allRows = tab.groups.flatMap((group) => getSortedFilteredRows(tab, group.rows));
  return allRows.find((row) => row.item_id === state.selectedItemByTab[tab.id]) || allRows[0] || null;
}

function syncSelectedRow() {
  const tab = getActiveTab();
  if (!tab) {
    return;
  }
  const availableRows = tab.groups.flatMap((group) => getSortedFilteredRows(tab, group.rows));
  const selectedItem = state.selectedItemByTab[tab.id];
  const selectionStillVisible = availableRows.some((row) => row.item_id === selectedItem);
  if (!selectionStillVisible) {
    state.selectedItemByTab[tab.id] = availableRows[0]?.item_id || null;
  }
}

function sliceSeriesByRange(points, rangeKey) {
  if (!points.length) {
    return { points: [], hasSufficientHistory: false };
  }
  const range = RANGE_OPTIONS.find((entry) => entry.key === rangeKey) || RANGE_OPTIONS[RANGE_OPTIONS.length - 1];
  const endDate = parseIsoDateUtc(points[points.length - 1].date);
  const anchorDate = subtractCalendarOffsetUtc(endDate, range);
  const pointDates = points.map((point) => parseIsoDateUtc(point.date));
  const firstVisibleIndex = pointDates.findIndex((date) => date.getTime() >= anchorDate.getTime());

  if (firstVisibleIndex === -1) {
    return { points, hasSufficientHistory: false };
  }

  const firstVisibleDate = pointDates[firstVisibleIndex];
  const hasSufficientHistory = firstVisibleIndex > 0 || firstVisibleDate.getTime() === anchorDate.getTime();
  return {
    points: points.slice(firstVisibleIndex),
    hasSufficientHistory,
  };
}

function parseIsoDateUtc(dateString) {
  const [year, month, day] = dateString.split("-").map(Number);
  return new Date(Date.UTC(year, month - 1, day));
}

function subtractCalendarOffsetUtc(date, range) {
  const monthsToSubtract = (range.months || 0) + ((range.years || 0) * 12);
  const sourceYear = date.getUTCFullYear();
  const sourceMonth = date.getUTCMonth();
  const sourceDay = date.getUTCDate();
  const totalMonths = (sourceYear * 12) + sourceMonth - monthsToSubtract;
  const targetYear = Math.floor(totalMonths / 12);
  const targetMonth = totalMonths % 12;
  const lastDayOfTargetMonth = new Date(Date.UTC(targetYear, targetMonth + 1, 0)).getUTCDate();
  const targetDay = Math.min(sourceDay, lastDayOfTargetMonth);
  return new Date(Date.UTC(targetYear, targetMonth, targetDay));
}

function getRangeChangeValue(point) {
  if (!point) {
    return null;
  }
  if (state.displayCurrency === "INR") {
    return point.inr_close ?? null;
  }
  return point.close ?? null;
}

function formatDateTime(date) {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function formatPrice(value, currency) {
  if (value == null) {
    return "N/A";
  }
  try {
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency: currency || "USD",
      maximumFractionDigits: 2,
    }).format(value);
  } catch (error) {
    return `${(value || 0).toFixed(2)} ${currency || ""}`.trim();
  }
}

function formatRowPrice(row) {
  const value = getDisplayLastPrice(row);
  if (row.asset_type !== "index") {
    return formatPrice(value, state.displayCurrency);
  }
  if (value == null) {
    return "N/A";
  }
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value);
}

function formatTer(value) {
  if (value === null || value === undefined || value === "" || !Number.isFinite(Number(value))) {
    return "N/A";
  }
  return Number(value).toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  });
}

function formatPercent(value, includeSign = false) {
  if (value == null || Number.isNaN(value)) {
    return "N/A";
  }
  const prefix = includeSign && value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(2)}%`;
}

function getChangeClass(value) {
  if (value == null || Number.isNaN(value)) {
    return "value-neutral";
  }
  if (value > 0) {
    return "change-positive";
  }
  if (value < 0) {
    return "change-negative";
  }
  return "value-neutral";
}

async function fetchJson(path) {
  const url = new URL(path, document.baseURI);
  url.searchParams.set("refresh", Date.now().toString());
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Request failed for ${path}: ${response.status}`);
  }
  return response.json();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}
