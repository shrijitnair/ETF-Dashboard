const RANGE_OPTIONS = [
  { key: "1M", months: 1 },
  { key: "3M", months: 3 },
  { key: "6M", months: 6 },
  { key: "1Y", years: 1 },
  { key: "3Y", years: 3 },
  { key: "5Y", years: 5 },
];

const state = {
  dashboard: null,
  snapshot: null,
  history: null,
  meta: null,
  activeTabId: null,
  sortStateByTab: {},
  selectedItemByTab: {},
  chartRangeByTab: {},
  filterText: "",
  isAddingTicker: false,
  isRefreshing: false,
  addStatus: { kind: "idle", message: "" },
  refreshStatus: { kind: "idle", message: "" },
};

const elements = {
  tablist: document.getElementById("tablist"),
  tablePanel: document.getElementById("table-panel"),
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
  emptyTemplate: document.getElementById("empty-state-template"),
  returnBasisNote: document.getElementById("return-basis-note"),
  chartChangeLabel: document.getElementById("chart-change-label"),
  refreshButton: document.getElementById("refresh-button"),
  refreshStatus: document.getElementById("refresh-status"),
  addTickerForm: document.getElementById("add-ticker-form"),
  addTickerInput: document.getElementById("add-ticker-input"),
  addTickerButton: document.getElementById("add-ticker-button"),
  addTargetLabel: document.getElementById("add-target-label"),
  addTickerStatus: document.getElementById("add-ticker-status"),
};

init();

async function init() {
  try {
    bindPersistentEvents();
    await reloadDashboard();
  } catch (error) {
    renderAppError(error.message || "Failed to load dashboard data.");
  }
}

function bindPersistentEvents() {
  elements.searchInput.addEventListener("input", (event) => {
    state.filterText = event.target.value.trim().toLowerCase();
    syncSelectedRow();
    render();
  });

  elements.addTickerInput.addEventListener("input", () => {
    if (state.addStatus.kind !== "idle") {
      setAddStatus("idle", "");
    }
  });

  elements.refreshButton.addEventListener("click", async () => {
    if (state.isRefreshing) {
      return;
    }

    if (!state.snapshot) {
      setRefreshStatus("error", "Dashboard data is not loaded yet.");
      renderRefreshState();
      return;
    }

    state.isRefreshing = true;
    setRefreshStatus("loading", "Refreshing data from Yahoo Finance...");
    renderRefreshState();

    const preferredTabId = state.activeTabId;
    const preferredItemId = preferredTabId ? state.selectedItemByTab[preferredTabId] || null : null;

    try {
      const response = await postJson("/api/refresh", {});
      await reloadDashboard({
        preferredTabId,
        preferredItemId,
      });
      const builtAt = response.built_at ? formatDateTime(new Date(response.built_at)) : "just now";
      setRefreshStatus("success", `Refreshed ${response.instrument_count} instruments at ${builtAt}.`);
    } catch (error) {
      setRefreshStatus("error", error.message || "Refresh failed.");
    } finally {
      state.isRefreshing = false;
      render();
    }
  });

  elements.addTickerForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (state.isAddingTicker) {
      return;
    }

    const tab = getActiveTab();
    const rawTicker = elements.addTickerInput.value.trim().toUpperCase();
    if (!tab || !rawTicker) {
      setAddStatus("error", "Enter a Yahoo ticker before submitting.");
      renderAddFormState();
      return;
    }

    state.isAddingTicker = true;
    setAddStatus("loading", `Adding ${rawTicker} to ${tab.label}...`);
    renderAddFormState();

    try {
      const response = await postJson("/api/instruments", {
        tab_id: tab.id,
        ticker: rawTicker,
      });

      state.filterText = "";
      elements.searchInput.value = "";
      elements.addTickerInput.value = "";
      await reloadDashboard({
        preferredTabId: response.tab_id,
        preferredItemId: response.item?.item_id || null,
      });
      setAddStatus("success", `${response.item?.ticker || rawTicker} was added to the Custom group.`);
    } catch (error) {
      setAddStatus("error", error.message || "Ticker add failed.");
    } finally {
      state.isAddingTicker = false;
      render();
    }
  });
}

async function reloadDashboard(options = {}) {
  const previousTabId = options.preferredTabId || state.activeTabId;
  const dashboard = await fetchJson("/api/dashboard");

  state.dashboard = dashboard;
  state.snapshot = dashboard.snapshot;
  state.history = dashboard.history;
  state.meta = dashboard.meta;

  const nextTabId = state.snapshot.tabs.some((tab) => tab.id === previousTabId)
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
  renderToolbarState();
  renderReturnBasisUi();
  renderRangeButtons();
  renderTable();
  renderDetail();
  renderRefreshState();
  renderAddFormState();
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
  elements.tablist.innerHTML = state.snapshot.tabs.map((tab) => {
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

function renderToolbarState() {
  const tab = getActiveTab();
  if (!tab) {
    elements.addTargetLabel.textContent = "Current Tab";
    return;
  }

  elements.addTargetLabel.textContent = `${tab.label} • Custom`;
  if (tab.id === "ucits-etfs-lse") {
    elements.addTickerInput.placeholder = "e.g. CSPX.L";
  } else if (tab.id === "us-stocks") {
    elements.addTickerInput.placeholder = "e.g. AMD";
  } else {
    elements.addTickerInput.placeholder = "e.g. VXUS";
  }
}

function renderReturnBasisUi() {
  const returnBasisLabel = state.meta?.return_basis_label || "Local-currency";
  elements.returnBasisNote.textContent = `Returns shown as ${returnBasisLabel.toLowerCase()} performance.`;
  elements.chartChangeLabel.textContent = isInrAdjustedReturnBasis() ? "Range Change (INR)" : "Range Change";
}

function renderAddFormState() {
  elements.addTickerButton.disabled = state.isAddingTicker;
  elements.addTickerButton.textContent = state.isAddingTicker ? "Adding..." : "Add";

  const { kind, message } = state.addStatus;
  elements.addTickerStatus.textContent = message;
  elements.addTickerStatus.className = `add-ticker-status ${kind === "idle" ? "subdued" : `status-${kind}`}`;
}

function renderRefreshState() {
  elements.refreshButton.disabled = state.isRefreshing;
  elements.refreshButton.textContent = state.isRefreshing ? "Refreshing..." : "Refresh Data";

  const { kind, message } = state.refreshStatus;
  elements.refreshStatus.textContent = message;
  elements.refreshStatus.className = `refresh-status ${kind === "idle" ? "subdued" : `status-${kind}`}`;
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

function renderTable() {
  const tab = getActiveTab();
  if (!tab) {
    elements.tablePanel.innerHTML = "";
    return;
  }

  const columns = getColumnsForTab(tab);
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
        <div class="name-cell">
          <strong>${escapeHtml(row.name)}</strong>
          <div class="subdued">${escapeHtml(row.exchange)}${row.currency ? " • " + escapeHtml(row.currency) : ""}</div>
        </div>
      `;
    case "last_price":
      return `<strong>${formatPrice(row.last_price, row.currency)}</strong>`;
    case "aum":
      return row.aum ? `<span class="aum-pill">${escapeHtml(row.aum_display)}</span>` : `<span class="subdued">N/A</span>`;
    default:
      return `<span class="${getChangeClass(row[column.key])}">${escapeHtml(formatPercent(row[column.key], true))}</span>`;
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
  elements.detailTitle.textContent = row.ticker;
  elements.detailSubtitle.textContent = `${row.name} • ${row.exchange}${row.currency ? " • " + row.currency : ""}`;

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

  elements.chartLast.textContent = formatPrice(row.last_price, row.currency);
  elements.chartChange.textContent = formatPercent(rangeChange, true);
  elements.chartChange.className = getChangeClass(rangeChange);
  elements.chartStart.textContent = rangedSeries[0]?.date || "--";
  elements.chartEnd.textContent = rangedSeries[rangedSeries.length - 1]?.date || "--";

  const stats = [
    { label: getMetaColumnLabel("daily_pct", "1D"), value: formatPercent(row.daily_pct, true), className: getChangeClass(row.daily_pct) },
    { label: getMetaColumnLabel("five_day_pct", "5D"), value: formatPercent(row.five_day_pct, true), className: getChangeClass(row.five_day_pct) },
    { label: getMetaColumnLabel("one_month_pct", "1M"), value: formatPercent(row.one_month_pct, true), className: getChangeClass(row.one_month_pct) },
    { label: getMetaColumnLabel("three_month_pct", "3M"), value: formatPercent(row.three_month_pct, true), className: getChangeClass(row.three_month_pct) },
    { label: getMetaColumnLabel("one_year_pct", "1Y"), value: formatPercent(row.one_year_pct, true), className: getChangeClass(row.one_year_pct) },
    { label: getMetaColumnLabel("three_year_pct", "3Y"), value: formatPercent(row.three_year_pct, true), className: getChangeClass(row.three_year_pct) },
    { label: getMetaColumnLabel("five_year_pct", "5Y"), value: formatPercent(row.five_year_pct, true), className: getChangeClass(row.five_year_pct) },
    { label: getMetaColumnLabel("ytd_pct", "YTD"), value: formatPercent(row.ytd_pct, true), className: getChangeClass(row.ytd_pct) },
    { label: "Asset Type", value: row.asset_type === "etf" ? "ETF" : "Stock", className: "" },
  ];

  if (row.asset_type === "etf") {
    stats.push({ label: "AUM", value: row.aum_display || "N/A", className: "" });
  }

  elements.detailStats.innerHTML = stats.map((stat) => `
    <div class="stat-card">
      <span class="metric-label">${escapeHtml(stat.label)}</span>
      <strong class="${stat.className || ""}">${escapeHtml(stat.value)}</strong>
    </div>
  `).join("");
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
  const columns = [
    { key: "ticker", label: getMetaColumnLabel("ticker", "Ticker"), sortable: true, className: "ticker-cell" },
    { key: "last_price", label: getMetaColumnLabel("last_price", "Last"), sortable: true },
    { key: "daily_pct", label: getMetaColumnLabel("daily_pct", "1D INR"), sortable: true },
    { key: "five_day_pct", label: getMetaColumnLabel("five_day_pct", "5D INR"), sortable: true },
    { key: "one_month_pct", label: getMetaColumnLabel("one_month_pct", "1M INR"), sortable: true },
    { key: "three_month_pct", label: getMetaColumnLabel("three_month_pct", "3M INR"), sortable: true },
    { key: "one_year_pct", label: getMetaColumnLabel("one_year_pct", "1Y INR"), sortable: true },
    { key: "three_year_pct", label: getMetaColumnLabel("three_year_pct", "3Y INR"), sortable: true },
    { key: "five_year_pct", label: getMetaColumnLabel("five_year_pct", "5Y INR"), sortable: true },
    { key: "ytd_pct", label: getMetaColumnLabel("ytd_pct", "YTD INR"), sortable: true },
  ];

  if (tab.asset_type === "etf") {
    columns.push({ key: "aum", label: getMetaColumnLabel("aum", "AUM"), sortable: true });
  }
  return columns;
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
  if (isInrAdjustedReturnBasis()) {
    return point.inr_close ?? null;
  }
  return point.close ?? null;
}

function isInrAdjustedReturnBasis() {
  return state.meta?.return_basis === "inr_adjusted";
}

function setAddStatus(kind, message) {
  state.addStatus = { kind, message };
}

function setRefreshStatus(kind, message) {
  state.refreshStatus = { kind, message };
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
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Request failed for ${path}: ${response.status}`);
  }
  return response.json();
}

async function postJson(path, payload) {
  const response = await fetch(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(body.message || `Request failed for ${path}: ${response.status}`);
  }
  return body;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}
