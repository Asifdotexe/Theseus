/**
 * ui.js - Metrics calculations, tooltips, keyboard shortcuts, fossil cards,
 * repository request form, and legend interaction for Ship of Theseus.
 */

/**
 * Calculate and render summary metrics into the DOM.
 *
 * @param {Array<object>} points
 * @param {Array<string>} years
 */
export function updateInsights(points, years) {
  if (!points || points.length === 0) return;
  const last = points[points.length - 1];

  // 1. Birth Year (Genesis)
  const birthYear = years[0];
  const birthYearEl = document.getElementById("birth-year");
  if (birthYearEl) birthYearEl.textContent = birthYear || "----";

  // 2. Oldest Surviving Year
  let oldestSurviving = "--";
  for (const year of years) {
    if (last[year] > 0) {
      oldestSurviving = year;
      break;
    }
  }
  const oldestLineEl = document.getElementById("oldest-line");
  if (oldestLineEl) oldestLineEl.textContent = oldestSurviving;

  // 3. Percent Replaced (Foundation percent remaining)
  const foundationLines = last[birthYear] || 0;
  const totalLines = last.total || 0;
  const percentReplacedEl = document.getElementById("percent-replaced");
  if (percentReplacedEl) {
    if (birthYear && totalLines > 0) {
      const foundationPercent = (foundationLines / totalLines) * 100;
      percentReplacedEl.textContent = `${foundationPercent.toFixed(1)}%`;
    } else if (totalLines > 0) {
      percentReplacedEl.textContent = "0.0%";
    } else {
      percentReplacedEl.textContent = "--";
    }
  }

  // 4. Mean Code Age (Weighted average)
  const lastDate = new Date(last.date);
  const currentYear = lastDate.getFullYear();
  const meanCodeAgeEl = document.getElementById("mean-code-age");
  if (meanCodeAgeEl) {
    if (totalLines > 0) {
      let totalAge = 0;
      years.forEach((y) => {
        const lines = last[y] || 0;
        const age = currentYear - parseInt(y, 10);
        totalAge += lines * age;
      });
      const meanAge = totalAge / totalLines;
      meanCodeAgeEl.textContent = `${meanAge.toFixed(1)} yrs`;
    } else {
      meanCodeAgeEl.textContent = "0.0 yrs";
    }
  }

  // 5. Peak Preservation (Largest legacy year)
  let peakYear = "--";
  let peakVal = 0;
  years.forEach((y) => {
    if (parseInt(y, 10) < currentYear) {
      const val = last[y] || 0;
      if (val > peakVal) {
        peakVal = val;
        peakYear = y;
      }
    }
  });
  const peakYearEl = document.getElementById("peak-year");
  if (peakYearEl) peakYearEl.textContent = peakYear;

  // 6. Greatest Transformation (Largest single drop in origin)
  let maxDrop = 0;
  let dropDate = "--";
  if (birthYear && points.length > 1) {
    for (let i = 1; i < points.length; i++) {
      const prev = points[i - 1][birthYear] || 0;
      const curr = points[i][birthYear] || 0;
      const drop = prev - curr;
      if (drop > maxDrop) {
        maxDrop = drop;
        const d = new Date(points[i].date);
        dropDate = d.toLocaleDateString("en-US", {
          month: "short",
          year: "numeric",
        });
      }
    }
  }
  const transformationDateEl = document.getElementById("transformation-date");
  if (transformationDateEl) transformationDateEl.textContent = dropDate;
}

/**
 * Tooltip management class handling content formatting and viewport clamping.
 */
export class TooltipManager {
  constructor(tooltipEl, canvasEl) {
    this.tooltip = tooltipEl;
    this.canvas = canvasEl;
  }

  show(point, x, y, years, milestones = [], margin = { left: 60, top: 10 }) {
    if (!this.tooltip || !point) return;
    this.tooltip.classList.remove("hidden");
    this.tooltip.setAttribute("role", "tooltip");

    const dateStr =
      point.date instanceof Date
        ? point.date.toISOString().split("T")[0]
        : point.date;

    const foundationYear = years[0];
    const foundationVal = point[foundationYear] || 0;

    const existingYears = Object.keys(point)
      .filter((k) => k !== "date" && k !== "total" && point[k] > 0)
      .sort();
    const oldestSurvivingYear = existingYears[0];
    const oldestSurvivingVal = point[oldestSurvivingYear] || 0;

    const isFoundationAlive = foundationVal > 0;
    const refactoredVal = point.total - foundationVal;

    // Milestone check (within 3 months)
    let milestoneHTML = "";
    if (milestones && milestones.length > 0) {
      const pointDate = new Date(point.date);
      for (const m of milestones) {
        const milestoneDate = new Date(m.date + "-01");
        const monthsDiff = Math.abs(
          (pointDate - milestoneDate) / (1000 * 60 * 60 * 24 * 30)
        );
        if (monthsDiff <= 3) {
          milestoneHTML = `
            <div class="milestone-banner">
              <div class="milestone-title">${m.title}</div>
              <div class="milestone-desc">${m.description}</div>
            </div>
          `;
          break;
        }
      }
    }

    const pct = (val) =>
      point.total > 0 ? ((val / point.total) * 100).toFixed(1) + "%" : "0.0%";

    this.tooltip.innerHTML = `
      ${milestoneHTML}
      <div class="tooltip-header">${dateStr}</div>
      <div class="tooltip-total">${point.total.toLocaleString()} lines</div>
      <div class="tooltip-breakdown">
        <div class="tooltip-row">
          <span class="color-dot" style="background:oklch(68% 0.14 195)"></span>
          <span class="tooltip-row-label">Foundation (${foundationYear})</span>
          <span class="tooltip-row-value">${foundationVal.toLocaleString()}</span>
          <span class="tooltip-row-pct">${pct(foundationVal)}</span>
        </div>
        <div class="tooltip-row">
          <span class="color-dot" style="background:oklch(72% 0.16 65)"></span>
          <span class="tooltip-row-label">Refactored</span>
          <span class="tooltip-row-value">${refactoredVal.toLocaleString()}</span>
          <span class="tooltip-row-pct">${pct(refactoredVal)}</span>
        </div>
        ${
          !isFoundationAlive &&
          oldestSurvivingYear &&
          oldestSurvivingYear !== foundationYear
            ? `
        <div class="tooltip-row">
          <span class="color-dot" style="background:oklch(52% 0.22 285)"></span>
          <span class="tooltip-row-label">Oldest (${oldestSurvivingYear})</span>
          <span class="tooltip-row-value">${oldestSurvivingVal.toLocaleString()}</span>
          <span class="tooltip-row-pct">${pct(oldestSurvivingVal)}</span>
        </div>
        `
            : ""
        }
      </div>
    `;

    // Viewport edge flipping
    const tooltipWidth = this.tooltip.offsetWidth || 340;
    const tooltipHeight = this.tooltip.offsetHeight || 220;
    const svgRect = this.canvas.getBoundingClientRect();

    const targetY = y !== null ? y : this.tooltip.offsetHeight + 20;
    let left = x + 15;
    let top = targetY + 15;

    if (svgRect.left + left + tooltipWidth > window.innerWidth - 20) {
      left = x - tooltipWidth - 15;
    }
    if (svgRect.top + top + tooltipHeight > window.innerHeight - 20) {
      top = targetY - tooltipHeight - 15;
    }

    this.tooltip.style.left = `${left}px`;
    this.tooltip.style.top = `${top}px`;
  }

  hide() {
    if (this.tooltip) {
      this.tooltip.classList.add("hidden");
    }
  }
}

/**
 * Configure global keyboard navigation shortcuts.
 *
 * @param {object} handlers
 * @param {Function} [handlers.onSelectRepoIndex]
 * @param {Function} [handlers.onToggleMode]
 * @param {Function} [handlers.onToggleScale]
 */
export function setupKeyboardShortcuts({ onSelectRepoIndex, onToggleMode, onToggleScale }) {
  document.addEventListener("keydown", (e) => {
    if (["INPUT", "TEXTAREA", "SELECT"].includes(e.target.tagName)) return;
    if (e.metaKey || e.ctrlKey || e.altKey) return;

    const key = e.key;

    if (key >= "1" && key <= "9") {
      const idx = parseInt(key, 10) - 1;
      if (onSelectRepoIndex) {
        onSelectRepoIndex(idx);
      } else {
        const btns = document.querySelectorAll(".repo-btn");
        if (idx < btns.length) btns[idx].click();
      }
      return;
    }

    if (key === "m" || key === "M") {
      if (onToggleMode) {
        onToggleMode();
      } else {
        const active = document.querySelector(".mode-btn.active");
        if (active) {
          const next =
            active.nextElementSibling || active.previousElementSibling;
          if (next) next.click();
        }
      }
      return;
    }

    if (key === "s" || key === "S") {
      if (onToggleScale) {
        onToggleScale();
      } else {
        const active = document.querySelector(".scale-btn.active");
        if (active) {
          const next =
            active.nextElementSibling || active.previousElementSibling;
          if (next) next.click();
        }
      }
    }
  });
}

/**
 * Render Genesis and Survivor fossil cards with clickable GitHub links.
 *
 * @param {object} fossils
 * @param {string|null} repoPath
 */
export function renderFossils(fossils = {}, repoPath = null) {
  const genesis = fossils.genesis || {};
  const survivor = fossils.survivor || {};

  const buildLink = (fossil) => {
    const display = fossil.file ? `${fossil.file}:${fossil.line}` : "--";

    if (!fossil.file || !repoPath) return document.createTextNode(display);

    const linkCommit = fossil.view_commit || fossil.commit;
    if (!linkCommit) return document.createTextNode(display);

    const safeFile = fossil.file.split("/").map(encodeURIComponent).join("/");
    const safeCommit = encodeURIComponent(linkCommit);
    const safeLine = parseInt(fossil.line, 10) || 0;
    const url = `https://github.com/${repoPath}/blob/${safeCommit}/${safeFile}#L${safeLine}`;

    const a = document.createElement("a");
    a.href = url;
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    a.textContent = display;
    a.className = "fossil-link";
    return a;
  };

  // Genesis (Historical Fossil)
  const genesisYear = document.getElementById("genesis-year");
  if (genesisYear) genesisYear.textContent = genesis.year || "----";
  const genesisFile = document.getElementById("genesis-file");
  if (genesisFile) genesisFile.replaceChildren(buildLink(genesis));
  const genesisContent = document.getElementById("genesis-content");
  if (genesisContent) {
    genesisContent.textContent = genesis.content
      ? genesis.content.trim()
      : "No fossil data";
  }
  const genesisCommit = document.getElementById("genesis-commit");
  if (genesisCommit) genesisCommit.textContent = genesis.commit || "";

  // Survivor (Living Fossil)
  const survivorYear = document.getElementById("survivor-year");
  if (survivorYear) survivorYear.textContent = survivor.year || "----";
  const survivorFile = document.getElementById("survivor-file");
  if (survivorFile) survivorFile.replaceChildren(buildLink(survivor));
  const survivorContent = document.getElementById("survivor-content");
  if (survivorContent) {
    survivorContent.textContent = survivor.content
      ? survivor.content.trim()
      : "No fossil data";
  }
  const survivorCommit = document.getElementById("survivor-commit");
  if (survivorCommit) {
    survivorCommit.textContent = survivor.view_commit || survivor.commit || "";
  }

  // Keyboard and click interaction for fossil card containers
  ["fossil-genesis", "fossil-survivor"].forEach((id) => {
    const card = document.getElementById(id);
    if (!card || card.dataset.fossilInited) return;
    card.dataset.fossilInited = "true";
    card.setAttribute("tabindex", "0");
    card.setAttribute("role", "link");

    const openLink = () => {
      const link = card.querySelector(".fossil-link");
      if (link && link.href) window.open(link.href, "_blank");
    };

    card.addEventListener("click", (e) => {
      if (e.target.closest(".fossil-link")) return;
      openLink();
    });

    card.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        openLink();
      }
    });
  });
}

/**
 * Configure repository request form with a zero-cost path that opens a pre-filled GitHub issue.
 */
export function setupRepoRequest() {
  const form = document.getElementById("repo-request-form");
  const input = document.getElementById("repo-url");
  const feedback = document.getElementById("repo-request-feedback");
  if (!form || !input || !feedback) return;

  const section = form.closest(".repo-request");
  const star = section?.querySelector(".repo-request-icon");

  function setFeedback(html, type) {
    feedback.innerHTML = html;
    feedback.className = "repo-request-feedback" + (type ? " " + type : "");
    feedback.classList.toggle("visible", !!html);
  }

  function validateUrl(raw) {
    const cleaned = raw
      .replace(/^https?:\/\//, "")
      .replace(/^www\./, "")
      .replace(/\/$/, "")
      .replace(/^github\.com\//, "");
    return /^[\w.-]+\/[\w.-]+$/.test(cleaned) ? cleaned : null;
  }

  if (star) {
    input.addEventListener("focus", () => star.classList.add("seafoam"));
    input.addEventListener("blur", () => star.classList.remove("seafoam"));
  }

  input.addEventListener("input", () => {
    input.classList.remove("invalid");
    if (feedback.textContent) setFeedback("", "");
  });

  form.addEventListener("submit", (e) => {
    e.preventDefault();
    const raw = input.value.trim();
    if (!raw) {
      input.classList.add("invalid");
      setFeedback("Enter a repository URL or owner/repo name.", "error");
      return;
    }

    const slug = validateUrl(raw);
    if (!slug) {
      input.classList.add("invalid");
      setFeedback("Enter a valid GitHub URL (e.g. owner/repo)", "error");
      return;
    }

    const title = encodeURIComponent(`Repository Request: ${slug}`);
    const body = encodeURIComponent(
      `### Repository Request\n\n` +
      `**Repository:** https://github.com/${slug}\n\n` +
      `**Why should this repository be added to Ship of Theseus?**\n` +
      `(Add any context or reasons here)\n\n` +
      `---\n*Submitted via Ship of Theseus web app*`
    );
    const issueUrl = `https://github.com/Asifdotexe/Theseus/issues/new?title=${title}&body=${body}`;

    const newWindow = window.open(issueUrl, "_blank", "noopener,noreferrer");
    if (!newWindow || newWindow.closed || typeof newWindow.closed === "undefined") {
      setFeedback(
        `Popup blocked. <a href="${issueUrl}" target="_blank" rel="noopener noreferrer" style="color:var(--seafoam);text-decoration:underline;">Click here to open the GitHub issue</a>.`,
        "success"
      );
    } else {
      setFeedback(
        `Opening GitHub issue template. <a href="${issueUrl}" target="_blank" rel="noopener noreferrer" style="color:var(--seafoam);text-decoration:underline;">Click here if not redirected</a>.`,
        "success"
      );
    }

    input.value = "";
    if (star) {
      star.classList.remove("seafoam");
      star.classList.add("pulse");
      setTimeout(() => star.classList.remove("pulse"), 800);
    }
    if (section) {
      section.classList.add("glow");
      setTimeout(() => section.classList.remove("glow"), 2000);
    }
  });
}

/**
 * Render the interactive collapsible legend for stacked areas.
 *
 * @param {HTMLElement} legendEl
 * @param {Array<string>} years
 * @param {string} vizMode
 */
export function renderLegend(legendEl, years, vizMode) {
  if (!legendEl) return;
  legendEl.innerHTML = "";

  let items;
  if (vizMode === "identity") {
    items = [
      { label: "Original Code", color: "oklch(68% 0.14 195)" },
      { label: "Refactored", color: "oklch(72% 0.16 65)" },
    ];
  } else {
    items = years.map((y, i) => ({
      label: y,
      color: `oklch(70% 0.14 ${(195 + i * 36) % 360})`,
    }));
  }

  const triggerText =
    vizMode === "identity"
      ? "Original + Refactored · 2 layers"
      : `${years[0]}–${years[years.length - 1]} · ${years.length} years`;

  const trigger = document.createElement("button");
  trigger.className = "legend-trigger";
  trigger.setAttribute("aria-expanded", "false");
  trigger.setAttribute(
    "aria-label",
    `Legend: ${triggerText}. Hover or focus to expand.`
  );
  trigger.innerHTML = `${triggerText}<span class="legend-trigger-icon"></span>`;
  legendEl.appendChild(trigger);

  const panel = document.createElement("div");
  panel.className = "legend-panel";
  panel.setAttribute("role", "tooltip");
  legendEl.appendChild(panel);

  const d3 = window.d3;

  items.forEach((item) => {
    const el = document.createElement("div");
    el.className = "legend-item";
    el.innerHTML = `<span class="color-dot" style="background: ${item.color}"></span><span>${item.label}</span>`;

    el.onmouseenter = () => {
      if (!d3) return;
      const label = item.label;
      const firstYear = years[0];

      d3.selectAll(".chart-area").style("opacity", 0.1);

      if (vizMode === "identity") {
        if (label === "Original Code") {
          d3.selectAll(`.chart-area[data-year='${firstYear}']`).style(
            "opacity",
            1
          );
        } else {
          d3.selectAll(".chart-area")
            .filter(function () {
              return d3.select(this).attr("data-year") !== firstYear;
            })
            .style("opacity", 1);
        }
      } else {
        d3.selectAll(`.chart-area[data-year='${label}']`).style("opacity", 1);
      }
    };

    el.onmouseleave = () => {
      if (!d3) return;
      d3.selectAll(".chart-area").style("opacity", 1);
    };

    panel.appendChild(el);
  });
}

/**
 * Attach expand/collapse listeners to the legend component.
 *
 * @param {HTMLElement} legendEl
 */
export function attachLegendHandlers(legendEl) {
  if (!legendEl || legendEl.dataset.legendInited) return;
  legendEl.dataset.legendInited = "true";

  const toggleExpanded = (expanded) => {
    const trigger = legendEl.querySelector(".legend-trigger");
    if (trigger) trigger.setAttribute("aria-expanded", String(expanded));
  };

  legendEl.addEventListener("mouseenter", () => toggleExpanded(true));
  legendEl.addEventListener("mouseleave", () => toggleExpanded(false));

  legendEl.addEventListener("focusin", (e) => {
    if (legendEl.contains(e.target)) toggleExpanded(true);
  });
  legendEl.addEventListener("focusout", (e) => {
    if (!legendEl.contains(e.relatedTarget)) toggleExpanded(false);
  });
}

/**
 * Toggle the skeleton loading indicator and chart container visibility.
 *
 * @param {HTMLElement} loadingEl
 * @param {HTMLElement} chartContainerEl
 * @param {boolean} show
 */
export function showLoading(loadingEl, chartContainerEl, show) {
  if (show) {
    if (loadingEl) loadingEl.classList.remove("hidden");
    if (chartContainerEl) chartContainerEl.style.opacity = "0";
  } else {
    if (loadingEl) loadingEl.classList.add("hidden");
    if (chartContainerEl) {
      chartContainerEl.style.transition = "opacity 0.35s ease";
      chartContainerEl.style.opacity = "1";
    }
  }
}

/**
 * Display an error message banner.
 *
 * @param {HTMLElement} errorEl
 * @param {string} msg
 */
export function showError(errorEl, msg) {
  if (errorEl) {
    errorEl.textContent = msg;
    errorEl.classList.remove("hidden");
  }
}

/**
 * Hide the error message banner.
 *
 * @param {HTMLElement} errorEl
 */
export function hideError(errorEl) {
  if (errorEl) {
    errorEl.classList.add("hidden");
  }
}
