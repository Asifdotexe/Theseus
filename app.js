/**
 * Ship of Theseus - Code Visualizer
 * Core Logic (Vanilla JS + SVG)
 * Data is loaded from data/*.json files - run with a local server
 */

class TheseusVisualizer {
  constructor() {
    this.manifest = null;
    this.currentData = null;
    this.currentRepo = null;
    this.canvas = document.getElementById("main-chart");
    this.tooltip = document.getElementById("tooltip");
    this.legend = document.getElementById("chart-legend");
    this.repoSelector = document.getElementById("repo-selector");
    this.repoDescription = document.getElementById("repo-description");
    this.vizToggle = document.getElementById("viz-mode-toggle");
    this.scaleToggle = document.getElementById("scale-toggle");
    this.loadingState = document.getElementById("chart-loading");

    this.margin = { top: 10, right: 20, bottom: 50, left: 60 };
    this.years = [];
    this.points = [];
    this.vizMode = "chronological"; // 'chronological' | 'identity'
    this.yScaleMode = "linear"; // 'linear' | 'log'
    this.fossils = {};
    this.reducedMotion = window.matchMedia(
      "(prefers-reduced-motion: reduce)",
    ).matches;
    this.animDuration = this.reducedMotion ? 0 : 800;

    this.focusIndex = undefined;
    this.abortController = null;
    this.legendHandlersAttached = false;
    this.init();
  }

  async init() {
    try {
      const response = await fetch("theseus.config.json");
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      let data = await response.json();
      // Fallback for backward compatibility, but normally expected to be under 'repositories'
      this.manifest =
        data.repositories || (Array.isArray(data) ? data : [data]);

      this.renderSelectors();
      this.setupModeToggle();
      this.setupScaleToggle();

      if (this.manifest.length > 0) {
        this.loadRepo(this.manifest[0].name);
      }
    } catch (err) {
      this.showError("Failed to load repository manifest: " + err.message);
    }

    window.addEventListener("resize", () => this.debouncedRender());
    this.setupKeyboardShortcuts();
    this.setupRepoRequest();
    this.attachLegendHandlers();
  }

  setupModeToggle() {
    this.vizToggle.addEventListener("click", (e) => {
      const btn = e.target.closest(".mode-btn");
      if (!btn || btn.classList.contains("active")) return;

      document
        .querySelectorAll(".mode-btn")
        .forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");

      this.vizMode = btn.dataset.mode;
      if (this.currentData) this.renderChart();
    });
  }

  setupScaleToggle() {
    this.scaleToggle.addEventListener("click", (e) => {
      const btn = e.target.closest(".scale-btn");
      if (!btn || btn.classList.contains("active")) return;

      document
        .querySelectorAll(".scale-btn")
        .forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");

      this.yScaleMode = btn.dataset.scale;
      if (this.currentData) this.renderChart();
    });
  }

  setupKeyboardShortcuts() {
    document.addEventListener("keydown", (e) => {
      if (["INPUT", "TEXTAREA", "SELECT"].includes(e.target.tagName)) return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;

      const key = e.key;

      if (key >= "1" && key <= "9") {
        const idx = parseInt(key, 10) - 1;
        const btns = document.querySelectorAll(".repo-btn");
        if (idx < btns.length) {
          btns[idx].click();
        }
        return;
      }

      if (key === "m" || key === "M") {
        const active = document.querySelector(".mode-btn.active");
        if (active) {
          const next =
            active.nextElementSibling || active.previousElementSibling;
          if (next) next.click();
        }
        return;
      }

      if (key === "s" || key === "S") {
        const active = document.querySelector(".scale-btn.active");
        if (active) {
          const next =
            active.nextElementSibling || active.previousElementSibling;
          if (next) next.click();
        }
        return;
      }
    });
  }

  setupRepoRequest() {
    const form = document.getElementById("repo-request-form");
    const input = document.getElementById("repo-url");
    const feedback = document.getElementById("repo-request-feedback");
    if (!form || !input || !feedback) return;

    const section = form.closest(".repo-request");
    const star = section?.querySelector(".repo-request-icon");

    const WEB3FORM_KEY = "__WEB3FORM_ACCESS_KEY__";

    function setFeedback(message, type) {
      feedback.textContent = message;
      feedback.className = "repo-request-feedback" + (type ? " " + type : "");
      feedback.classList.toggle("visible", !!message);
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
      if (feedback.textContent) setFeedback("");
    });

    form.addEventListener("submit", async (e) => {
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

      if (WEB3FORM_KEY.startsWith("__")) {
        setFeedback(
          "Form submissions are not available in development mode.",
          "error",
        );
        return;
      }

      const submitBtn = form.querySelector('button[type="submit"]');
      const originalText = submitBtn.textContent;
      submitBtn.disabled = true;
      submitBtn.textContent = "Submitting...";
      submitBtn.classList.add("loading");

      const formController = new AbortController();

      try {
        const response = await fetch("https://api.web3forms.com/submit", {
          method: "POST",
          signal: formController.signal,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            access_key: WEB3FORM_KEY,
            subject: "Repository request: " + slug,
            message: [
              "**Repository:** " + raw,
              "",
              "---",
              "Submitted via the Ship of Theseus dashboard.",
            ].join("\n"),
          }),
        });

        const result = await response.json();
        if (result.success) {
          setFeedback(
            "Request submitted. I will look into it soon.",
            "success",
          );
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
        } else {
          setFeedback("Submission failed. Please try again later.", "error");
        }
      } catch {
        setFeedback("Network error. Please check your connection.", "error");
      } finally {
        submitBtn.disabled = false;
        submitBtn.textContent = originalText;
        submitBtn.classList.remove("loading");
      }
    });
  }

  renderSelectors() {
    this.repoSelector.innerHTML = "";
    this.manifest.forEach((repo) => {
      const btn = document.createElement("button");
      btn.className = "repo-btn";
      btn.textContent = repo.name.replace(/-/g, " ");
      btn.dataset.repo = repo.name;
      btn.onclick = () => this.loadRepo(repo.name);
      this.repoSelector.appendChild(btn);
    });
  }

  async loadRepo(repoName) {
    if (this.currentRepo === repoName && this.currentData) return;

    if (this.abortController) {
      this.abortController.abort();
    }
    this.abortController = new AbortController();
    const signal = this.abortController.signal;

    this.showLoading(true);
    this.hideError();

    try {
      const repoInfo = this.manifest.find((r) => r.name === repoName);
      if (!repoInfo) {
        this.showError("Repository not found: " + repoName);
        return;
      }
      this.repoDescription.textContent = repoInfo.description || "";

      const response = await fetch(`data/${repoInfo.file}`, { signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const rawData = await response.json();

      // Handle both list and object schemas
      if (Array.isArray(rawData)) {
        this.currentData = rawData;
        this.fossils = {};
      } else {
        this.currentData = rawData.snapshots || [];
        this.fossils = rawData.fossils || {};
      }

      this.currentRepo = repoName;
      this.updateActiveBtn(repoName);

      this.processData();
      this.renderChart();
      this.updateInsights();
      this.renderFossils();
    } catch (err) {
      if (err.name === "AbortError") return;
      console.error(err);
      this.showError(`Failed to load data for ${repoName}`);
    } finally {
      this.showLoading(false);
    }
  }

  updateActiveBtn(name) {
    document.querySelectorAll(".repo-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.repo === name);
    });
  }

  processData() {
    // Validate snapshot dates, filter out invalid entries
    this.currentData = this.currentData.filter((d) => {
      const ts = new Date(d.snapshot_date).getTime();
      return !isNaN(ts);
    });

    // Sort snapshots chronologically
    this.currentData.sort(
      (a, b) =>
        new Date(a.snapshot_date).getTime() -
        new Date(b.snapshot_date).getTime(),
    );

    const yearSet = new Set();
    this.currentData.forEach((d) => {
      Object.keys(d.composition).forEach((y) => yearSet.add(y));
    });
    this.years = Array.from(yearSet).sort();

    // Convert to D3 stack-ready format
    this.points = this.currentData.map((d) => {
      const totalLines = Object.values(d.composition).reduce(
        (acc, val) => acc + val,
        0,
      );
      const point = {
        date: new Date(d.snapshot_date),
        total: totalLines,
      };
      this.years.forEach((year) => {
        point[year] = d.composition[year] || 0;
      });
      return point;
    });
  }

  renderChart() {
    if (!this.points || this.points.length === 0) {
      this.showError("No snapshot data to display.");
      return;
    }

    const width = this.canvas.clientWidth;
    const height = this.canvas.clientHeight;
    if (!width || !height) return;

    const chartWidth = width - this.margin.left - this.margin.right;
    const chartHeight = height - this.margin.top - this.margin.bottom;

    const svg = d3.select(this.canvas);
    const needsBuild = !this._built;
    const dimsChanged =
      this._chartWidth !== chartWidth || this._chartHeight !== chartHeight;

    this.focusIndex = undefined;

    // — Structural phase (first time, resize, or repo switch) —
    if (needsBuild || dimsChanged) {
      svg.selectAll("*").remove();
      svg.append("defs");
      this._g = svg
        .append("g")
        .attr("class", "chart-container")
        .attr("transform", `translate(${this.margin.left},${this.margin.top})`);
      this._chartWidth = chartWidth;
      this._chartHeight = chartHeight;
      this._built = true;
    }

    const g = this._g;

    // — Clear data-driven children (keep g itself, defs, and their attributes) —
    g.selectAll(
      "g.axis-y, g.axis-x, .milestone-marker, path.layer, .scrubber-line, rect[fill='transparent'], text.axis-label",
    ).remove();
    svg.select("defs").selectAll("linearGradient").remove();

    // — Scales —
    const xScale = d3
      .scaleTime()
      .domain(d3.extent(this.points, (d) => d.date))
      .range([0, chartWidth]);
    this.xScale = xScale;

    const maxTotal = d3.max(this.points, (d) => d.total);
    let yScale;
    if (this.yScaleMode === "log") {
      yScale = d3
        .scaleLog()
        .domain([1, maxTotal * 1.1])
        .range([chartHeight, 0])
        .clamp(true);
    } else {
      yScale = d3
        .scaleLinear()
        .domain([0, maxTotal * 1.05])
        .range([chartHeight, 0]);
    }

    // — Gradients —
    const defs = svg.select("defs");
    const getBaseColor = (seriesName, seriesIndex) => {
      if (this.vizMode === "identity") {
        return seriesIndex === 0 ? "oklch(68% 0.14 195)" : "oklch(72% 0.16 65)";
      }
      const yearIdx = this.years.indexOf(seriesName);
      return `oklch(70% 0.14 ${(195 + yearIdx * 36) % 360})`;
    };

    this.years.forEach((year, i) => {
      const color = getBaseColor(year, i);
      const grad = defs
        .append("linearGradient")
        .attr("id", `grad-${year}`)
        .attr("x1", "0%")
        .attr("y1", "0%")
        .attr("x2", "0%")
        .attr("y2", "100%");
      grad
        .append("stop")
        .attr("offset", "0%")
        .attr("stop-color", color)
        .attr("stop-opacity", 0.9);
      grad
        .append("stop")
        .attr("offset", "100%")
        .attr("stop-color", color)
        .attr("stop-opacity", 0.4);
    });

    if (this.vizMode === "identity") {
      ["original", "refactored"].forEach((id) => {
        const color =
          id === "original" ? "oklch(68% 0.14 195)" : "oklch(72% 0.16 65)";
        const grad = defs
          .append("linearGradient")
          .attr("id", `grad-id-${id}`)
          .attr("x1", "0%")
          .attr("y1", "0%")
          .attr("x2", "0%")
          .attr("y2", "100%");
        grad
          .append("stop")
          .attr("offset", "0%")
          .attr("stop-color", color)
          .attr("stop-opacity", 0.9);
        grad
          .append("stop")
          .attr("offset", "100%")
          .attr("stop-color", color)
          .attr("stop-opacity", 0.4);
      });
    }

    // — Stack & Area (data join) —
    const stackGenerator = d3.stack().keys(this.years);

    const stackedData = stackGenerator(this.points);

    const areaGenerator = d3
      .area()
      .x((d) => xScale(d.data.date))
      .y0((d) => yScale(this.yScaleMode === "log" ? Math.max(1, d[0]) : d[0]))
      .y1((d) => yScale(this.yScaleMode === "log" ? Math.max(1, d[1]) : d[1]))
      .curve(d3.curveMonotoneX);

    const layers = g.selectAll(".layer").data(stackedData, (d) => d.key);

    const getFill = (d, i) => {
      if (this.vizMode === "identity") {
        const id = i === 0 ? "original" : "refactored";
        return `url(#grad-id-${id})`;
      }
      return `url(#grad-${d.key})`;
    };

    layers
      .enter()
      .append("path")
      .attr("class", "chart-area layer")
      .attr("data-year", (d) => d.key)
      .attr("fill", getFill)
      .attr("d", areaGenerator)
      .style("opacity", 0)
      .transition()
      .duration(this.animDuration)
      .delay((d, i) => (this.reducedMotion ? 0 : i * 50))
      .style("opacity", 1);

    layers
      .transition()
      .duration(this.animDuration)
      .attr("d", areaGenerator)
      .attr("fill", getFill);

    layers.exit().remove();

    // — Axes, Milestones, Interaction, Legend —
    this.renderAxes(g, chartWidth, chartHeight, xScale, yScale);
    this.renderMilestoneMarkers(g, chartWidth, chartHeight, xScale);
    this.setupInteractivity(g, chartWidth, chartHeight, xScale, yScale);
    this.renderLegend();
  }

  renderMilestoneMarkers(g, chartWidth, chartHeight, xScale) {
    const repoInfo = this.manifest.find((r) => r.name === this.currentRepo);
    if (!repoInfo || !repoInfo.milestones) return;

    const tooltip = this;

    repoInfo.milestones.forEach((m) => {
      const milestoneDate = new Date(m.date + "-01");
      const xPos = xScale(milestoneDate);

      if (xPos >= 0 && xPos <= chartWidth) {
        const marker = g
          .append("g")
          .attr("class", "milestone-marker")
          .attr("transform", `translate(${xPos}, 0)`)
          .attr("tabindex", "0")
          .attr("role", "button")
          .attr("aria-label", `${m.title}: ${m.description}`)
          .style("cursor", "pointer");

        marker
          .append("text")
          .attr("x", 0)
          .attr("y", 18)
          .attr("text-anchor", "middle")
          .attr("font-size", "14px")
          .attr("fill", "oklch(68% 0.14 195)")
          .text("★")
          .style("opacity", 0.8)
          .style("filter", "drop-shadow(0 0 4px oklch(68% 0.14 195 / 0.6))");

        marker.append("title").text(m.title + ": " + m.description);

        const animDur = this.reducedMotion ? 0 : 200;

        const enlarge = function () {
          d3.select(this)
            .select("text")
            .transition()
            .duration(animDur)
            .attr("font-size", "18px")
            .style("opacity", 1);
        };

        const shrink = function () {
          d3.select(this)
            .select("text")
            .transition()
            .duration(animDur)
            .attr("font-size", "14px")
            .style("opacity", 0.8);
        };

        marker
          .on("mouseenter", enlarge)
          .on("mouseleave", shrink)
          .on("focus", enlarge)
          .on("blur", shrink);
      }
    });
  }

  renderLegend() {
    this.legend.innerHTML = "";

    let items;
    if (this.vizMode === "identity") {
      items = [
        { label: "Original Code", color: "oklch(68% 0.14 195)" },
        { label: "Refactored", color: "oklch(72% 0.16 65)" },
      ];
    } else {
      items = this.years.map((y, i) => ({
        label: y,
        color: `oklch(70% 0.14 ${(195 + i * 36) % 360})`,
      }));
    }

    // Trigger pill
    const years = this.years;
    const triggerText =
      this.vizMode === "identity"
        ? "Original + Refactored · 2 layers"
        : `${years[0]}–${years[years.length - 1]} · ${years.length} years`;

    const trigger = document.createElement("button");
    trigger.className = "legend-trigger";
    trigger.setAttribute("aria-expanded", "false");
    trigger.setAttribute(
      "aria-label",
      `Legend: ${triggerText}. Hover or focus to expand.`,
    );
    trigger.innerHTML = `${triggerText}<span class="legend-trigger-icon"></span>`;
    this.legend.appendChild(trigger);

    // Panel
    const panel = document.createElement("div");
    panel.className = "legend-panel";
    panel.setAttribute("role", "tooltip");
    this.legend.appendChild(panel);

    items.forEach((item) => {
      const el = document.createElement("div");
      el.className = "legend-item";
      el.innerHTML = `<span class="color-dot" style="background: ${item.color}"></span><span>${item.label}</span>`;

      el.onmouseenter = () => {
        const label = item.label;
        const firstYear = years[0];

        d3.selectAll(".chart-area").style("opacity", 0.1);

        if (this.vizMode === "identity") {
          if (label === "Original Code") {
            d3.selectAll(`.chart-area[data-year='${firstYear}']`).style(
              "opacity",
              1,
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
        d3.selectAll(".chart-area").style("opacity", 1);
      };

      panel.appendChild(el);
    });

  }

  attachLegendHandlers() {
    if (this.legendHandlersAttached || !this.legend) return;
    this.legendHandlersAttached = true;

    const toggleExpanded = (expanded) => {
      const trigger = this.legend.querySelector(".legend-trigger");
      if (trigger) trigger.setAttribute("aria-expanded", String(expanded));
    };

    this.legend.addEventListener("mouseenter", () => toggleExpanded(true));
    this.legend.addEventListener("mouseleave", () => toggleExpanded(false));

    this.legend.addEventListener("focusin", (e) => {
      if (this.legend.contains(e.target)) toggleExpanded(true);
    });
    this.legend.addEventListener("focusout", (e) => {
      if (!this.legend.contains(e.relatedTarget)) toggleExpanded(false);
    });
  }

  renderAxes(g, width, height, xScale, yScale) {
    // Y Axis - Custom Grid & Labels
    const yAxis = d3
      .axisLeft(yScale)
      .ticks(5)
      .tickFormat((v) => {
        if (v >= 1000000) return `${(v / 1000000).toFixed(1)}M`;
        if (v >= 1000) return `${(v / 1000).toFixed(1)}k`;
        return Math.round(v);
      })
      .tickSize(-width);

    const yGroup = g.append("g").attr("class", "axis-y").call(yAxis);

    yGroup
      .selectAll(".tick line")
      .attr("stroke", "oklch(30% 0.01 260)")
      .attr("stroke-dasharray", "3,3")
      .attr("stroke-opacity", 0.5);

    yGroup
      .selectAll("text")
      .attr("x", -10)
      .attr("fill", "oklch(55% 0.015 255)")
      .attr("font-size", "10px")
      .attr("font-family", "inherit");

    yGroup.select(".domain").remove();

    // X Axis
    const xAxis = d3
      .axisBottom(xScale)
      .ticks(Math.min(this.points.length, 6))
      .tickFormat(d3.timeFormat("%Y"));

    const xGroup = g
      .append("g")
      .attr("class", "axis-x")
      .attr("transform", `translate(0,${height})`)
      .call(xAxis);

    xGroup
      .selectAll("text")
      .attr("y", 15)
      .attr("fill", "oklch(55% 0.015 255)")
      .attr("font-size", "11px")
      .attr("letter-spacing", "0.05em")
      .attr("font-family", "inherit");

    xGroup.select(".domain").attr("stroke", "oklch(100% 0 0 / 0.1)");
    xGroup.selectAll(".tick line").attr("stroke", "oklch(100% 0 0 / 0.1)");

    // Axis Labels
    g.append("text")
      .attr("class", "axis-label")
      .attr("x", width / 2)
      .attr("y", height + 40)
      .attr("fill", "oklch(55% 0.015 255)")
      .attr("font-size", "12px")
      .attr("text-anchor", "middle")
      .text("Time");

    g.append("text")
      .attr("class", "axis-label")
      .attr("transform", "rotate(-90)")
      .attr("x", -height / 2)
      .attr("y", -45)
      .attr("fill", "oklch(55% 0.015 255)")
      .attr("font-size", "12px")
      .attr("text-anchor", "middle")
      .text("Lines of Code");
  }

  setupInteractivity(g, width, height, xScale, yScale) {
    const scrubber = g
      .append("line")
      .attr("class", "scrubber-line hidden")
      .attr("y1", 0)
      .attr("y2", height)
      .attr("stroke", "oklch(100% 0 0 / 0.2)")
      .attr("stroke-width", 1);
    const bisect = d3.bisector((d) => d.date).left;

    const self = this;

    g.append("rect")
      .attr("width", width)
      .attr("height", height)
      .attr("fill", "transparent")
      .attr("tabindex", "0")
      .attr("role", "img")
      .attr(
        "aria-label",
        "Chart of code composition by year. Use Arrow Left and Arrow Right to navigate data points.",
      )
      .on("mousemove", function (event) {
        const mouseX = d3.pointer(event)[0];
        const date = xScale.invert(mouseX);
        const idx = bisect(self.points, date, 1);
        const d0 = self.points[idx - 1];
        const d1 = self.points[idx];

        // Handle single-point or edge cases
        if (!d0 && !d1) return;
        let d;
        if (!d0) d = d1;
        else if (!d1) d = d0;
        else d = date - d0.date > d1.date - date ? d1 : d0;

        const snappedX = xScale(d.date);
        scrubber
          .attr("x1", snappedX)
          .attr("x2", snappedX)
          .classed("hidden", false);

        const svgRect = self.canvas.getBoundingClientRect();
        self.showTooltip(
          d,
          snappedX + self.margin.left,
          d3.pointer(event)[1] + self.margin.top,
        );
      })
      .on("mouseleave", () => {
        self.hideTooltip();
        scrubber.classed("hidden", true);
      })
      .on("focus", () => {
        if (
          self.focusIndex === undefined ||
          self.focusIndex >= self.points.length
        ) {
          self.focusIndex = self.points.length - 1;
        }
        self.updateFocusPoint(xScale, scrubber);
      })
      .on("blur", () => {
        self.hideTooltip();
        scrubber.classed("hidden", true);
      })
      .on("keydown", (event) => {
        if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
          event.preventDefault();
          const len = self.points.length;
          if (len === 0) return;

          if (self.focusIndex === undefined) {
            self.focusIndex = len - 1;
          }

          if (event.key === "ArrowLeft") {
            self.focusIndex = Math.max(0, self.focusIndex - 1);
          } else {
            self.focusIndex = Math.min(len - 1, self.focusIndex + 1);
          }

          self.updateFocusPoint(xScale, scrubber);
        }
      });
  }

  showTooltip(point, x, y) {
    this.tooltip.classList.remove("hidden");
    this.tooltip.setAttribute("role", "tooltip");

    const dateStr =
      point.date instanceof Date
        ? point.date.toISOString().split("T")[0]
        : point.date;

    const foundationYear = this.years[0];
    const foundationVal = point[foundationYear] || 0;

    const existingYears = Object.keys(point)
      .filter((k) => k !== "date" && k !== "total" && point[k] > 0)
      .sort();
    const oldestSurvivingYear = existingYears[0];
    const oldestSurvivingVal = point[oldestSurvivingYear] || 0;

    const isFoundationAlive = foundationVal > 0;
    const refactoredVal = point.total - foundationVal;

    // Find milestone if close to current date
    let milestoneHTML = "";
    const repoInfo = this.manifest.find((r) => r.name === this.currentRepo);
    if (repoInfo && repoInfo.milestones) {
      const pointDate = new Date(point.date);
      for (const m of repoInfo.milestones) {
        const milestoneDate = new Date(m.date + "-01");
        const monthsDiff = Math.abs(
          (pointDate - milestoneDate) / (1000 * 60 * 60 * 24 * 30),
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

    // Positioning AFTER content injection
    const tooltipWidth = this.tooltip.offsetWidth || 340;
    const tooltipHeight = this.tooltip.offsetHeight || 220;
    const svgRect = this.canvas.getBoundingClientRect();

    let left = x + 15;
    let top = y + 15;

    // Flip if clipping window edges
    if (svgRect.left + left + tooltipWidth > window.innerWidth - 20) {
      left = x - tooltipWidth - 15;
    }
    if (svgRect.top + top + tooltipHeight > window.innerHeight - 20) {
      top = y - tooltipHeight - 15;
    }

    this.tooltip.style.left = `${left}px`;
    this.tooltip.style.top = `${top}px`;
  }

  hideTooltip() {
    this.tooltip.classList.add("hidden");
  }

  updateFocusPoint(xScale, scrubber) {
    const d = this.points[this.focusIndex];
    if (!d) return;

    const snappedX = xScale(d.date);
    scrubber.attr("x1", snappedX).attr("x2", snappedX).classed("hidden", false);

    this.showTooltip(
      d,
      snappedX + this.margin.left,
      this.tooltip.offsetHeight + 20,
    );
  }

  updateInsights() {
    if (!this.points || this.points.length === 0) return;
    const first = this.points[0];
    const last = this.points[this.points.length - 1];

    // 1. Birth Year (Genesis)
    const birthYear = this.years[0];
    document.getElementById("birth-year").textContent = birthYear;

    // 2. Oldest Surviving Year
    let oldestSurviving = "--";
    for (const year of this.years) {
      if (last[year] > 0) {
        oldestSurviving = year;
        break;
      }
    }
    document.getElementById("oldest-line").textContent = oldestSurviving;

    const foundationLines = last[birthYear] || 0;
    const totalLines = last.total || 0;
    if (birthYear && totalLines > 0) {
      const foundationPercent = (foundationLines / totalLines) * 100;
      document.getElementById("percent-replaced").textContent =
        `${foundationPercent.toFixed(1)}%`;
    } else if (totalLines > 0) {
      document.getElementById("percent-replaced").textContent = "0.0%";
    } else {
      document.getElementById("percent-replaced").textContent = "--";
    }

    // Mean Code Age (Weighted average)
    const lastDate = new Date(last.date);
    const currentYear = lastDate.getFullYear();
    if (totalLines > 0) {
      let totalAge = 0;
      this.years.forEach((y) => {
        const lines = last[y] || 0;
        const age = currentYear - parseInt(y, 10);
        totalAge += lines * age;
      });
      const meanAge = totalAge / totalLines;
      document.getElementById("mean-code-age").textContent =
        `${meanAge.toFixed(1)} yrs`;
    } else {
      document.getElementById("mean-code-age").textContent = "0.0 yrs";
    }

    // Peak Preservation (Largest legacy year)
    let peakYear = "--";
    let peakVal = 0;
    this.years.forEach((y) => {
      if (parseInt(y, 10) < currentYear) {
        const val = last[y] || 0;
        if (val > peakVal) {
          peakVal = val;
          peakYear = y;
        }
      }
    });
    document.getElementById("peak-year").textContent = peakYear;

    // 7. Greatest Transformation (Largest single drop in origin)
    let maxDrop = 0;
    let dropDate = "--";
    if (birthYear && this.points.length > 1) {
      for (let i = 1; i < this.points.length; i++) {
        const prev = this.points[i - 1][birthYear] || 0;
        const curr = this.points[i][birthYear] || 0;
        const drop = prev - curr;
        if (drop > maxDrop) {
          maxDrop = drop;
          const d = new Date(this.points[i].date);
          dropDate = d.toLocaleDateString("en-US", {
            month: "short",
            year: "numeric",
          });
        }
      }
    }
    document.getElementById("transformation-date").textContent = dropDate;
  }

  renderFossils() {
    const genesis = this.fossils.genesis || {};
    const survivor = this.fossils.survivor || {};

    const repoInfo = this.manifest.find((r) => r.name === this.currentRepo);
    const repoPath = repoInfo ? repoInfo.repo : null;

    const buildLink = (fossil) => {
      const display = fossil.file ? `${fossil.file}:${fossil.line}` : "--";

      // No file or no repo → plain text node, nothing to link
      if (!fossil.file || !repoPath) return document.createTextNode(display);

      const linkCommit = fossil.view_commit || fossil.commit;
      if (!linkCommit) return document.createTextNode(display);

      // URL-encode the file path (preserve /, encode special chars per segment)
      // and the commit ref (branch names can contain slashes so encode fully)
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

    // Genesis (Historical Fossil) — show the pinned blame commit hash (frozen in history)
    document.getElementById("genesis-year").textContent =
      genesis.year || "----";
    document.getElementById("genesis-file").replaceChildren(buildLink(genesis));
    document.getElementById("genesis-content").textContent = genesis.content
      ? genesis.content.trim()
      : "No fossil data";
    document.getElementById("genesis-commit").textContent =
      genesis.commit || "";

    // Survivor (Living Fossil) — show branch name (e.g. "main"), not old blame hash
    document.getElementById("survivor-year").textContent =
      survivor.year || "----";
    document
      .getElementById("survivor-file")
      .replaceChildren(buildLink(survivor));
    document.getElementById("survivor-content").textContent = survivor.content
      ? survivor.content.trim()
      : "No fossil data";
    document.getElementById("survivor-commit").textContent =
      survivor.view_commit || survivor.commit || "";

    // Keyboard interaction for fossil card divs
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

  createSVGElement(tag, attrs = {}) {
    const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
    Object.entries(attrs).forEach(([key, val]) => el.setAttribute(key, val));
    return el;
  }

  showLoading(show) {
    if (show) {
      this.loadingState.classList.remove("hidden");
      // Also hide the chart container while skeleton shows
      const chartContainer = document.getElementById("chart-container");
      if (chartContainer) chartContainer.style.opacity = "0";
    } else {
      this.loadingState.classList.add("hidden");
      // Fade the chart back in smoothly
      const chartContainer = document.getElementById("chart-container");
      if (chartContainer) {
        chartContainer.style.transition = "opacity 0.35s ease";
        chartContainer.style.opacity = "1";
      }
    }
  }

  showError(msg) {
    const error = document.getElementById("chart-error");
    if (error) {
      error.textContent = msg;
      error.classList.remove("hidden");
    }
  }

  hideError() {
    const error = document.getElementById("chart-error");
    if (error) error.classList.add("hidden");
  }

  debouncedRender() {
    clearTimeout(this.resizeTimer);
    this.resizeTimer = setTimeout(() => {
      if (this.currentData) this.renderChart();
    }, 100);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  new TheseusVisualizer();
});
