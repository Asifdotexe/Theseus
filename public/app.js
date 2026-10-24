/**
 * Ship of Theseus - Code Visualizer
 * Core Entry Point (ES6 Module)
 */

import { fetchConfig, fetchRepoData, processSnapshots } from "./api.js";
import { ChartRenderer } from "./chart.js";
import {
  attachLegendHandlers,
  hideError,
  renderFossils,
  renderLegend,
  setupKeyboardShortcuts,
  setupRepoRequest,
  showError,
  showLoading,
  TooltipManager,
  updateInsights,
} from "./ui.js";

class TheseusVisualizer {
  constructor() {
    this.manifest = [];
    this.globalMilestones = [];
    this.currentData = null;
    this.currentRepo = null;
    this.years = [];
    this.points = [];
    this.fossils = {};

    this.vizMode = "chronological"; // 'chronological' | 'identity'
    this.yScaleMode = "linear"; // 'linear' | 'log'

    this.canvas = document.getElementById("main-chart");
    this.tooltipEl = document.getElementById("tooltip");
    this.legend = document.getElementById("chart-legend");
    this.repoSelector = document.getElementById("repo-selector");
    this.repoDescription = document.getElementById("repo-description");
    this.vizToggle = document.getElementById("viz-mode-toggle");
    this.scaleToggle = document.getElementById("scale-toggle");
    this.loadingState = document.getElementById("chart-loading");
    this.chartContainer = document.getElementById("chart-container");
    this.chartError = document.getElementById("chart-error");

    this.reducedMotion = window.matchMedia(
      "(prefers-reduced-motion: reduce)"
    ).matches;
    this.animDuration = this.reducedMotion ? 0 : 800;

    this.abortController = null;
    this.resizeTimer = null;

    this.tooltipManager = new TooltipManager(this.tooltipEl, this.canvas);
    this.chartRenderer = new ChartRenderer(this.canvas, {
      margin: { top: 10, right: 20, bottom: 50, left: 60 },
      onHover: (point, x, y) => {
        this.tooltipManager.show(
          point,
          x,
          y,
          this.years,
          this.getCurrentMilestones(),
          this.chartRenderer.margin
        );
      },
      onLeave: () => {
        this.tooltipManager.hide();
      },
    });

    this.init();
  }

  async init() {
    try {
      const { manifest, globalMilestones } = await fetchConfig();
      this.manifest = manifest;
      this.globalMilestones = globalMilestones;

      this.renderSelectors();
      this.setupModeToggle();
      this.setupScaleToggle();

      if (this.manifest.length > 0) {
        this.loadRepo(this.manifest[0].name);
      }
    } catch (err) {
      showError(this.chartError, "Failed to load repository manifest: " + err.message);
    }

    window.addEventListener("resize", () => this.debouncedRender());

    setupKeyboardShortcuts({
      onSelectRepoIndex: (idx) => {
        const btns = document.querySelectorAll(".repo-btn");
        if (idx < btns.length) btns[idx].click();
      },
      onToggleMode: () => {
        const active = document.querySelector(".mode-btn.active");
        if (active) {
          const next =
            active.nextElementSibling || active.previousElementSibling;
          if (next) next.click();
        }
      },
      onToggleScale: () => {
        const active = document.querySelector(".scale-btn.active");
        if (active) {
          const next =
            active.nextElementSibling || active.previousElementSibling;
          if (next) next.click();
        }
      },
    });

    setupRepoRequest();
    attachLegendHandlers(this.legend);
  }

  setupModeToggle() {
    if (!this.vizToggle) return;
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
    if (!this.scaleToggle) return;
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

  renderSelectors() {
    if (!this.repoSelector) return;
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

  updateActiveBtn(name) {
    document.querySelectorAll(".repo-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.repo === name);
    });
  }

  getCurrentMilestones() {
    const repoInfo = this.manifest.find((r) => r.name === this.currentRepo);
    const repoMilestones = repoInfo && repoInfo.milestones ? repoInfo.milestones : [];
    return [...this.globalMilestones, ...repoMilestones];
  }

  async loadRepo(repoName) {
    if (this.currentRepo === repoName && this.currentData) return;

    if (this.abortController) {
      this.abortController.abort();
    }
    this.abortController = new AbortController();
    const signal = this.abortController.signal;

    showLoading(this.loadingState, this.chartContainer, true);
    hideError(this.chartError);

    try {
      const repoInfo = this.manifest.find((r) => r.name === repoName);
      if (!repoInfo) {
        showError(this.chartError, "Repository not found: " + repoName);
        return;
      }
      if (this.repoDescription) {
        this.repoDescription.textContent = repoInfo.description || "";
      }

      const { snapshots, fossils } = await fetchRepoData(repoInfo.name, signal);
      const { currentData, years, points } = processSnapshots(snapshots);

      this.currentData = currentData;
      this.years = years;
      this.points = points;
      this.fossils = fossils;
      this.currentRepo = repoName;

      this.updateActiveBtn(repoName);
      this.renderChart();
      updateInsights(this.points, this.years);
      renderFossils(this.fossils, repoInfo.repo);
    } catch (err) {
      if (err.name === "AbortError") return;
      console.error(err);
      showError(this.chartError, `Failed to load data for ${repoName}`);
    } finally {
      if (!signal.aborted) {
        showLoading(this.loadingState, this.chartContainer, false);
      }
    }
  }

  renderChart() {
    if (!this.points || this.points.length === 0) {
      showError(this.chartError, "No snapshot data to display.");
      return;
    }

    this.chartRenderer.render({
      points: this.points,
      years: this.years,
      vizMode: this.vizMode,
      yScaleMode: this.yScaleMode,
      milestones: this.getCurrentMilestones(),
      animDuration: this.animDuration,
      reducedMotion: this.reducedMotion,
    });

    renderLegend(this.legend, this.years, this.vizMode);
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
