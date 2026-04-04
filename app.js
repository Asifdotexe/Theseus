/**
 * Ship of Theseus - Code Visualizer
 * Core Logic (Vanilla JS + SVG)
 */

class TheseusVisualizer {
    constructor() {
        this.manifest = null;
        this.currentData = null;
        this.currentRepo = null;
        this.canvas = document.getElementById('main-chart');
        this.tooltip = document.getElementById('tooltip');
        this.repoSelector = document.getElementById('repo-selector');
        this.repoDescription = document.getElementById('repo-description');
        this.loadingState = document.getElementById('chart-loading');
        
        this.margin = { top: 40, right: 20, bottom: 60, left: 60 };
        this.years = [];
        this.points = [];
        
        this.init();
    }

    async init() {
        try {
            const response = await fetch('data/manifest.json');
            let data = await response.json();
            
            // Normalize manifest to array
            this.manifest = Array.isArray(data) ? data : [data];
            
            this.renderSelectors();
            if (this.manifest.length > 0) {
                this.loadRepo(this.manifest[0].name);
            }
        } catch (err) {
            this.showError("Failed to load repository manifest.");
        }

        window.addEventListener('resize', () => this.debouncedRender());
    }

    renderSelectors() {
        this.repoSelector.innerHTML = '';
        this.manifest.forEach(repo => {
            const btn = document.createElement('button');
            btn.className = 'repo-btn';
            btn.textContent = repo.name.replace(/-/g, ' ');
            btn.dataset.repo = repo.name;
            btn.onclick = () => this.loadRepo(repo.name);
            this.repoSelector.appendChild(btn);
        });
    }

    async loadRepo(repoName) {
        if (this.currentRepo === repoName) return;
        
        this.currentRepo = repoName;
        this.updateActiveBtn(repoName);
        this.showLoading(true);
        this.hideError();

        try {
            const repoInfo = this.manifest.find(r => r.name === repoName);
            const fileName = repoInfo.file;
            this.repoDescription.textContent = repoInfo.description || '';
            
            const response = await fetch(`data/${fileName}`);
            this.currentData = await response.json();
            
            this.processData();
            this.renderChart();
            this.updateInsights();
            this.showLoading(false);
        } catch (err) {
            console.error(err);
            this.showError(`Failed to load data for ${repoName}`);
        }
    }

    updateActiveBtn(name) {
        document.querySelectorAll('.repo-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.repo === name);
        });
    }

    processData() {
        // Collect all composing years
        const yearSet = new Set();
        this.currentData.forEach(d => {
            Object.keys(d.composition).forEach(y => yearSet.add(y));
        });
        this.years = Array.from(yearSet).sort();

        // Map data points
        this.points = this.currentData.map(d => {
            const point = {
                date: d.snapshot_date,
                total: d.total_lines,
                composition: d.composition,
                stack: {}
            };
            
            let cumulative = 0;
            this.years.forEach(year => {
                const value = d.composition[year] || 0;
                point.stack[year] = {
                    start: cumulative,
                    end: cumulative + value,
                    value: value
                };
                cumulative += value;
            });
            
            return point;
        });
    }

    renderChart() {
        const { width, height } = this.canvas.getBoundingClientRect();
        this.canvas.innerHTML = ''; // Clear previous

        // Define Gradients
        const defs = this.createSVGElement('defs');
        this.years.forEach((year, i) => {
            const hue = (180 + i * 40) % 360; // Spread colors starting from Cyan
            const grad = this.createSVGElement('linearGradient', {
                id: `grad-${year}`,
                x1: '0%', y1: '0%', x2: '0%', y2: '100%'
            });
            grad.appendChild(this.createSVGElement('stop', { offset: '0%', 'stop-color': `hsl(${hue}, 70%, 55%)`, 'stop-opacity': 0.8 }));
            grad.appendChild(this.createSVGElement('stop', { offset: '100%', 'stop-color': `hsl(${hue}, 70%, 55%)`, 'stop-opacity': 0.1 }));
            defs.appendChild(grad);
        });
        this.canvas.appendChild(defs);

        const chartWidth = width - this.margin.left - this.margin.right;
        const chartHeight = height - this.margin.top - this.margin.bottom;

        // Scales
        const xScale = (i) => (i / (this.points.length - 1)) * chartWidth;
        const maxVal = Math.max(...this.points.map(p => p.total));
        const yScale = (v) => chartHeight - (v / maxVal) * chartHeight;

        // Render Areas
        const group = this.createSVGElement('g', { transform: `translate(${this.margin.left}, ${this.margin.top})` });
        
        this.years.forEach(year => {
            const pathData = this.points.map((p, i) => `${xScale(i)},${yScale(p.stack[year].end)}`);
            const bottomData = this.points.map((p, i) => `${xScale(i)},${yScale(p.stack[year].start)}`).reverse();
            
            const areaPath = this.createSVGElement('path', {
                d: `M${pathData.join(' L')} L${bottomData.join(' L')} Z`,
                fill: `url(#grad-${year})`,
                class: 'chart-area',
                'data-year': year
            });
            
            // Initial state for animation
            areaPath.style.opacity = '0';
            areaPath.style.transition = 'opacity 1.5s ease-out';
            group.appendChild(areaPath);
            
            setTimeout(() => areaPath.style.opacity = '1', 50);
        });

        // Axes (Subtle)
        this.renderAxes(group, chartWidth, chartHeight, xScale, yScale, maxVal);

        // Interaction Overlay
        const overlay = this.createSVGElement('rect', {
            width: chartWidth,
            height: chartHeight,
            fill: 'transparent'
        });
        
        overlay.onmousemove = (e) => this.handleMouseMove(e, chartWidth, xScale);
        overlay.onmouseleave = () => this.hideTooltip();
        group.appendChild(overlay);

        this.canvas.appendChild(group);
    }

    renderAxes(group, width, height, xScale, yScale, maxVal) {
        // Horizontal Grid Lines
        const tickCount = 5;
        for (let i = 0; i <= tickCount; i++) {
            const val = (i / tickCount) * maxVal;
            const y = yScale(val);
            
            const line = this.createSVGElement('line', {
                x1: 0, x2: width, y1: y, y2: y,
                stroke: '#374151', 'stroke-dasharray': '3,3', 'stroke-opacity': 0.5
            });
            group.appendChild(line);

            const label = this.createSVGElement('text', {
                x: -10, y: y + 4, 'text-anchor': 'end', fill: '#6b7280', 'font-size': '10px'
            });
            label.textContent = val >= 1000 ? `${(val/1000).toFixed(1)}k` : Math.round(val);
            group.appendChild(label);
        }

        // X Axis labels (Show every few snapshots)
        const xStep = Math.max(1, Math.floor(this.points.length / 6));
        this.points.forEach((p, i) => {
            if (i % xStep === 0 || p.date.endsWith('-01')) {
                const label = this.createSVGElement('text', {
                    x: xScale(i), y: height + 25, 'text-anchor': 'middle', fill: '#6b7280', 'font-size': '10px'
                });
                label.textContent = p.date.endsWith('-01') ? p.date.split('-')[0] : (i % xStep === 0 ? p.date : '');
                if (label.textContent) group.appendChild(label);
            }
        });
    }

    handleMouseMove(e, chartWidth, xScale) {
        const svgRect = this.canvas.getBoundingClientRect();
        const mouseX = e.clientX - svgRect.left - this.margin.left;
        
        // Find nearest point
        const index = Math.round((mouseX / chartWidth) * (this.points.length - 1));
        if (index >= 0 && index < this.points.length) {
            const p = this.points[index];
            this.showTooltip(p, e.clientX, e.clientY);
        }
    }

    showTooltip(point, x, y) {
        this.tooltip.classList.remove('hidden');
        this.tooltip.style.left = `${x + 15}px`;
        this.tooltip.style.top = `${y + 15}px`;

        let compositionHtml = '';
        this.years.slice().sort((a,b) => b-a).forEach(year => {
            const val = point.composition[year] || 0;
            if (val > 0) {
                const pct = ((val / point.total) * 100).toFixed(1);
                compositionHtml += `
                    <div class="tooltip-item">
                        <span>Code from ${year}:</span>
                        <strong>${val.toLocaleString()} lines (${pct}%)</strong>
                    </div>
                `;
            }
        });

        this.tooltip.innerHTML = `
            <div class="tooltip-header">Snapshot: ${point.date}</div>
            <div class="tooltip-item">
                <span>Total Size:</span>
                <strong>${point.total.toLocaleString()} lines</strong>
            </div>
            ${compositionHtml}
        `;
    }

    hideTooltip() {
        this.tooltip.classList.add('hidden');
    }

    updateInsights() {
        if (!this.currentData || this.currentData.length === 0) return;

        const first = this.currentData[0];
        const last = this.currentData[this.currentData.length - 1];

        // % Replaced calculation
        const originalYear = Object.keys(first.composition)[0];
        const originalLinesInLast = last.composition[originalYear] || 0;
        const originalLinesInFirst = first.total_lines;
        
        const replaced = ((originalLinesInFirst - originalLinesInLast) / originalLinesInFirst) * 100;
        document.getElementById('percent-replaced').textContent = `${Math.min(100, Math.max(0, replaced)).toFixed(1)}%`;

        // Oldest line logic
        const oldestYear = this.years[0];
        document.getElementById('oldest-line').textContent = oldestYear;
    }

    // Utilities
    createSVGElement(tag, attrs = {}) {
        const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
        Object.entries(attrs).forEach(([key, val]) => el.setAttribute(key, val));
        return el;
    }

    showLoading(show) {
        this.loadingState.classList.toggle('hidden', !show);
    }

    showError(msg) {
        const error = document.getElementById('chart-error');
        error.textContent = msg;
        error.classList.remove('hidden');
    }

    hideError() {
        document.getElementById('chart-error').classList.add('hidden');
    }

    debouncedRender() {
        clearTimeout(this.resizeTimer);
        this.resizeTimer = setTimeout(() => {
            if (this.currentData) this.renderChart();
        }, 100);
    }
}

// Spark the void
document.addEventListener('DOMContentLoaded', () => {
    new TheseusVisualizer();
});
