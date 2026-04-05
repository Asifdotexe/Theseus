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
        this.canvas = document.getElementById('main-chart');
        this.tooltip = document.getElementById('tooltip');
        this.legend = document.getElementById('chart-legend');
        this.repoSelector = document.getElementById('repo-selector');
        this.repoDescription = document.getElementById('repo-description');
        this.vizToggle = document.getElementById('viz-mode-toggle');
        this.scaleToggle = document.getElementById('scale-toggle');
        this.loadingState = document.getElementById('chart-loading');

        this.margin = { top: 10, right: 0, bottom: 30, left: 50 };
        this.years = [];
        this.points = [];
        this.vizMode = 'chronological'; // 'chronological' | 'identity'
        this.yScaleMode = 'linear'; // 'linear' | 'log'

        this.init();
    }

    async init() {
        try {
            const response = await fetch('data/manifest.json');
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            let data = await response.json();
            this.manifest = Array.isArray(data) ? data : [data];

            this.renderSelectors();
            this.setupModeToggle();
            this.setupScaleToggle();

            if (this.manifest.length > 0) {
                this.loadRepo(this.manifest[0].name);
            }
        } catch (err) {
            this.showError("Failed to load repository manifest: " + err.message);
        }

        window.addEventListener('resize', () => this.debouncedRender());
    }

    setupModeToggle() {
        this.vizToggle.addEventListener('click', (e) => {
            const btn = e.target.closest('.mode-btn');
            if (!btn || btn.classList.contains('active')) return;

            document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            this.vizMode = btn.dataset.mode;
            if (this.currentData) this.renderChart();
        });
    }

    setupScaleToggle() {
        this.scaleToggle.addEventListener('click', (e) => {
            const btn = e.target.closest('.scale-btn');
            if (!btn || btn.classList.contains('active')) return;

            document.querySelectorAll('.scale-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            this.yScaleMode = btn.dataset.scale;
            if (this.currentData) this.renderChart();
        });
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
        if (this.currentRepo === repoName && this.currentData) return;

        this.showLoading(true);
        this.hideError();

        try {
            const repoInfo = this.manifest.find(r => r.name === repoName);
            this.repoDescription.textContent = repoInfo.description || '';

            const response = await fetch(`data/${repoInfo.file}`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            this.currentData = await response.json();

            this.currentRepo = repoName;
            this.updateActiveBtn(repoName);

            this.processData();
            this.renderChart();
            this.updateInsights();
        } catch (err) {
            console.error(err);
            this.showError(`Failed to load data for ${repoName}`);
        } finally {
            this.showLoading(false);
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
        const width = this.canvas.clientWidth;
        const height = this.canvas.clientHeight;
        if (!width || !height) return;

        this.canvas.innerHTML = '';

        // Define Gradients
        const defs = this.createSVGElement('defs');
        if (this.vizMode === 'identity') {
            const gradOriginal = this.createSVGElement('linearGradient', { id: `grad-original`, x1: '0%', y1: '0%', x2: '0%', y2: '100%' });
            gradOriginal.appendChild(this.createSVGElement('stop', { offset: '0%', 'stop-color': `#3bc7c7`, 'stop-opacity': 0.8 }));
            gradOriginal.appendChild(this.createSVGElement('stop', { offset: '100%', 'stop-color': `#3bc7c7`, 'stop-opacity': 0.1 }));
            defs.appendChild(gradOriginal);

            const gradRefactored = this.createSVGElement('linearGradient', { id: `grad-refactored`, x1: '0%', y1: '0%', x2: '0%', y2: '100%' });
            gradRefactored.appendChild(this.createSVGElement('stop', { offset: '0%', 'stop-color': `#f0a33b`, 'stop-opacity': 0.8 }));
            gradRefactored.appendChild(this.createSVGElement('stop', { offset: '100%', 'stop-color': `#f0a33b`, 'stop-opacity': 0.1 }));
            defs.appendChild(gradRefactored);
        } else {
            this.years.forEach((year, i) => {
                const hue = (180 + i * 40) % 360;
                const grad = this.createSVGElement('linearGradient', { id: `grad-${year}`, x1: '0%', y1: '0%', x2: '0%', y2: '100%' });
                grad.appendChild(this.createSVGElement('stop', { offset: '0%', 'stop-color': `hsl(${hue}, 70%, 55%)`, 'stop-opacity': 0.8 }));
                grad.appendChild(this.createSVGElement('stop', { offset: '100%', 'stop-color': `hsl(${hue}, 70%, 55%)`, 'stop-opacity': 0.1 }));
                defs.appendChild(grad);
            });
        }
        this.canvas.appendChild(defs);

        const chartWidth = width - this.margin.left - this.margin.right;
        const chartHeight = height - this.margin.top - this.margin.bottom;

        // Scales
        const xDenominator = Math.max(1, this.points.length - 1);
        const xScale = (i) => (i / xDenominator) * chartWidth;
        const maxVal = Math.max(...this.points.map(p => p.total));

        let yScale;
        if (this.yScaleMode === 'log') {
            const minLog = 0; // log10(1)
            const maxLog = Math.log10(maxVal + 1);
            yScale = (v) => {
                const logV = Math.log10(v + 1);
                return chartHeight - ((logV - minLog) / (maxLog - minLog)) * chartHeight;
            };
        } else {
            yScale = maxVal > 0 ? (v) => chartHeight - (v / maxVal) * chartHeight : (v) => chartHeight;
        }

        // Render Areas
        const group = this.createSVGElement('g', { transform: `translate(${this.margin.left}, ${this.margin.top})` });

        this.years.forEach((year, idx) => {
            const pathData = this.points.map((p, i) => `${xScale(i)},${yScale(p.stack[year].end)}`);
            const bottomData = this.points.map((p, i) => `${xScale(i)},${yScale(p.stack[year].start)}`).reverse();

            let fillUrl;
            if (this.vizMode === 'identity') {
                fillUrl = `url(#grad-${idx === 0 ? 'original' : 'refactored'})`;
            } else {
                fillUrl = `url(#grad-${year})`;
            }

            const areaPath = this.createSVGElement('path', {
                d: `M${pathData.join(' L')} L${bottomData.join(' L')} Z`,
                fill: fillUrl,
                class: 'chart-area',
                'data-year': year
            });

            areaPath.style.opacity = '0';
            areaPath.style.transition = 'opacity 1.5s ease-out';
            group.appendChild(areaPath);
            setTimeout(() => areaPath.style.opacity = '1', 50);
        });

        // Legend
        this.renderLegend();

        // Axes
        this.renderAxes(group, chartWidth, chartHeight, xScale, yScale, maxVal);

        // Interaction
        const overlay = this.createSVGElement('rect', { width: chartWidth, height: chartHeight, fill: 'transparent' });
        overlay.onmousemove = (e) => {
            const svgRect = this.canvas.getBoundingClientRect();
            const mouseX = e.clientX - svgRect.left - this.margin.left;
            const index = Math.round((mouseX / chartWidth) * (this.points.length - 1));
            if (index >= 0 && index < this.points.length) {
                this.showTooltip(this.points[index], e.clientX - svgRect.left, e.clientY - svgRect.top);
            }
        };
        overlay.onmouseleave = () => this.hideTooltip();
        group.appendChild(overlay);

        this.canvas.appendChild(group);
    }

    renderLegend() {
        this.legend.innerHTML = '';
        const items = this.vizMode === 'identity'
            ? [{ label: 'Original Code', color: '#3bc7c7' }, { label: 'Refactored', color: '#f0a33b' }]
            : this.years.map((y, i) => ({ label: y, color: `hsl(${(180 + i * 40) % 360}, 70%, 55%)` }));

        items.forEach(item => {
            const div = document.createElement('div');
            div.className = 'legend-item';
            div.innerHTML = `
                <span class="color-dot" style="background: ${item.color}; box-shadow: 0 0 10px ${item.color}44"></span>
                <span>${item.label}</span>
            `;
            this.legend.appendChild(div);
        });
    }

    renderAxes(group, width, height, xScale, yScale, maxVal) {
        const formatValue = (v) => {
            if (v >= 1000000) return `${(v / 1000000).toFixed(1)}M`;
            if (v >= 1000) return `${(v / 1000).toFixed(1)}k`;
            return Math.round(v);
        };

        let lastY = -100;
        const minGap = 20;

        if (this.yScaleMode === 'log') {
            let val = 1;
            while (val <= maxVal * 10) {
                const y = yScale(Math.min(val, maxVal));
                if (y >= 0 && y <= height && Math.abs(y - lastY) > minGap) {
                    group.appendChild(this.createSVGElement('line', { x1: 0, x2: width, y1: y, y2: y, stroke: '#374151', 'stroke-dasharray': '3,3', 'stroke-opacity': 0.5 }));
                    const label = this.createSVGElement('text', { x: -10, y: y + 4, 'text-anchor': 'end', fill: '#6b7280', 'font-size': '10px' });
                    label.textContent = formatValue(val);
                    group.appendChild(label);
                    lastY = y;
                }
                val *= 10;
                if (val === 10 && maxVal < 1) break;
            }
        } else {
            const tickCount = 5;
            for (let i = 0; i <= tickCount; i++) {
                const val = (i / tickCount) * maxVal;
                const y = yScale(val);
                if (Math.abs(y - lastY) > minGap) {
                    group.appendChild(this.createSVGElement('line', { x1: 0, x2: width, y1: y, y2: y, stroke: '#374151', 'stroke-dasharray': '3,3', 'stroke-opacity': 0.5 }));
                    const label = this.createSVGElement('text', { x: -10, y: y + 4, 'text-anchor': 'end', fill: '#6b7280', 'font-size': '10px' });
                    label.textContent = formatValue(val);
                    group.appendChild(label);
                    lastY = y;
                }
            }
        }

        const xStep = Math.max(1, Math.floor(this.points.length / 6));
        this.points.forEach((p, i) => {
            if (i % xStep === 0 || p.date.endsWith('-01')) {
                const label = this.createSVGElement('text', { x: xScale(i), y: height + 25, 'text-anchor': 'middle', fill: '#6b7280', 'font-size': '10px' });
                label.textContent = p.date.endsWith('-01') ? p.date.split('-')[0] : (i % xStep === 0 ? p.date : '');
                if (label.textContent) group.appendChild(label);
            }
        });
    }

    showTooltip(point, x, y) {
        this.tooltip.classList.remove('hidden');

        // Initial placement
        let left = x + 15;
        let top = y + 15;

        // Get bounds
        const tooltipWidth = this.tooltip.offsetWidth;
        const tooltipHeight = this.tooltip.offsetHeight;
        const containerWidth = document.body.clientWidth;
        const svgRect = this.canvas.getBoundingClientRect();

        // Horizontal flip if too close to right edge
        if (svgRect.left + left + tooltipWidth > containerWidth - 20) {
            left = x - tooltipWidth - 15;
        }

        // Vertical flip if too close to bottom (relative to viewport)
        if (svgRect.top + top + tooltipHeight > window.innerHeight - 20) {
            top = y - tooltipHeight - 15;
        }

        this.tooltip.style.left = `${left}px`;
        this.tooltip.style.top = `${top}px`;

        const getColor = (idx, year) => {
            if (this.vizMode === 'identity') return idx === 0 ? '#3bc7c7' : '#f0a33b';
            const yearIdx = this.years.indexOf(year);
            return `hsl(${(180 + yearIdx * 40) % 360}, 70%, 55%)`;
        };

        let compositionHtml = '';
        if (this.vizMode === 'identity') {
            const oldestYear = this.years[0];
            const originalVal = point.composition[oldestYear] || 0;
            const refactoredVal = point.total - originalVal;

            compositionHtml += `
                <div class="tooltip-item">
                    <div class="label-group">
                        <span class="color-dot" style="background: #3bc7c7"></span>
                        <span>Original (${oldestYear})</span>
                    </div>
                    <div class="value-group">
                        <strong>${originalVal.toLocaleString()}</strong>
                        <span class="percent-tag">${((originalVal / point.total) * 100).toFixed(1)}%</span>
                    </div>
                </div>
                <div class="tooltip-item">
                    <div class="label-group">
                        <span class="color-dot" style="background: #f0a33b"></span>
                        <span>Refactored</span>
                    </div>
                    <div class="value-group">
                        <strong>${refactoredVal.toLocaleString()}</strong>
                        <span class="percent-tag">${((refactoredVal / point.total) * 100).toFixed(1)}%</span>
                    </div>
                </div>
                <div class="tooltip-divider"></div>
            `;
        }

        this.years.slice().sort((a, b) => b - a).forEach(year => {
            const val = point.composition[year] || 0;
            if (val > 0) {
                const yearColor = getColor(null, year);
                compositionHtml += `
                    <div class="tooltip-item">
                        <div class="label-group">
                            <span class="color-dot" style="background: ${yearColor}"></span>
                            <span>${year}</span>
                        </div>
                        <div class="value-group">
                            <strong>${val.toLocaleString()}</strong>
                            <span class="percent-tag">${((val / point.total) * 100).toFixed(1)}%</span>
                        </div>
                    </div>
                `;
            }
        });

        this.tooltip.innerHTML = `
            <div class="tooltip-header">Snapshot: ${point.date}</div>
            <div class="tooltip-item" style="margin-bottom: 0.5rem; opacity: 0.8">
                <span>Total Project Size</span>
                <strong>${point.total.toLocaleString()} lines</strong>
            </div>
            <div class="tooltip-divider"></div>
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

        let originalYear = this.years[0];
        if (!originalYear || first.total_lines === 0) {
            document.getElementById('percent-replaced').textContent = '--';
        } else {
            const originalLinesInFirst = first.composition[originalYear] || 0;
            const originalLinesInLast = last.composition[originalYear] || 0;
            const replaced = ((originalLinesInFirst - originalLinesInLast) / originalLinesInFirst) * 100;
            document.getElementById('percent-replaced').textContent = `${Math.min(100, Math.max(0, replaced)).toFixed(1)}%`;
        }
        document.getElementById('oldest-line').textContent = this.years[0];
    }

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
        if (error) {
            error.textContent = msg;
            error.classList.remove('hidden');
        }
    }

    hideError() {
        const error = document.getElementById('chart-error');
        if (error) error.classList.add('hidden');
    }

    debouncedRender() {
        clearTimeout(this.resizeTimer);
        this.resizeTimer = setTimeout(() => {
            if (this.currentData) this.renderChart();
        }, 100);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new TheseusVisualizer();
});
