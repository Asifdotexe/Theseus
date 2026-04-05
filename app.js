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
        const yearSet = new Set();
        this.currentData.forEach(d => {
            Object.keys(d.composition).forEach(y => yearSet.add(y));
        });
        this.years = Array.from(yearSet).sort();

        // Convert to D3 stack-ready format
        this.points = this.currentData.map(d => {
            const point = {
                date: new Date(d.snapshot_date),
                total: d.total_lines
            };
            this.years.forEach(year => {
                point[year] = d.composition[year] || 0;
            });
            return point;
        });
    }

    renderChart() {
        const width = this.canvas.clientWidth;
        const height = this.canvas.clientHeight;
        if (!width || !height) return;

        const chartWidth = width - this.margin.left - this.margin.right;
        const chartHeight = height - this.margin.top - this.margin.bottom;

        const svg = d3.select(this.canvas);
        svg.selectAll("*").remove();

        // Containers
        const g = svg.append("g")
            .attr("transform", `translate(${this.margin.left},${this.margin.top})`);

        // Scales
        const xScale = d3.scaleTime()
            .domain(d3.extent(this.points, d => d.date))
            .range([0, chartWidth]);

        const maxTotal = d3.max(this.points, d => d.total);
        let yScale;
        if (this.yScaleMode === 'log') {
            yScale = d3.scaleLog()
                .domain([1, maxTotal * 1.1])
                .range([chartHeight, 0])
                .clamp(true);
        } else {
            yScale = d3.scaleLinear()
                .domain([0, maxTotal * 1.05])
                .range([chartHeight, 0]);
        }

        // Color Logic & Gradients
        const defs = svg.append("defs");

        const getBaseColor = (seriesName, seriesIndex) => {
            if (this.vizMode === 'identity') {
                return (seriesIndex === 0) ? '#3bc7c7' : '#f0a33b';
            }
            const yearIdx = this.years.indexOf(seriesName);
            return `hsl(${(180 + yearIdx * 40) % 360}, 70%, 55%)`;
        };

        // Create gradients for each series
        const seriesKeys = this.vizMode === 'identity' ? [this.years[0], 'refactored'] : this.years;
        this.years.forEach((year, i) => {
            const color = getBaseColor(year, i);
            const grad = defs.append("linearGradient")
                .attr("id", `grad-${year}`)
                .attr("x1", "0%").attr("y1", "0%")
                .attr("x2", "0%").attr("y2", "100%");

            grad.append("stop").attr("offset", "0%").attr("stop-color", color).attr("stop-opacity", 0.6);
            grad.append("stop").attr("offset", "100%").attr("stop-color", color).attr("stop-opacity", 0.05);
        });

        // Specialized gradients for Identity mode if needed
        if (this.vizMode === 'identity') {
            ['original', 'refactored'].forEach(id => {
                const color = id === 'original' ? '#3bc7c7' : '#f0a33b';
                const grad = defs.append("linearGradient")
                    .attr("id", `grad-id-${id}`)
                    .attr("x1", "0%").attr("y1", "0%")
                    .attr("x2", "0%").attr("y2", "100%");
                grad.append("stop").attr("offset", "0%").attr("stop-color", color).attr("stop-opacity", 0.6);
                grad.append("stop").attr("offset", "100%").attr("stop-color", color).attr("stop-opacity", 0.05);
            });
        }

        // Stack & Area
        const stackGenerator = d3.stack()
            .keys(this.years);

        const stackedData = stackGenerator(this.points);

        const areaGenerator = d3.area()
            .x(d => xScale(d.data.date))
            .y0(d => yScale(this.yScaleMode === 'log' ? Math.max(1, d[0]) : d[0]))
            .y1(d => yScale(this.yScaleMode === 'log' ? Math.max(1, d[1]) : d[1]))
            .curve(d3.curveMonotoneX);

        // Render Layers (Data Join)
        const layers = g.selectAll(".layer")
            .data(stackedData, d => d.key);

        const getFill = (d, i) => {
            if (this.vizMode === 'identity') {
                const id = i === 0 ? 'original' : 'refactored';
                return `url(#grad-id-${id})`;
            }
            return `url(#grad-${d.key})`;
        };

        layers.enter().append("path")
            .attr("class", "chart-area layer")
            .attr("data-year", d => d.key)
            .attr("fill", getFill)
            .attr("d", areaGenerator)
            .style("opacity", 0)
            .transition()
            .duration(800)
            .style("opacity", 1);

        layers.transition()
            .duration(800)
            .attr("d", areaGenerator)
            .attr("fill", getFill);

        layers.exit().remove();

        // Interaction Components (Legend, Axes, Scrubber)
        this.renderLegend();
        this.renderAxes(g, chartWidth, chartHeight, xScale, yScale);
        this.setupInteractivity(g, chartWidth, chartHeight, xScale, yScale);
    }

    renderLegend() {
        this.legend.innerHTML = '';
        const items = this.vizMode === 'identity'
            ? [{ label: 'Original Code', color: '#3bc7c7' }, { label: 'Refactored', color: '#f0a33b' }]
            : this.years.map((y, i) => ({ label: y, color: `hsl(${(180 + i * 40) % 360}, 70%, 55%)` }));

        items.forEach(item => {
            const div = document.createElement('div');
            div.className = 'legend-item';
            div.style.cursor = 'pointer';
            div.innerHTML = `
                <span class="color-dot" style="background: ${item.color}; box-shadow: 0 0 10px ${item.color}44"></span>
                <span>${item.label}</span>
            `;

            div.onmouseenter = () => {
                const label = item.label;
                const firstYear = this.years[0];

                d3.selectAll(".chart-area").style("opacity", 0.1);

                if (this.vizMode === 'identity') {
                    if (label === 'Original Code') {
                        d3.selectAll(`.chart-area[data-year='${firstYear}']`).style("opacity", 1);
                    } else {
                        // All years except the first one
                        d3.selectAll(".chart-area")
                            .filter(function () { return d3.select(this).attr("data-year") !== firstYear; })
                            .style("opacity", 1);
                    }
                } else {
                    d3.selectAll(`.chart-area[data-year='${label}']`).style("opacity", 1);
                }
            };

            div.onmouseleave = () => {
                d3.selectAll(".chart-area").style("opacity", 1);
            };

            this.legend.appendChild(div);
        });
    }

    renderAxes(g, width, height, xScale, yScale) {
        // Y Axis - Custom Grid & Labels
        const yAxis = d3.axisLeft(yScale)
            .ticks(5)
            .tickFormat(v => {
                if (v >= 1000000) return `${(v / 1000000).toFixed(1)}M`;
                if (v >= 1000) return `${(v / 1000).toFixed(1)}k`;
                return Math.round(v);
            })
            .tickSize(-width);

        const yGroup = g.append("g")
            .attr("class", "axis-y")
            .call(yAxis);

        yGroup.selectAll(".tick line")
            .attr("stroke", "#374151")
            .attr("stroke-dasharray", "3,3")
            .attr("stroke-opacity", 0.5);

        yGroup.selectAll("text")
            .attr("x", -10)
            .attr("fill", "#6b7280")
            .attr("font-size", "10px")
            .attr("font-family", "inherit");

        yGroup.select(".domain").remove();

        // X Axis
        const xAxis = d3.axisBottom(xScale)
            .ticks(Math.min(this.points.length, 6))
            .tickFormat(d3.timeFormat("%Y"));

        const xGroup = g.append("g")
            .attr("class", "axis-x")
            .attr("transform", `translate(0,${height})`)
            .call(xAxis);

        xGroup.selectAll("text")
            .attr("y", 15)
            .attr("fill", "#8b949e")
            .attr("font-size", "11px")
            .attr("letter-spacing", "0.05em")
            .attr("font-family", "inherit");

        xGroup.select(".domain").attr("stroke", "rgba(255, 255, 255, 0.1)");
        xGroup.selectAll(".tick line").attr("stroke", "rgba(255, 255, 255, 0.1)");
    }

    setupInteractivity(g, width, height, xScale, yScale) {
        const scrubber = g.append("line")
            .attr("class", "scrubber-line hidden")
            .attr("y1", 0)
            .attr("y2", height)
            .attr("stroke", "rgba(255,255,255,0.2)")
            .attr("stroke-width", 1);

        const bisect = d3.bisector(d => d.date).left;

        g.append("rect")
            .attr("width", width)
            .attr("height", height)
            .attr("fill", "transparent")
            .on("mousemove", (event) => {
                const mouseX = d3.pointer(event)[0];
                const date = xScale.invert(mouseX);
                const idx = bisect(this.points, date, 1);
                const d0 = this.points[idx - 1];
                const d1 = this.points[idx];
                if (!d0 || !d1) return;
                const d = date - d0.date > d1.date - date ? d1 : d0;

                const snappedX = xScale(d.date);
                scrubber.attr("x1", snappedX).attr("x2", snappedX).classed("hidden", false);

                const svgRect = this.canvas.getBoundingClientRect();
                this.showTooltip(d, snappedX + this.margin.left, d3.pointer(event)[1] + this.margin.top);
            })
            .on("mouseleave", () => {
                this.hideTooltip();
                scrubber.classed("hidden", true);
            });
    }

    showTooltip(point, x, y) {
        this.tooltip.classList.remove('hidden');

        const dateStr = point.date instanceof Date
            ? point.date.toISOString().split('T')[0]
            : point.date;

        const oldestYear = this.years[0];
        const originalVal = point[oldestYear] || 0;

        // Find previous point to detect refactor
        const idx = this.points.indexOf(point);
        const prev = idx > 0 ? this.points[idx - 1] : null;
        const prevOldVal = prev ? (prev[oldestYear] || 0) : null;
        const isRefactor = prevOldVal && originalVal < prevOldVal * 0.85;

        const evolutionVal = point.total - originalVal;

        let refactorHTML = '';
        if (originalVal === 0) {
            refactorHTML = `
                <div style="background: rgba(248, 113, 113, 0.15); border: 1px solid rgba(248, 113, 113, 0.4); 
                            padding: 1rem; border-radius: 1rem; margin-bottom: 1.25rem; color: #f87171; 
                            font-size: 0.85rem; line-height: 1.5;">
                    <strong style="display: block; margin-bottom: 0.35rem; text-transform: uppercase; letter-spacing: 0.05em;">Ship of Theseus: The Great Rebirth</strong>
                    The original source code is now entirely gone.<br/><strong>Is this still the same codebase?</strong>
                </div>
            `;
        } else if (isRefactor) {
            refactorHTML = `
                <div style="background: rgba(240, 163, 59, 0.15); border: 1px solid rgba(240, 163, 59, 0.4); 
                            padding: 0.75rem; border-radius: 0.75rem; margin-bottom: 1rem; color: #f0a33b; 
                            font-size: 0.85rem; line-height: 1.4;">
                    <strong style="display: block; margin-bottom: 0.25rem;">Ship of Theseus: Major Refactor</strong>
                    A significant part of the original source was refactored here.<br/>How much can you change before the identity shifts?
                </div>
            `;
        }

        this.tooltip.innerHTML = `
            ${refactorHTML}
            <div class="tooltip-header">Snapshot: ${dateStr}</div>
            <div class="tooltip-item" style="margin-bottom: 0.5rem; opacity: 0.9">
                <span class="label-group">Total Project Size</span>
                <strong class="value-group">${point.total.toLocaleString()} lines</strong>
            </div>
            <div class="tooltip-divider"></div>
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
                    <strong>${evolutionVal.toLocaleString()}</strong>
                    <span class="percent-tag">${((evolutionVal / point.total) * 100).toFixed(1)}%</span>
                </div>
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
        this.tooltip.classList.add('hidden');
    }

    updateInsights() {
        if (!this.points || this.points.length === 0) return;
        const first = this.points[0];
        const last = this.points[this.points.length - 1];

        // 1. Birth Year (Genesis)
        const birthYear = this.years[0];
        document.getElementById('birth-year').textContent = birthYear;

        // 2. Oldest Surviving Year
        let oldestSurviving = '--';
        for (const year of this.years) {
            if (last[year] > 0) {
                oldestSurviving = year;
                break;
            }
        }
        document.getElementById('oldest-line').textContent = oldestSurviving;

        if (birthYear && first.total > 0) {
            const originalLinesInFirst = first[birthYear] || 0;
            const originalLinesInLast = last[birthYear] || 0;
            const replaced = ((originalLinesInFirst - originalLinesInLast) / originalLinesInFirst) * 100;
            document.getElementById('percent-replaced').textContent = `${Math.min(100, Math.max(0, replaced)).toFixed(1)}%`;
        } else {
            document.getElementById('percent-replaced').textContent = '--';
        }

        // 4. Modernization Velocity (Δ Old Code / Δ Time)
        const lastDate = new Date(last.date);
        const currentYear = lastDate.getFullYear();
        const oldThreshold = currentYear - 3;

        // Find snapshot approx 6 months ago (180 days)
        const targetMs = lastDate.getTime() - (180 * 24 * 60 * 60 * 1000);
        let prevSnapshot = this.points[0];
        for (let i = this.points.length - 1; i >= 0; i--) {
            if (new Date(this.points[i].date).getTime() <= targetMs) {
                prevSnapshot = this.points[i];
                break;
            }
        }

        const getOldLines = (snap) => {
            return this.years
                .filter(y => y <= oldThreshold)
                .reduce((sum, y) => sum + (snap[y] || 0), 0);
        };

        const oldNow = getOldLines(last);
        const oldThen = getOldLines(prevSnapshot);
        const months = Math.max(1, (lastDate - new Date(prevSnapshot.date)) / (30 * 24 * 60 * 60 * 1000));
        const velocity = (oldThen - oldNow) / months;

        const velEl = document.getElementById('modernization-velocity');
        if (this.points.length < 2 || oldThen === 0) {
            velEl.textContent = 'Stable';
        } else {
            velEl.textContent = `${Math.max(0, Math.round(velocity)).toLocaleString()}`;
        }

        // 5. Mean Code Age (Weighted average)
        const totalLines = last.total;
        if (totalLines > 0) {
            let totalAge = 0;
            this.years.forEach(y => {
                const lines = last[y] || 0;
                const age = currentYear - parseInt(y);
                totalAge += lines * age;
            });
            const meanAge = totalAge / totalLines;
            document.getElementById('mean-code-age').textContent = `${meanAge.toFixed(1)} yrs`;
        }

        // 6. Peak Preservation (Largest legacy year)
        let peakYear = '--';
        let peakVal = 0;
        this.years.forEach(y => {
            if (parseInt(y) < currentYear) {
                const val = last[y] || 0;
                if (val > peakVal) {
                    peakVal = val;
                    peakYear = y;
                }
            }
        });
        document.getElementById('peak-year').textContent = peakYear;

        // 7. Greatest Transformation (Largest single drop in origin)
        let maxDrop = 0;
        let dropDate = '--';
        if (birthYear && this.points.length > 1) {
            for (let i = 1; i < this.points.length; i++) {
                const prev = this.points[i - 1][birthYear] || 0;
                const curr = this.points[i][birthYear] || 0;
                const drop = prev - curr;
                if (drop > maxDrop) {
                    maxDrop = drop;
                    const d = new Date(this.points[i].date);
                    dropDate = d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
                }
            }
        }
        document.getElementById('transformation-date').textContent = dropDate;
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
