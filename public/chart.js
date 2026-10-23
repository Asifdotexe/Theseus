/**
 * chart.js - D3 scales, stacked area paths, axes, and time scrubber.
 */

export class ChartRenderer {
  constructor(canvas, options = {}) {
    this.canvas = canvas;
    this.margin = options.margin || { top: 10, right: 20, bottom: 50, left: 60 };
    this.built = false;
    this.chartWidth = 0;
    this.chartHeight = 0;
    this.xScale = null;
    this.yScale = null;
    this.scrubber = null;
    this.focusIndex = undefined;
    this.points = [];
    this.years = [];
    this.vizMode = "chronological";
    this.yScaleMode = "linear";
    this.onHover = options.onHover || null;
    this.onLeave = options.onLeave || null;
  }

  createXScale(points, width) {
    const d3 = window.d3;
    return d3
      .scaleTime()
      .domain(d3.extent(points, (d) => d.date))
      .range([0, width]);
  }

  createYScale(points, height, yScaleMode) {
    const d3 = window.d3;
    const maxTotal = d3.max(points, (d) => d.total) || 1;
    if (yScaleMode === "log") {
      return d3
        .scaleLog()
        .domain([1, maxTotal * 1.1])
        .range([height, 0])
        .clamp(true);
    }
    return d3
      .scaleLinear()
      .domain([0, maxTotal * 1.05])
      .range([height, 0]);
  }

  createGradients(defs, years, vizMode) {
    defs.selectAll("linearGradient").remove();

    const getBaseColor = (seriesName, seriesIndex) => {
      if (vizMode === "identity") {
        return seriesIndex === 0 ? "oklch(68% 0.14 195)" : "oklch(72% 0.16 65)";
      }
      const yearIdx = years.indexOf(seriesName);
      return `oklch(70% 0.14 ${(195 + yearIdx * 36) % 360})`;
    };

    years.forEach((year, i) => {
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

    if (vizMode === "identity") {
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
  }

  renderStackedLayers(
    g,
    stackedData,
    xScale,
    yScale,
    yScaleMode,
    vizMode,
    animDuration,
    reducedMotion
  ) {
    const d3 = window.d3;
    const areaGenerator = d3
      .area()
      .x((d) => xScale(d.data.date))
      .y0((d) => yScale(yScaleMode === "log" ? Math.max(1, d[0]) : d[0]))
      .y1((d) => yScale(yScaleMode === "log" ? Math.max(1, d[1]) : d[1]))
      .curve(d3.curveMonotoneX);

    const layers = g.selectAll(".layer").data(stackedData, (d) => d.key);

    const getFill = (d, i) => {
      if (vizMode === "identity") {
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
      .duration(animDuration)
      .delay((_d, i) => (reducedMotion ? 0 : i * 50))
      .style("opacity", 1);

    layers
      .transition()
      .duration(animDuration)
      .attr("d", areaGenerator)
      .attr("fill", getFill);

    layers.exit().remove();
  }

  renderAxes(g, width, height, xScale, yScale, pointsLength) {
    const d3 = window.d3;
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
      .ticks(Math.min(pointsLength, 6))
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

  renderMilestones(g, chartWidth, chartHeight, xScale, milestones, reducedMotion) {
    if (!milestones || milestones.length === 0) return;
    const d3 = window.d3;

    milestones.forEach((m) => {
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
          .append("line")
          .attr("x1", 0)
          .attr("y1", 16)
          .attr("x2", 0)
          .attr("y2", chartHeight)
          .attr("stroke", "var(--nebula)")
          .attr("stroke-width", 1)
          .attr("stroke-dasharray", "4,4")
          .style("opacity", 0.5);

        marker
          .append("text")
          .attr("x", 0)
          .attr("y", 8)
          .attr("text-anchor", "middle")
          .attr("font-size", "14px")
          .attr("fill", "var(--nebula)")
          .text("★")
          .style("opacity", 0.8)
          .style("filter", "drop-shadow(0 0 4px oklch(52% 0.22 285 / 0.6))");

        marker.append("title").text(m.title + ": " + m.description);

        const animDur = reducedMotion ? 0 : 200;

        const enlarge = function () {
          d3.select(this)
            .select("text")
            .transition()
            .duration(animDur)
            .attr("font-size", "18px")
            .style("opacity", 1);
          d3.select(this)
            .select("line")
            .transition()
            .duration(animDur)
            .style("opacity", 1)
            .attr("stroke-width", 2);
        };

        const shrink = function () {
          d3.select(this)
            .select("text")
            .transition()
            .duration(animDur)
            .attr("font-size", "14px")
            .style("opacity", 0.8);
          d3.select(this)
            .select("line")
            .transition()
            .duration(animDur)
            .style("opacity", 0.5)
            .attr("stroke-width", 1);
        };

        marker
          .on("mouseenter", enlarge)
          .on("mouseleave", shrink)
          .on("focus", enlarge)
          .on("blur", shrink);
      }
    });
  }

  setupInteractivity(g, width, height, xScale) {
    const d3 = window.d3;
    const scrubber = g
      .append("line")
      .attr("class", "scrubber-line hidden")
      .attr("y1", 0)
      .attr("y2", height)
      .attr("stroke", "oklch(100% 0 0 / 0.2)")
      .attr("stroke-width", 1);
    this.scrubber = scrubber;

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
        "Chart of code composition by year. Use Arrow Left and Arrow Right to navigate data points."
      )
      .on("mousemove", function (event) {
        const mouseX = d3.pointer(event)[0];
        const date = xScale.invert(mouseX);
        const idx = bisect(self.points, date, 1);
        const d0 = self.points[idx - 1];
        const d1 = self.points[idx];

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

        if (self.onHover) {
          self.onHover(
            d,
            snappedX + self.margin.left,
            d3.pointer(event)[1] + self.margin.top
          );
        }
      })
      .on("mouseleave", () => {
        scrubber.classed("hidden", true);
        if (self.onLeave) self.onLeave();
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
        scrubber.classed("hidden", true);
        if (self.onLeave) self.onLeave();
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

  updateFocusPoint(xScale, scrubber) {
    const d = this.points[this.focusIndex];
    if (!d) return;

    const snappedX = xScale(d.date);
    scrubber.attr("x1", snappedX).attr("x2", snappedX).classed("hidden", false);

    if (this.onHover) {
      this.onHover(d, snappedX + this.margin.left, null);
    }
  }

  render({
    points,
    years,
    vizMode,
    yScaleMode,
    milestones = [],
    animDuration = 800,
    reducedMotion = false,
  }) {
    this.points = points;
    this.years = years;
    this.vizMode = vizMode;
    this.yScaleMode = yScaleMode;

    const width = this.canvas.clientWidth;
    const height = this.canvas.clientHeight;
    if (!width || !height) return;

    const chartWidth = width - this.margin.left - this.margin.right;
    const chartHeight = height - this.margin.top - this.margin.bottom;

    const d3 = window.d3;
    const svg = d3.select(this.canvas);
    const needsBuild = !this.built;
    const dimsChanged =
      this.chartWidth !== chartWidth || this.chartHeight !== chartHeight;

    this.focusIndex = undefined;

    if (needsBuild || dimsChanged) {
      svg.selectAll("*").remove();
      svg.append("defs");
      this.g = svg
        .append("g")
        .attr("class", "chart-container")
        .attr("transform", `translate(${this.margin.left},${this.margin.top})`);
      this.chartWidth = chartWidth;
      this.chartHeight = chartHeight;
      this.built = true;
    }

    const g = this.g;

    g.selectAll(
      "g.axis-y, g.axis-x, .milestone-marker, path.layer, .scrubber-line, rect[fill='transparent'], text.axis-label"
    ).remove();

    const xScale = this.createXScale(points, chartWidth);
    this.xScale = xScale;

    const yScale = this.createYScale(points, chartHeight, yScaleMode);
    this.yScale = yScale;

    this.createGradients(svg.select("defs"), years, vizMode);

    const stackGenerator = d3.stack().keys(years);
    const stackedData = stackGenerator(points);

    this.renderStackedLayers(
      g,
      stackedData,
      xScale,
      yScale,
      yScaleMode,
      vizMode,
      animDuration,
      reducedMotion
    );

    this.renderAxes(g, chartWidth, chartHeight, xScale, yScale, points.length);
    this.renderMilestones(g, chartWidth, chartHeight, xScale, milestones, reducedMotion);
    this.setupInteractivity(g, chartWidth, chartHeight, xScale);
  }
}
