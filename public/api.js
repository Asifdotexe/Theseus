/**
 * api.js - Configuration and dataset fetching for Ship of Theseus.
 */

/**
 * Fetch the application manifest and global milestones.
 *
 * @returns {Promise<{manifest: Array<object>, globalMilestones: Array<object>}>}
 */
export async function fetchConfig() {
  const response = await fetch("theseus.config.json");
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const data = await response.json();
  const manifest = data.repositories || (Array.isArray(data) ? data : [data]);
  const globalMilestones = data.globalMilestones || [];
  return { manifest, globalMilestones };
}

/**
 * Fetch snapshot graph and fossil data for a specific repository.
 *
 * @param {string} repoName
 * @param {AbortSignal} [signal]
 * @returns {Promise<{snapshots: Array<object>, fossils: object}>}
 */
export async function fetchRepoData(repoName, signal) {
  const response = await fetch(`data/processed/${repoName}_graph.json`, { signal });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const rawData = await response.json();

  if (Array.isArray(rawData)) {
    return {
      snapshots: rawData,
      fossils: {},
    };
  }

  return {
    snapshots: rawData.snapshots || [],
    fossils: rawData.fossils || {},
  };
}

/**
 * Clean, sort, and structure snapshot records into D3 stack-ready points.
 *
 * @param {Array<object>} rawSnapshots
 * @returns {{currentData: Array<object>, years: Array<string>, points: Array<object>}}
 */
export function processSnapshots(rawSnapshots) {
  if (!rawSnapshots || !Array.isArray(rawSnapshots)) {
    return { currentData: [], years: [], points: [] };
  }

  // Validate snapshot dates, filter out invalid entries
  const currentData = rawSnapshots.filter((d) => {
    const ts = new Date(d.snapshot_date).getTime();
    return !isNaN(ts);
  });

  // Sort snapshots chronologically
  currentData.sort(
    (a, b) =>
      new Date(a.snapshot_date).getTime() - new Date(b.snapshot_date).getTime()
  );

  const yearSet = new Set();
  currentData.forEach((d) => {
    if (d.composition && typeof d.composition === "object") {
      Object.keys(d.composition).forEach((y) => yearSet.add(y));
    }
  });
  const years = Array.from(yearSet).sort();

  // Convert to D3 stack-ready format
  const points = currentData.map((d) => {
    const comp = d.composition || {};
    const totalLines = Object.values(comp).reduce(
      (acc, val) => acc + (typeof val === "number" ? val : 0),
      0
    );
    const point = {
      date: new Date(d.snapshot_date),
      total: totalLines,
    };
    years.forEach((year) => {
      point[year] = comp[year] || 0;
    });
    return point;
  });

  return { currentData, years, points };
}
