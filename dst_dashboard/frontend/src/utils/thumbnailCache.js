// Module-scoped (persists for the SPA session) cache of fetched-but-not-yet-
// themed panel options. HomePage fully remounts on every route change
// (PageTransition keys the page tree by pathname), which would otherwise
// re-fetch and rebuild every thumbnail's chart data each time the user
// navigates back to Home. Theming is left out of the cache since it's a
// cheap, theme-reactive derivation (see ExperimentThumbnail's useMemo).
//
// Cached per-panel (keyed on `${experimentId}:${panelName}`), not per-
// experiment: an experiment's panels are fetched independently and can
// settle at very different times (or not at all, if one hangs/errors), so
// bundling them into one all-or-nothing cache entry means one slow or
// broken panel permanently prevents caching the others. Per-panel keys let
// each panel be reused the instant it succeeds, regardless of its siblings.
const panelOptionCache = new Map();

// The panel list itself (name/title metadata) is cached separately so a
// fully-cached experiment can skip the network entirely on remount, instead
// of always paying for at least one request just to know which panels exist.
const panelListCache = new Map();

export function getCachedPanelOption(experimentId, panelName) {
    return panelOptionCache.get(`${experimentId}:${panelName}`);
}

export function setCachedPanelOption(experimentId, panelName, rawOption) {
    panelOptionCache.set(`${experimentId}:${panelName}`, rawOption);
}

export function getCachedPanelList(experimentId) {
    return panelListCache.get(experimentId);
}

export function setCachedPanelList(experimentId, panels) {
    panelListCache.set(experimentId, panels);
}
