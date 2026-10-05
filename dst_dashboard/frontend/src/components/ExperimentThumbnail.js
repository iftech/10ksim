import React, { Suspense, lazy, useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import { API_BASE_URL } from '../config';
import { useInView } from '../hooks/useInView';
import { buildThumbnailOption } from '../utils/chartOptions';
import {
    getCachedPanelList,
    getCachedPanelOption,
    setCachedPanelList,
    setCachedPanelOption,
} from '../utils/thumbnailCache';
import { useTheme } from '../context/ThemeContext';

const ReactECharts = lazy(() => import('echarts-for-react'));

const THUMB_HEIGHT = 152;
// Candidates considered per card, tried one at a time. The common case is
// one request (the first panel succeeds); a candidate is only fetched if
// every earlier one failed, so a broken panel doesn't blank out the card.
// Capped so an experiment with many broken panels can't chain into an
// unbounded number of retries.
const MAX_PANEL_CANDIDATES = 4;

function ThumbnailSkeleton() {
    return (
        <div className="animate-pulse w-full" style={{ height: THUMB_HEIGHT }}>
            <div className="h-full w-full bg-base-300/60" />
        </div>
    );
}

// A quick "glance" preview of an experiment's results, rendered only once the
// card scrolls near the viewport, keeping the home page (6 featured
// experiments) to one panel request per card in the normal case instead of
// one per panel. Only if every attempted candidate fails does the thumbnail
// fall back to the "failed" placeholder.
function ExperimentThumbnail({ experimentId }) {
    const { isDark } = useTheme();
    const [ref, inView] = useInView();
    const [rawOption, setRawOption] = useState(() => {
        const panels = getCachedPanelList(experimentId);
        if (!panels) return null;
        for (const panel of panels) {
            const cachedOption = getCachedPanelOption(experimentId, panel.name);
            if (cachedOption) return cachedOption;
        }
        return null;
    });
    const [failed, setFailed] = useState(false);

    useEffect(() => {
        if (!inView || rawOption) return;
        const controller = new AbortController();

        const fetchPanelOption = (panelName) =>
            axios
                .get(`${API_BASE_URL}/experiments/${experimentId}/panels/${panelName}`, {
                    signal: controller.signal,
                })
                .then((res) => res.data?.option)
                .catch(() => null);

        const getPanelList = () => {
            const cachedPanels = getCachedPanelList(experimentId);
            if (cachedPanels) return Promise.resolve(cachedPanels);
            return axios
                .get(`${API_BASE_URL}/experiments/${experimentId}`, { signal: controller.signal })
                .then((res) => {
                    const panels = (res.data?.panels || []).slice(0, MAX_PANEL_CANDIDATES);
                    setCachedPanelList(experimentId, panels);
                    return panels;
                });
        };

        const tryFromIndex = (panels, index) => {
            if (controller.signal.aborted) return;
            if (index >= panels.length) {
                setFailed(true);
                return;
            }
            const panel = panels[index];
            const cachedOption = getCachedPanelOption(experimentId, panel.name);
            if (cachedOption) {
                setRawOption(cachedOption);
                return;
            }
            fetchPanelOption(panel.name).then((option) => {
                if (controller.signal.aborted) return;
                if (option) {
                    setCachedPanelOption(experimentId, panel.name, option);
                    setRawOption(option);
                } else {
                    tryFromIndex(panels, index + 1);
                }
            });
        };

        getPanelList()
            .then((panels) => {
                if (controller.signal.aborted) return;
                if (!panels.length) {
                    setFailed(true);
                    return;
                }
                tryFromIndex(panels, 0);
            })
            .catch(() => {
                if (!controller.signal.aborted) setFailed(true);
            });

        return () => {
            controller.abort();
        };
    }, [inView, experimentId, rawOption]);

    // Re-theming on a dark/light toggle is just recoloring already-fetched
    // data, so it's a cheap useMemo rather than something the fetch effect
    // above needs to re-run for.
    const option = useMemo(
        () => (rawOption ? buildThumbnailOption(rawOption, isDark) : null),
        [rawOption, isDark]
    );

    return (
        <div ref={ref} className="bg-base-100 border-b border-base-100 shrink-0 overflow-hidden" style={{ height: THUMB_HEIGHT }}>
            {option ? (
                <Suspense fallback={<ThumbnailSkeleton />}>
                    <ReactECharts
                        option={option}
                        style={{ height: THUMB_HEIGHT, width: '100%' }}
                        opts={{ renderer: 'canvas' }}
                        notMerge={true}
                        lazyUpdate={true}
                    />
                </Suspense>
            ) : failed ? (
                <div className="h-full w-full flex items-center justify-center text-base-content-tertiary">
                    <i className="bi bi-bar-chart-line text-2xl"></i>
                </div>
            ) : (
                <ThumbnailSkeleton />
            )}
        </div>
    );
}

export default ExperimentThumbnail;
