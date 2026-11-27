from unittest.mock import MagicMock

import pytest

from dst_dashboard.config.data_structures import (
    DashboardFullConfig,
    DeriveField,
    ExperimentConfig,
    PanelConfig,
    PanelStyle,
    PanelTransform,
)
from dst_dashboard.processors.panel_processor import PanelProcessor, deep_merge

# --------------------------------------------------------------------------- #
# Helper Functions
# --------------------------------------------------------------------------- #


def _make_processor(db=None) -> PanelProcessor:
    """Create a PanelProcessor with a minimal real config and a fake db."""
    config = DashboardFullConfig(datasources=[])
    return PanelProcessor(config, db if db is not None else MagicMock())


def _create_panel_config(
    name: str = "test-panel",
    title: str = "Test Panel",
    panel_type: str = "boxplot",
    dataset: str = "test-dataset",
    transform: PanelTransform = None,
    style: PanelStyle = None,
    echarts_options: dict = None,
    publish: bool = True,
) -> PanelConfig:
    """Create a real PanelConfig object for testing."""
    return PanelConfig(
        name=name,
        title=title,
        type=panel_type,
        dataset=dataset,
        transform=transform if transform is not None else PanelTransform(),
        style=style,
        echarts_options=echarts_options,
        publish=publish,
    )


def _create_experiment_config(
    experiment_id: str = "exp-1",
    panels: list = None,
) -> ExperimentConfig:
    """Create a real ExperimentConfig object for testing."""
    return ExperimentConfig(
        id=experiment_id,
        title="Test Experiment",
        family="test/family",
        metadata={},
        datasets=[],
        panels=panels or [],
        publish=True,
    )


# --------------------------------------------------------------------------- #
# deep_merge Tests
# --------------------------------------------------------------------------- #
class TestDeepMerge:
    """Tests for the deep_merge function."""

    def test_merge_disjoint_keys_combines_both(self):
        """Should include keys from both dictionaries when they don't overlap."""
        base = {"a": 1}
        override = {"b": 2}

        assert deep_merge(base, override) == {"a": 1, "b": 2}

    def test_merge_overlapping_scalar_keys_prefers_override(self):
        """Should let override values win for non-dict keys present in both."""
        base = {"a": 1, "b": 2}
        override = {"b": 99}

        assert deep_merge(base, override) == {"a": 1, "b": 99}

    def test_merge_nested_dicts_recursively(self):
        """Should recursively merge nested dictionaries instead of replacing them wholesale."""
        base = {"grid": {"left": "10%", "right": "10%"}}
        override = {"grid": {"right": "5%"}}

        assert deep_merge(base, override) == {"grid": {"left": "10%", "right": "5%"}}

    def test_merge_does_not_mutate_inputs(self):
        """Should leave the original base and override dictionaries untouched."""
        base = {"a": {"x": 1}}
        override = {"a": {"y": 2}}

        deep_merge(base, override)

        assert base == {"a": {"x": 1}}
        assert override == {"a": {"y": 2}}


# --------------------------------------------------------------------------- #
# PanelProcessor.process_panel Tests
# --------------------------------------------------------------------------- #
class TestProcessPanel:
    """Tests for PanelProcessor.process_panel."""

    def test_missing_dataset_returns_false_without_processing(self):
        """Should skip processing and return False when the panel's dataset doesn't exist."""
        db = MagicMock()
        db.dataset_exists.return_value = False
        processor = _make_processor(db)
        panel_config = _create_panel_config(dataset="missing-dataset")

        result = processor.process_panel("exp-1", panel_config)

        assert result is False
        db.store_panel_data.assert_not_called()

    def test_successful_transform_stores_option_and_returns_true(self, mocker):
        """Should store the transformed ECharts option and return True on success."""
        db = MagicMock()
        db.dataset_exists.return_value = True
        processor = _make_processor(db)
        panel_config = _create_panel_config(name="my-panel")
        echarts_option = {"title": {"text": "My Panel"}}
        mocker.patch.object(processor, "transform_panel_data", return_value=echarts_option)

        result = processor.process_panel("exp-1", panel_config)

        assert result is True
        db.store_panel_data.assert_called_once_with("exp-1", "my-panel", echarts_option)

    def test_transform_exception_is_caught_and_returns_false(self, mocker):
        """Should catch exceptions from the transform step and return False instead of raising."""
        db = MagicMock()
        db.dataset_exists.return_value = True
        processor = _make_processor(db)
        panel_config = _create_panel_config()
        mocker.patch.object(processor, "transform_panel_data", side_effect=ValueError("boom"))

        result = processor.process_panel("exp-1", panel_config)

        assert result is False
        db.store_panel_data.assert_not_called()


# --------------------------------------------------------------------------- #
# PanelProcessor.process_experiment_panels Tests
# --------------------------------------------------------------------------- #
class TestProcessExperimentPanels:
    """Tests for PanelProcessor.process_experiment_panels."""

    def test_no_panels_returns_zero(self):
        """Should return 0 immediately when the experiment has no panels."""
        processor = _make_processor()
        experiment = _create_experiment_config(panels=[])

        assert processor.process_experiment_panels(experiment) == 0

    def test_single_panel_processed_sequentially(self, mocker):
        """Should process a lone panel directly without spinning up a thread pool."""
        processor = _make_processor()
        panel = _create_panel_config(name="only-panel")
        experiment = _create_experiment_config(panels=[panel])
        mock_process_panel = mocker.patch.object(processor, "process_panel", return_value=True)

        result = processor.process_experiment_panels(experiment)

        assert result == 1
        mock_process_panel.assert_called_once_with(experiment.id, panel)

    def test_max_workers_one_processes_sequentially(self, mocker):
        """Should fall back to sequential processing when max_workers is 1."""
        processor = _make_processor()
        panels = [_create_panel_config(name="p1"), _create_panel_config(name="p2")]
        experiment = _create_experiment_config(panels=panels)
        mocker.patch.object(processor, "process_panel", return_value=True)

        result = processor.process_experiment_panels(experiment, max_workers=1)

        assert result == 2

    def test_multiple_panels_all_succeed_counts_all(self, mocker):
        """Should process all panels concurrently and count every success."""
        processor = _make_processor()
        panels = [_create_panel_config(name=f"p{i}") for i in range(4)]
        experiment = _create_experiment_config(panels=panels)
        mocker.patch.object(processor, "process_panel", return_value=True)

        result = processor.process_experiment_panels(experiment, max_workers=4)

        assert result == 4

    def test_multiple_panels_mixed_results_counts_only_successes(self, mocker):
        """Should only count panels that return True, not failed ones."""
        processor = _make_processor()
        panels = [_create_panel_config(name=f"p{i}") for i in range(3)]
        experiment = _create_experiment_config(panels=panels)
        results_by_name = {"p0": True, "p1": False, "p2": True}
        mocker.patch.object(
            processor,
            "process_panel",
            side_effect=lambda experiment_id, panel_config: results_by_name[panel_config.name],
        )

        result = processor.process_experiment_panels(experiment, max_workers=4)

        assert result == 2

    def test_panel_raising_unexpectedly_does_not_stop_others(self, mocker):
        """Should skip a panel whose processing raises without losing the other results."""
        processor = _make_processor()
        panels = [_create_panel_config(name="ok-panel"), _create_panel_config(name="bad-panel")]
        experiment = _create_experiment_config(panels=panels)

        def fake_process_panel(experiment_id, panel_config):
            if panel_config.name == "bad-panel":
                raise RuntimeError("unexpected failure")
            return True

        mocker.patch.object(processor, "process_panel", side_effect=fake_process_panel)

        result = processor.process_experiment_panels(experiment, max_workers=4)

        assert result == 1


# --------------------------------------------------------------------------- #
# PanelProcessor._apply_derive_transformations Tests
# --------------------------------------------------------------------------- #
class TestApplyDeriveTransformations:
    """Tests for PanelProcessor._apply_derive_transformations."""

    def test_no_derive_rules_returns_data_unchanged(self):
        """Should return the data unchanged when the panel's transform has no derive rules."""
        processor = _make_processor()
        panel_config = _create_panel_config(transform=PanelTransform())
        data = [{"pod_name": "node-1"}]

        result = processor._apply_derive_transformations(data, panel_config)

        assert result == data

    def test_regex_match_assigns_match_value(self):
        """Should assign the 'match' value when the regex pattern matches the field."""
        processor = _make_processor()
        derive = DeriveField(
            name="pod_group",
            function="regex_match",
            field="pod_name",
            pattern=".*slow.*",
            match="slow",
            no_match="normal",
        )
        panel_config = _create_panel_config(transform=PanelTransform(derive=[derive]))
        data = [{"pod_name": "nimp2p-slow-1"}]

        result = processor._apply_derive_transformations(data, panel_config)

        assert result == [{"pod_name": "nimp2p-slow-1", "pod_group": "slow"}]

    def test_regex_no_match_assigns_no_match_value(self):
        """Should assign the 'no_match' value when the regex pattern doesn't match the field."""
        processor = _make_processor()
        derive = DeriveField(
            name="pod_group",
            function="regex_match",
            field="pod_name",
            pattern=".*slow.*",
            match="slow",
            no_match="normal",
        )
        panel_config = _create_panel_config(transform=PanelTransform(derive=[derive]))
        data = [{"pod_name": "nimp2p-fast-1"}]

        result = processor._apply_derive_transformations(data, panel_config)

        assert result == [{"pod_name": "nimp2p-fast-1", "pod_group": "normal"}]

    def test_unknown_derive_function_leaves_row_without_new_field(self):
        """Should leave the row without the derived field when the derive function is unknown."""
        processor = _make_processor()
        derive = DeriveField(name="foo", function="unsupported_function", field="pod_name")
        panel_config = _create_panel_config(transform=PanelTransform(derive=[derive]))
        data = [{"pod_name": "node-1"}]

        result = processor._apply_derive_transformations(data, panel_config)

        assert result == [{"pod_name": "node-1"}]

    def test_original_data_rows_are_not_mutated(self):
        """Should return new row dicts instead of mutating the original input rows."""
        processor = _make_processor()
        derive = DeriveField(
            name="pod_group",
            function="regex_match",
            field="pod_name",
            pattern=".*slow.*",
            match="slow",
            no_match="normal",
        )
        panel_config = _create_panel_config(transform=PanelTransform(derive=[derive]))
        original_row = {"pod_name": "nimp2p-slow-1"}
        data = [original_row]

        processor._apply_derive_transformations(data, panel_config)

        assert original_row == {"pod_name": "nimp2p-slow-1"}


# --------------------------------------------------------------------------- #
# PanelProcessor._transform_to_boxplot Tests
# --------------------------------------------------------------------------- #
class TestTransformToBoxplot:
    """Tests for PanelProcessor._transform_to_boxplot."""

    def test_missing_groupby_raises_value_error(self):
        """Should raise ValueError when 'groupBy' is missing from the transform."""
        processor = _make_processor()
        panel_config = _create_panel_config(transform=PanelTransform(value="delayMs"))

        with pytest.raises(ValueError, match="requires 'groupBy' and 'value'"):
            processor._transform_to_boxplot([], panel_config)

    def test_missing_value_raises_value_error(self):
        """Should raise ValueError when 'value' is missing from the transform."""
        processor = _make_processor()
        panel_config = _create_panel_config(transform=PanelTransform(groupBy="pod_name"))

        with pytest.raises(ValueError, match="requires 'groupBy' and 'value'"):
            processor._transform_to_boxplot([], panel_config)

    def test_computes_boxplot_stats_per_group_sorted_by_category(self):
        """Should compute [min, Q1, median, Q3, max] per group, sorted by category name."""
        processor = _make_processor()
        panel_config = _create_panel_config(
            transform=PanelTransform(groupBy="pod_name", value="delayMs")
        )
        data = [
            {"pod_name": "b", "delayMs": 10},
            {"pod_name": "a", "delayMs": 1},
            {"pod_name": "a", "delayMs": 2},
            {"pod_name": "a", "delayMs": 3},
        ]

        option = processor._transform_to_boxplot(data, panel_config)

        assert option["xAxis"]["data"] == ["a", "b"]
        series_data = option["series"][0]["data"]
        assert series_data[0] == [1.0, 1.0, 2.0, 3.0, 3.0]
        assert series_data[1] == [10.0, 10.0, 10.0, 10.0, 10.0]

    def test_top_filter_keeps_highest_average_groups(self):
        """Should keep only the top-N groups by average value when 'top' is set."""
        processor = _make_processor()
        panel_config = _create_panel_config(
            transform=PanelTransform(groupBy="pod_name", value="delayMs", top=1)
        )
        data = [
            {"pod_name": "low", "delayMs": 1},
            {"pod_name": "high", "delayMs": 100},
        ]

        option = processor._transform_to_boxplot(data, panel_config)

        assert option["xAxis"]["data"] == ["high"]

    def test_style_options_are_applied_to_axes(self):
        """Should apply xLabel/yLabel/yMin/yMax from panel style onto the axes."""
        processor = _make_processor()
        style = PanelStyle(xLabel="Pod", yLabel="Delay (ms)", yMin=0, yMax=100)
        panel_config = _create_panel_config(
            transform=PanelTransform(groupBy="pod_name", value="delayMs"), style=style
        )
        data = [{"pod_name": "a", "delayMs": 1}]

        option = processor._transform_to_boxplot(data, panel_config)

        assert option["xAxis"]["name"] == "Pod"
        assert option["yAxis"]["name"] == "Delay (ms)"
        assert option["yAxis"]["min"] == 0
        assert option["yAxis"]["max"] == 100

    def test_rows_missing_the_value_field_are_ignored(self):
        """Should skip rows where the value field is missing/None instead of raising."""
        processor = _make_processor()
        panel_config = _create_panel_config(
            transform=PanelTransform(groupBy="pod_name", value="delayMs")
        )
        data = [{"pod_name": "a", "delayMs": 5}, {"pod_name": "a"}]

        option = processor._transform_to_boxplot(data, panel_config)

        assert option["series"][0]["data"] == [[5.0, 5.0, 5.0, 5.0, 5.0]]


# --------------------------------------------------------------------------- #
# PanelProcessor._transform_to_timeseries Tests
# --------------------------------------------------------------------------- #
class TestTransformToTimeseries:
    """Tests for PanelProcessor._transform_to_timeseries."""

    def test_missing_x_raises_value_error(self):
        """Should raise ValueError when 'x' is missing from the transform."""
        processor = _make_processor()
        panel_config = _create_panel_config(
            panel_type="timeseries", transform=PanelTransform(y="value")
        )

        with pytest.raises(ValueError, match="requires 'x' and 'y'"):
            processor._transform_to_timeseries([], panel_config)

    def test_missing_y_raises_value_error(self):
        """Should raise ValueError when 'y' is missing from the transform."""
        processor = _make_processor()
        panel_config = _create_panel_config(
            panel_type="timeseries", transform=PanelTransform(x="timestamp")
        )

        with pytest.raises(ValueError, match="requires 'x' and 'y'"):
            processor._transform_to_timeseries([], panel_config)

    def test_grouped_series_are_sorted_by_name_with_cycling_colors(self):
        """Should build one series per group, sorted alphabetically, cycling through the palette."""
        processor = _make_processor()
        panel_config = _create_panel_config(
            panel_type="timeseries",
            transform=PanelTransform(x="ts", y="v", groupBy="pod_name"),
        )
        data = [
            {"pod_name": "b", "ts": "t2", "v": 5},
            {"pod_name": "a", "ts": "t1", "v": 1},
            {"pod_name": "a", "ts": "t2", "v": 3},
            {"pod_name": "c", "ts": "t1", "v": 100},
        ]

        option = processor._transform_to_timeseries(data, panel_config)

        series = option["series"]
        assert [s["name"] for s in series] == ["a", "b", "c"]
        assert series[0]["data"] == [["t1", 1.0], ["t2", 3.0]]
        assert series[1]["data"] == [["t2", 5.0]]
        assert series[2]["data"] == [["t1", 100.0]]
        assert series[0]["lineStyle"]["color"] == "#5470c6"
        assert series[1]["lineStyle"]["color"] == "#91cc75"
        assert series[2]["lineStyle"]["color"] == "#fac858"

    def test_top_filter_keeps_highest_average_series(self):
        """Should keep only the top-N series by average value when 'top' is set."""
        processor = _make_processor()
        panel_config = _create_panel_config(
            panel_type="timeseries",
            transform=PanelTransform(x="ts", y="v", groupBy="pod_name", top=2),
        )
        data = [
            {"pod_name": "b", "ts": "t2", "v": 5},
            {"pod_name": "a", "ts": "t1", "v": 1},
            {"pod_name": "a", "ts": "t2", "v": 3},
            {"pod_name": "c", "ts": "t1", "v": 100},
        ]

        option = processor._transform_to_timeseries(data, panel_config)

        assert [s["name"] for s in option["series"]] == ["b", "c"]

    def test_firstn_filter_keeps_first_n_by_name(self):
        """Should keep only the first-N series by name (no value sorting) when 'firstN' is set."""
        processor = _make_processor()
        panel_config = _create_panel_config(
            panel_type="timeseries",
            transform=PanelTransform(x="ts", y="v", groupBy="pod_name", firstN=2),
        )
        data = [
            {"pod_name": "b", "ts": "t2", "v": 5},
            {"pod_name": "a", "ts": "t1", "v": 1},
            {"pod_name": "c", "ts": "t1", "v": 100},
        ]

        option = processor._transform_to_timeseries(data, panel_config)

        assert [s["name"] for s in option["series"]] == ["a", "b"]

    def test_single_series_without_groupby_has_x_pairing(self):
        """
        Should pair each value with its x (timestamp) as [x, y], matching the
        grouped path - required since xAxis is time-typed.
        """
        processor = _make_processor()
        panel_config = _create_panel_config(
            panel_type="timeseries", title="Solo", transform=PanelTransform(x="ts", y="v")
        )
        data = [{"ts": "t1", "v": 1}, {"ts": "t2", "v": 2}]

        option = processor._transform_to_timeseries(data, panel_config)

        assert option["series"] == [
            {
                "name": "Solo",
                "type": "line",
                "data": [["t1", 1.0], ["t2", 2.0]],
                "smooth": True,
                "sampling": "lttb",
                "symbol": "none",
                "lineStyle": {"width": 2, "color": "#00d4ff"},
                "areaStyle": {"color": "#00d4ff", "opacity": 0.15},
            }
        ]

    def test_datetime_x_values_are_serialized_to_isoformat(self):
        """Should convert datetime-like x values to ISO strings via .isoformat()."""
        from datetime import datetime

        processor = _make_processor()
        panel_config = _create_panel_config(
            panel_type="timeseries", transform=PanelTransform(x="ts", y="v", groupBy="pod")
        )
        data = [{"pod": "a", "ts": datetime(2026, 7, 5, 17, 47, 11), "v": 1}]

        option = processor._transform_to_timeseries(data, panel_config)

        assert option["series"][0]["data"] == [["2026-07-05T17:47:11", 1.0]]

    def test_yunit_style_sets_formatter_placeholders(self):
        """Should set the matching formatter placeholder on yAxis and tooltip for known units."""
        processor = _make_processor()
        style = PanelStyle(yUnit="ms")
        panel_config = _create_panel_config(
            panel_type="timeseries",
            transform=PanelTransform(x="ts", y="v", groupBy="pod"),
            style=style,
        )
        data = [{"pod": "a", "ts": "t1", "v": 1}]

        option = processor._transform_to_timeseries(data, panel_config)

        assert option["yAxis"]["axisLabel"]["formatter"] == "__MS_FORMATTER__"
        assert option["tooltip"]["valueFormatter"] == "__MS_FORMATTER__"

    def test_style_labels_are_applied_to_axes(self):
        """Should apply xLabel/yLabel/yMin/yMax from panel style onto the axes."""
        processor = _make_processor()
        style = PanelStyle(xLabel="Time", yLabel="Value", yMin=0, yMax=10)
        panel_config = _create_panel_config(
            panel_type="timeseries",
            transform=PanelTransform(x="ts", y="v", groupBy="pod"),
            style=style,
        )
        data = [{"pod": "a", "ts": "t1", "v": 1}]

        option = processor._transform_to_timeseries(data, panel_config)

        assert option["xAxis"]["name"] == "Time"
        assert option["yAxis"]["name"] == "Value"
        assert option["yAxis"]["min"] == 0
        assert option["yAxis"]["max"] == 10


# --------------------------------------------------------------------------- #
# PanelProcessor.transform_panel_data Tests
# --------------------------------------------------------------------------- #
class TestTransformPanelData:
    """Tests for PanelProcessor.transform_panel_data."""

    def test_missing_dataset_raises_value_error(self):
        """Should raise ValueError when the panel's dataset has no stored data."""
        db = MagicMock()
        db.get_dataset.return_value = None
        processor = _make_processor(db)
        panel_config = _create_panel_config(dataset="missing-dataset")

        with pytest.raises(ValueError, match="Dataset 'missing-dataset' not found"):
            processor.transform_panel_data("exp-1", panel_config)

    def test_empty_dataset_raises_value_error(self):
        """Should raise ValueError when the panel's dataset exists but has zero rows."""
        db = MagicMock()
        db.get_dataset.return_value = []
        processor = _make_processor(db)
        panel_config = _create_panel_config(dataset="empty-dataset")

        with pytest.raises(ValueError, match="Dataset 'empty-dataset' not found"):
            processor.transform_panel_data("exp-1", panel_config)

    def test_unsupported_format_raises_value_error(self):
        """Should raise ValueError for a viz_format that isn't recognized at all."""
        db = MagicMock()
        db.get_dataset.return_value = [{"a": 1}]
        processor = _make_processor(db)
        panel_config = _create_panel_config()

        with pytest.raises(ValueError, match="Unsupported visualization format"):
            processor.transform_panel_data("exp-1", panel_config, viz_format="svg")


# --------------------------------------------------------------------------- #
# PanelProcessor._transform_to_echarts Tests
# --------------------------------------------------------------------------- #
class TestTransformToEcharts:
    """Tests for PanelProcessor._transform_to_echarts."""

    def test_table_type_returns_data_as_is(self):
        """Should return the data unchanged (wrapped with type/title) for table panels."""
        processor = _make_processor()
        panel_config = _create_panel_config(panel_type="table", title="Raw Rows")
        data = [{"a": 1}, {"a": 2}]

        result = processor._transform_to_echarts(data, panel_config)

        assert result == {"type": "table", "title": "Raw Rows", "data": data}

    def test_unknown_type_raises_value_error(self):
        """Should raise ValueError for a panel type outside the known set."""
        processor = _make_processor()
        panel_config = _create_panel_config(panel_type="boxplot")
        panel_config.type = "unknown-type"  # bypass pydantic's Literal at construction time

        with pytest.raises(ValueError, match="Unknown panel type"):
            processor._transform_to_echarts([], panel_config)
