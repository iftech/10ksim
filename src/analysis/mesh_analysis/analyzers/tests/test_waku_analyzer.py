import json

import pandas as pd

from src.analysis.mesh_analysis.analyzers.waku.waku_analyzer import WakuAnalyzer


def _write_archive(folder, name, encoded_hashes):
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{name}.json").write_text(json.dumps(encoded_hashes))


def _write_received(path, log_hashes):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"msg_hash": log_hashes}).to_csv(path, index=False)


def test_store_archive_check_reports_each_node(tmp_path, caplog):
    first = "0x" + "aa" * 32
    second = "0x" + "bb" * 32
    received = tmp_path / "summary" / "received.csv"
    # Duplicated because every receiving node logs the same message.
    _write_received(received, [first, second, first])

    archives = tmp_path / "store_messages"
    _write_archive(archives, "store-0-0", [first, second])
    _write_archive(archives, "store-0-1", [first])

    with caplog.at_level("INFO"):
        WakuAnalyzer().check_store_archives(archives, received_csv=received)

    assert "`store-0-0` holds all 2 messages" in caplog.text
    assert "`store-0-1` holds 1 of 2 messages" in caplog.text
    assert "Store nodes with a complete archive: 1 of 2" in caplog.text


def test_store_archive_check_without_dumps(tmp_path, caplog):
    received = tmp_path / "summary" / "received.csv"
    _write_received(received, ["0x" + "aa" * 32])

    with caplog.at_level("INFO"):
        WakuAnalyzer().check_store_archives(tmp_path / "store_messages", received_csv=received)

    assert "No store archive dumps found" in caplog.text


def test_store_archive_check_returns_result_status(tmp_path):
    first = "0x" + "aa" * 32
    received = tmp_path / "summary" / "received.csv"
    _write_received(received, [first])
    archives = tmp_path / "store_messages"
    _write_archive(archives, "store-0-0", [first])

    result = WakuAnalyzer().check_store_archives(archives, received_csv=received)

    assert result.status == "passed"
    assert result.intermediates["complete_nodes"] == 1
    assert result.intermediates["nodes"]["store-0-0"]["missing"] == 0


def test_store_archive_check_skips_on_empty_summary(tmp_path):
    received = tmp_path / "summary" / "received.csv"
    _write_received(received, [])
    archives = tmp_path / "store_messages"
    _write_archive(archives, "store-0-0", ["0x" + "aa" * 32])

    result = WakuAnalyzer().check_store_archives(archives, received_csv=received)

    assert result.status == "skipped"
    assert "holds no messages" in result.intermediates["failed"]
