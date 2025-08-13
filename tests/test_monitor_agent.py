import pytest

from agent_orchestrated_etl import orchestrator


def test_monitor_logs_pipeline_execution():
    orch = orchestrator.DataOrchestrator()
    pipeline = orch.create_pipeline("s3")
    monitor = orchestrator.MonitorAgent()
    results = pipeline.execute(monitor=monitor)
    assert results["load"] is True  # nosec B101
    assert any("starting extract" in e for e in monitor.events)  # nosec B101
    assert any("completed load" in e for e in monitor.events)  # nosec B101


def test_monitor_logs_errors():
    orch = orchestrator.DataOrchestrator()
    # Disable graceful degradation to ensure exception propagation
    pipeline = orch.create_pipeline("s3", operations={"load": lambda _d: 1 / 0},
                                   enable_graceful_degradation=False)
    monitor = orchestrator.MonitorAgent()
    with pytest.raises(Exception):  # Pipeline wraps exceptions
        pipeline.execute(monitor=monitor)
    assert any(e.startswith("ERROR:") for e in monitor.events)  # nosec B101


def test_monitor_writes_file(tmp_path):
    """Events are appended to the provided log file."""
    log = tmp_path / "events.log"
    monitor = orchestrator.MonitorAgent(log)
    monitor.log("start")
    monitor.error("boom")
    lines = log.read_text().splitlines()
    # Check that entries contain expected messages with timestamps
    assert len(lines) == 2  # nosec B101
    assert lines[0].endswith(": start")  # nosec B101
    assert lines[1].endswith(": ERROR: boom")  # nosec B101
