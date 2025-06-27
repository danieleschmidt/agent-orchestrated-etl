from agent_orchestrated_etl import orchestrator


def test_pipeline_execution():
    """DataOrchestrator runs extract, transform, and load steps."""
    orch = orchestrator.DataOrchestrator()
    pipeline = orch.create_pipeline("s3")
    results = pipeline.execute()
    assert results["extract"] == [1, 2, 3]  # nosec B101
    assert results["transform"] == [1, 4, 9]  # nosec B101
    assert results["load"] is True  # nosec B101


def test_pipeline_custom_operation():
    """create_pipeline allows overriding default operations."""
    orch = orchestrator.DataOrchestrator()

    def always_false(_):
        return False

    pipeline = orch.create_pipeline("s3", operations={"load": always_false})
    results = pipeline.execute()
    assert results["load"] is False  # nosec B101
