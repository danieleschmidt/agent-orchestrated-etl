"""Real-time streaming data processor for Agent-Orchestrated-ETL."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any, Callable, Dict, List, Optional

from .logging_config import get_logger


@dataclass
class StreamMessage:
    """Represents a streaming data message."""
    data: Dict[str, Any]
    timestamp: float
    source: str
    message_id: str


class StreamProcessor:
    """Real-time streaming data processor with buffering and backpressure handling."""

    def __init__(self,
                 buffer_size: int = 1000,
                 batch_size: int = 100,
                 flush_interval: float = 5.0,
                 max_workers: int = 4):
        self.logger = get_logger("agent_etl.streaming")
        self.buffer_size = buffer_size
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_workers = max_workers

        self._buffer = Queue(maxsize=buffer_size)
        self._processors: Dict[str, Callable] = {}
        self._running = False
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._flush_thread: Optional[threading.Thread] = None

        # Metrics
        self.messages_processed = 0
        self.messages_dropped = 0
        self.processing_errors = 0

    def register_processor(self, source_type: str, processor: Callable[[List[StreamMessage]], None]) -> None:
        """Register a processor for a specific source type."""
        self._processors[source_type] = processor
        self.logger.info(f"Registered processor for source type: {source_type}")

    async def start(self) -> None:
        """Start the streaming processor."""
        if self._running:
            return

        self._running = True
        self.logger.info("Starting streaming processor")

        # Start flush thread for periodic batch processing
        self._flush_thread = threading.Thread(target=self._flush_worker, daemon=True)
        self._flush_thread.start()

    async def stop(self) -> None:
        """Stop the streaming processor gracefully."""
        if not self._running:
            return

        self.logger.info("Stopping streaming processor")
        self._running = False

        # Process remaining messages
        await self._process_remaining()

        if self._flush_thread:
            self._flush_thread.join(timeout=10.0)

        self._executor.shutdown(wait=True)

    async def ingest(self, data: Dict[str, Any], source: str) -> bool:
        """Ingest a message into the streaming processor."""
        try:
            message = StreamMessage(
                data=data,
                timestamp=time.time(),
                source=source,
                message_id=f"{source}_{int(time.time() * 1000000)}"
            )

            # Non-blocking put with backpressure handling
            self._buffer.put_nowait(message)
            return True

        except Exception as e:
            self.messages_dropped += 1
            self.logger.warning(f"Failed to ingest message from {source}: {str(e)}")
            return False

    def _flush_worker(self) -> None:
        """Background worker for periodic batch processing."""
        while self._running:
            try:
                batch = self._collect_batch()
                if batch:
                    self._process_batch(batch)

                time.sleep(min(self.flush_interval, 1.0))  # Check at least every second

            except Exception as e:
                self.processing_errors += 1
                self.logger.error(f"Error in flush worker: {str(e)}")

    def _collect_batch(self) -> List[StreamMessage]:
        """Collect a batch of messages from the buffer."""
        batch = []
        try:
            # Collect up to batch_size messages
            for _ in range(self.batch_size):
                try:
                    message = self._buffer.get_nowait()
                    batch.append(message)
                except Empty:
                    break

        except Exception as e:
            self.logger.error(f"Error collecting batch: {str(e)}")

        return batch

    def _process_batch(self, batch: List[StreamMessage]) -> None:
        """Process a batch of messages by source type."""
        if not batch:
            return

        # Group messages by source
        by_source: Dict[str, List[StreamMessage]] = {}
        for message in batch:
            if message.source not in by_source:
                by_source[message.source] = []
            by_source[message.source].append(message)

        # Process each source group
        futures = []
        for source, messages in by_source.items():
            if source in self._processors:
                future = self._executor.submit(self._safe_process, source, messages)
                futures.append(future)
            else:
                self.logger.warning(f"No processor registered for source: {source}")

        # Wait for all processing to complete
        for future in futures:
            try:
                future.result(timeout=30.0)  # 30 second timeout per batch
            except Exception as e:
                self.processing_errors += 1
                self.logger.error(f"Batch processing failed: {str(e)}")

        self.messages_processed += len(batch)

    def _safe_process(self, source: str, messages: List[StreamMessage]) -> None:
        """Safely process messages with error handling."""
        try:
            processor = self._processors[source]
            processor(messages)
        except Exception as e:
            self.processing_errors += 1
            self.logger.error(f"Processing failed for source {source}: {str(e)}")
            raise

    async def _process_remaining(self) -> None:
        """Process any remaining messages in the buffer."""
        try:
            remaining_messages = []
            while True:
                try:
                    message = self._buffer.get_nowait()
                    remaining_messages.append(message)
                except Empty:
                    break

            if remaining_messages:
                self.logger.info(f"Processing {len(remaining_messages)} remaining messages")
                self._process_batch(remaining_messages)

        except Exception as e:
            self.logger.error(f"Error processing remaining messages: {str(e)}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get processing metrics."""
        return {
            "messages_processed": self.messages_processed,
            "messages_dropped": self.messages_dropped,
            "processing_errors": self.processing_errors,
            "buffer_size": self._buffer.qsize(),
            "registered_processors": list(self._processors.keys()),
            "is_running": self._running
        }


class KafkaStreamConnector:
    """Kafka streaming connector (mock implementation for demonstration)."""

    def __init__(self, bootstrap_servers: List[str], topics: List[str]):
        self.logger = get_logger("agent_etl.kafka")
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self._running = False

    async def start_consuming(self, processor: StreamProcessor) -> None:
        """Start consuming messages from Kafka topics."""
        self.logger.info(f"Starting Kafka consumer for topics: {self.topics}")
        self._running = True

        # Mock Kafka consumer - in real implementation, use kafka-python or confluent-kafka
        while self._running:
            try:
                # Simulate receiving messages
                for topic in self.topics:
                    mock_data = {
                        "topic": topic,
                        "key": f"key_{int(time.time())}",
                        "value": {"sensor_id": "sensor_1", "temperature": 23.5, "humidity": 65.2},
                        "partition": 0,
                        "offset": int(time.time())
                    }
                    await processor.ingest(mock_data, f"kafka_{topic}")

                await asyncio.sleep(0.1)  # 100ms between messages

            except Exception as e:
                self.logger.error(f"Kafka consumption error: {str(e)}")
                await asyncio.sleep(1.0)  # Wait before retry

    async def stop(self) -> None:
        """Stop the Kafka consumer."""
        self.logger.info("Stopping Kafka consumer")
        self._running = False


# Simple usage example and default processors
def default_json_processor(messages: List[StreamMessage]) -> None:
    """Default processor that logs messages as JSON."""
    logger = get_logger("agent_etl.processors.json")

    for message in messages:
        logger.info(f"Processing message {message.message_id}: {json.dumps(message.data)}")


def database_sink_processor(messages: List[StreamMessage]) -> None:
    """Processor that would typically write to a database."""
    logger = get_logger("agent_etl.processors.database")

    # Mock database write
    logger.info(f"Writing {len(messages)} messages to database")
    for message in messages:
        # In real implementation, batch insert to database
        logger.debug(f"INSERT INTO events VALUES ({message.message_id}, {json.dumps(message.data)})")


async def demo_streaming_pipeline():
    """Demonstration of the streaming pipeline."""
    logger = get_logger("agent_etl.streaming.demo")

    # Create and configure streaming processor
    processor = StreamProcessor(buffer_size=500, batch_size=50, flush_interval=2.0)

    # Register processors for different data sources
    processor.register_processor("kafka_sensors", database_sink_processor)
    processor.register_processor("api_events", default_json_processor)

    # Start the processor
    await processor.start()

    try:
        # Simulate data ingestion
        for i in range(100):
            await processor.ingest(
                {"sensor_reading": i, "timestamp": time.time()},
                "kafka_sensors"
            )
            await processor.ingest(
                {"event_type": "user_action", "user_id": i, "action": "click"},
                "api_events"
            )

            if i % 10 == 0:
                metrics = processor.get_metrics()
                logger.info(f"Processing metrics: {metrics}")

            await asyncio.sleep(0.1)  # 100ms between ingestions

        # Let processing complete
        await asyncio.sleep(5.0)

    finally:
        await processor.stop()
        final_metrics = processor.get_metrics()
        logger.info(f"Final metrics: {final_metrics}")


if __name__ == "__main__":
    asyncio.run(demo_streaming_pipeline())
