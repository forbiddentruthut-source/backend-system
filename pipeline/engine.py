import asyncio
import time
import logging
from typing import List, Dict, Any
from dataclasses import dataclass

# Configuring professional structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("EnterpriseAsyncPipeline")

@dataclass
class PipelineMetrics:
    total_events: int = 0
    successful_events: int = 0
    failed_events: int = 0
    execution_time_sec: float = 0.0

class HighThroughputAsyncEngine:
    """
    Enterprise-grade asynchronous event processing engine designed for 
    high-throughput, non-blocking data ingestion and stream transformation.
    """
    def __init__(self, concurrency_limit: int = 50, max_retries: int = 3):
        self.concurrency_limit = concurrency_limit
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(concurrency_limit)

    async def _process_with_retry(
        self, event_id: int, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Executes an event processing task with exponential backoff retry logic."""
        attempt = 0
        backoff_factor = 0.5

        async with self.semaphore:
            while attempt < self.max_retries:
                try:
                    attempt += 1
                    # Simulating heavy backend operation / database transaction / API call
                    await asyncio.sleep(0.1) 
                    
                    # Simulated condition for transient error testing
                    if payload.get("trigger_error") and attempt == 1:
                        raise ConnectionError("Transient upstream service timeout.")

                    logger.debug(f"Event ID {event_id} processed successfully on attempt {attempt}")
                    return {
                        "event_id": event_id,
                        "status": "SUCCESS",
                        "attempts": attempt,
                        "timestamp": time.time()
                    }
                except Exception as e:
                    logger.warning(f"Event ID {event_id} failed on attempt {attempt}: {str(e)}")
                    if attempt >= self.max_retries:
                        logger.error(f"Event ID {event_id} permanently failed after {self.max_retries} attempts.")
                        return {
                            "event_id": event_id,
                            "status": "FAILED",
                            "error": str(e),
                            "timestamp": time.time()
                        }
                    # Exponential backoff delay
                    await asyncio.sleep(backoff_factor * (2 ** (attempt - 1)))

    async def execute_pipeline(self, events: List[Dict[str, Any]]) -> PipelineMetrics:
        """Executes the concurrent pipeline across a batch of payloads and aggregates metrics."""
        start_time = time.time()
        logger.info(f"Initiating pipeline execution for batch size: {len(events)} with concurrency limit: {self.concurrency_limit}")

        tasks = [
            self._process_with_retry(i, event_data)
            for i, event_data in enumerate(events, start=1)
        ]

        results = await asyncio.gather(*tasks)
        
        end_time = time.time()
        duration = end_time - start_time

        metrics = PipelineMetrics(
            total_events=len(events),
            successful_events=sum(1 for r in results if r["status"] == "SUCCESS"),
            failed_events=sum(1 for r in results if r["status"] == "FAILED"),
            execution_time_sec=round(duration, 4)
        )

        logger.info(
            f"Pipeline Execution Complete. "
            f"Success: {metrics.successful_events}/{metrics.total_events}, "
            f"Time: {metrics.execution_time_sec}s"
        )
        return metrics
