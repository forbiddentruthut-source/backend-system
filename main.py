import asyncio
from pipeline.engine import HighThroughputAsyncEngine

async def main():
    # Generating a simulated high-volume event stream (e.g., 200 concurrent events)
    simulated_events = [{"payload_id": i, "trigger_error": (i % 17 == 0)} for i in range(1, 201)]
    
    # Instantiate engine with high concurrency limits and fault tolerance
    engine = HighThroughputAsyncEngine(concurrency_limit=20, max_retries=3)
    
    # Run pipeline and capture metrics
    metrics = await engine.execute_pipeline(simulated_events)
    
    print("\n==============================")
    print(" PIPELINE EXECUTION REPORT ")
    print("==============================")
    print(f"Total Events Processed : {metrics.total_events}")
    print(f"Successful Dispatches  : {metrics.successful_events}")
    print(f"Failed Dispatches      : {metrics.failed_events}")
    print(f"Total Execution Time   : {metrics.execution_time_sec} seconds")
    print("==============================\n")

if __name__ == "__main__":
    asyncio.run(main())
