"""
==============================================
Pipeline Demo/Example Script
==============================================

This module demonstrates how to use the IngestAndClassify pipeline
for streaming data ingestion with automatic classification and routing.

USAGE EXAMPLES:

1. Basic streaming ingestion:
    from src.pipeline import StreamingPipeline
    
    pipeline = StreamingPipeline()
    pipeline.start_streaming()

2. Manual batch processing:
    from src.pipeline import StreamingPipeline
    
    pipeline = StreamingPipeline()
    records = [
        {"username": "alice", "age": 30, "city": "NYC"},
        {"username": "bob", "score": 95.5, "metadata": {"level": 5}}
    ]
    pipeline.process_batch(records)

3. Context manager (auto-cleanup):
    with StreamingPipeline() as pipeline:
        pipeline.start_streaming(max_records=100)

4. Check pipeline status:
    pipeline = StreamingPipeline()
    status = pipeline.get_pipeline_status()
    print(status)
    
    summary = pipeline.get_classification_summary()
    print(summary)
"""

import time
import requests
from typing import Optional

from src.config import get_config, AppConfig
from src.ingest_and_classify import IngestAndClassify


class StreamingPipeline:
    """
    High-level wrapper around IngestAndClassify for streaming data ingestion.
    Provides convenient methods for different ingestion patterns.
    """
    
    def __init__(self, config: Optional[AppConfig] = None):
        """
        Initialize the streaming pipeline.
        
        Args:
            config: Optional configuration. If None, loads from environment.
        
        USES:
            - IngestAndClassify (which uses all 4 topics)
        """
        self._config = config or get_config()
        self._pipeline = IngestAndClassify(self._config)
        self._is_running = False
        self._records_ingested = 0
        
    def start_streaming(
        self,
        max_records: Optional[int] = None,
        interval_seconds: float = 0.1
    ) -> dict:
        """
        Start streaming data from the configured data source.
        
        Args:
            max_records: Maximum records to ingest (None = indefinite)
            interval_seconds: Delay between fetches
            
        Returns:
            Summary statistics
        """
        print(f"🚀 Starting streaming ingestion from {self._config.data_stream_url}")
        if max_records:
            print(f"   → Will stop after {max_records} records")
        else:
            print("   → Press Ctrl+C to stop")
        
        self._is_running = True
        self._records_ingested = 0
        start_time = time.time()
        
        try:
            while self._is_running:
                # Check if we've reached the limit
                if max_records and self._records_ingested >= max_records:
                    print(f"\n✓ Reached target of {max_records} records")
                    break
                
                # Fetch a record from the data stream
                try:
                    record = self._fetch_record()
                    if record:
                        # Uses Topic 1: RecordNormalizer.normalize()
                        self._pipeline.ingest(record)
                        self._records_ingested += 1
                        
                        # Print progress every 10 records
                        if self._records_ingested % 10 == 0:
                            print(f"   → Ingested {self._records_ingested} records...", end='\r')
                    
                except Exception as e:
                    print(f"\n⚠ Error fetching record: {e}")
                
                # Small delay between fetches
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n⚠ Interrupted by user")
        finally:
            # Flush any remaining records
            print("\n🔄 Flushing remaining records...")
            # Uses Topics 1,2,3,4: All components in flush()
            flush_result = self._pipeline.flush()
            
            elapsed = time.time() - start_time
            
            summary = {
                "records_ingested": self._records_ingested,
                "elapsed_seconds": round(elapsed, 2),
                "records_per_second": round(self._records_ingested / elapsed, 2) if elapsed > 0 else 0,
                "final_flush": flush_result
            }
            
            print(f"\n📊 Summary:")
            print(f"   → Total records: {summary['records_ingested']}")
            print(f"   → Time elapsed: {summary['elapsed_seconds']}s")
            print(f"   → Rate: {summary['records_per_second']} records/sec")
            
            return summary
    
    def stop_streaming(self) -> None:
        """Stop the streaming ingestion."""
        self._is_running = False
    
    def process_batch(self, records: list[dict]) -> dict:
        """
        Process a batch of records and return the result.
        
        Args:
            records: List of raw JSON records
            
        Returns:
            Flush result with statistics
        
        USES:
            - Topic 1 (normalization/): RecordNormalizer.normalize_batch()
            - Topic 2 (analysis/): FieldAnalyzer.analyze_batch(), Classifier.classify_all()
            - Topic 3 (storage/): RecordRouter.route_batch()
            - Topic 4 (persistence/): MetadataStore.save_all()
        """
        print(f"📥 Processing batch of {len(records)} records...")
        self._pipeline.ingest_batch(records)
        return self._pipeline.flush()
    
    def process_single(self, record: dict) -> None:
        """
        Process a single record.
        
        Args:
            record: Raw JSON record
        
        USES:
            - Topic 1 (normalization/): RecordNormalizer.normalize()
        """
        self._pipeline.ingest(record)
    
    def manual_flush(self) -> dict:
        """
        Manually trigger a flush of buffered records.
        
        Returns:
            Flush result with statistics
        
        USES:
            - Topic 2 (analysis/): FieldAnalyzer.analyze_batch(), Classifier.classify_all()
            - Topic 3 (storage/): RecordRouter.route_batch()
            - Topic 4 (persistence/): MetadataStore.save_all()
        """
        return self._pipeline.flush()
    
    def get_pipeline_status(self) -> dict:
        """
        Get current pipeline status.
        
        Returns:
            Status dictionary with buffer info, records processed, etc.
        """
        return self._pipeline.get_status()
    
    def get_classification_summary(self) -> dict:
        """
        Get summary of how fields are classified.
        
        Returns:
            Summary of SQL vs MongoDB field assignments
        
        USES:
            - Topic 2 (analysis/): PlacementDecision objects from Classifier
        """
        return self._pipeline.get_classification_summary()
    
    def get_field_decisions(self) -> dict[str, dict]:
        """
        Get detailed placement decisions for all fields.
        
        Returns:
            Dictionary of field names to their decision details
        
        USES:
            - Topic 2 (analysis/): PlacementDecision objects from Classifier
        """
        decisions = self._pipeline.get_decisions()
        return {
            name: {
                "backend": decision.backend.name,
                "sql_type": decision.sql_type,
                "nullable": decision.is_nullable,
                "unique": decision.is_unique,
                "reason": decision.reason
            }
            for name, decision in decisions.items()
        }
    
    def _fetch_record(self) -> Optional[dict]:
        """
        Fetch a single record from the data stream.
        
        Returns:
            Record dictionary or None on error
        """
        try:
            response = requests.get(
                self._config.data_stream_url,
                timeout=5
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"\n⚠ Failed to fetch record: {e}")
            return None
    
    def close(self) -> None:
        """
        Close the pipeline and cleanup resources.
        
        USES:
            - Topic 3 (storage/): MySQLClient.close(), MongoClient.close()
        """
        self._pipeline.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False


# ==============================================
# CLI / Demo Functions
# ==============================================

def demo_basic_usage():
    """Demonstrate basic pipeline usage."""
    print("=" * 60)
    print("DEMO: Basic Pipeline Usage")
    print("=" * 60)
    
    # Sample records
    sample_records = [
        {
            "username": "alice",
            "age": 30,
            "city": "New York",
            "score": 95.5
        },
        {
            "username": "bob",
            "age": 25,
            "city": "San Francisco",
            "metadata": {
                "level": 5,
                "premium": True
            }
        },
        {
            "username": "charlie",
            "score": 88.0,
            "tags": ["python", "databases"]
        }
    ]
    
    # Create pipeline
    pipeline = StreamingPipeline()
    
    # Process batch
    print("\n1. Processing batch of records...")
    result = pipeline.process_batch(sample_records)
    print(f"   ✓ Processed {result['records_processed']} records")
    
    # Check status
    print("\n2. Checking pipeline status...")
    status = pipeline.get_pipeline_status()
    print(f"   → Total records: {status['total_records_processed']}")
    print(f"   → Fields discovered: {status['fields_discovered']}")
    
    # Get classification summary
    print("\n3. Classification summary...")
    summary = pipeline.get_classification_summary()
    print(f"   → SQL fields: {summary['counts']['sql']}")
    print(f"   → MongoDB fields: {summary['counts']['mongo']}")
    print(f"   → Both: {summary['counts']['both']}")
    
    # Show field decisions
    print("\n4. Field decisions:")
    decisions = pipeline.get_field_decisions()
    for field, decision in decisions.items():
        print(f"   → {field}: {decision['backend']} ({decision['reason']})")
    
    # Cleanup
    pipeline.close()
    print("\n✓ Demo complete!")


def demo_streaming():
    """Demonstrate streaming ingestion."""
    print("=" * 60)
    print("DEMO: Streaming Ingestion")
    print("=" * 60)
    
    with StreamingPipeline() as pipeline:
        # Stream 50 records
        pipeline.start_streaming(max_records=50)
        
        # Show final summary
        print("\nFinal classification:")
        summary = pipeline.get_classification_summary()
        print(f"SQL fields: {summary['counts']['sql']}")
        print(f"MongoDB fields: {summary['counts']['mongo']}")


def main():
    """Main entry point."""
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "demo":
            demo_basic_usage()
        elif command == "stream":
            max_records = int(sys.argv[2]) if len(sys.argv) > 2 else None
            pipeline = StreamingPipeline()
            pipeline.start_streaming(max_records=max_records)
        else:
            print(f"Unknown command: {command}")
            print("Usage: python -m src.pipeline [demo|stream [max_records]]")
    else:
        print("Realtime Adaptive Database Pipeline")
        print("=" * 60)
        print("\nUsage:")
        print("  python -m src.pipeline demo          # Run demo with sample data")
        print("  python -m src.pipeline stream        # Stream from data source")
        print("  python -m src.pipeline stream 100    # Stream 100 records")


if __name__ == "__main__":
    main()
