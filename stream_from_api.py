#!/usr/bin/env python3
"""
Stream data from the synthetic data API into the adaptive database pipeline
"""

import requests
import time
from src.ingest_and_classify import IngestAndClassify
from src.config import get_config

def stream_data(api_url: str = "http://127.0.0.1:8000/", max_records: int = None, delay: float = 0.1):
    """
    Stream data from the API into the pipeline.
    
    Args:
        api_url: The API endpoint URL
        max_records: Maximum number of records to ingest (None = infinite)
        delay: Delay between requests in seconds
    """
    print("=" * 60)
    print("Streaming Data from API to Adaptive Database")
    print("=" * 60)
    
    # Initialize pipeline
    print(f"\n1. Initializing pipeline...")
    pipeline = IngestAndClassify()
    print("✓ Pipeline initialized successfully")
    
    print(f"\n2. Starting data stream from {api_url}")
    print(f"   Max records: {max_records if max_records else 'unlimited'}")
    print(f"   Delay: {delay}s between requests")
    print("\n   Press Ctrl+C to stop streaming...\n")
    
    records_ingested = 0
    errors = 0
    start_time = time.time()
    
    try:
        while max_records is None or records_ingested < max_records:
            try:
                # Fetch record from API
                response = requests.get(api_url, timeout=10)
                response.raise_for_status()
                record = response.json()
                
                # Ingest into pipeline
                pipeline.ingest(record)
                records_ingested += 1
                
                # Print progress every 10 records
                if records_ingested % 10 == 0:
                    status = pipeline.get_status()
                    elapsed = time.time() - start_time
                    rate = records_ingested / elapsed if elapsed > 0 else 0
                    print(f"   ✓ {records_ingested} records ingested "
                          f"(Buffer: {status['buffer_size']}, "
                          f"Processed: {status['total_records_processed']}, "
                          f"Rate: {rate:.1f} rec/s)")
                
                # Small delay between requests
                time.sleep(delay)
                
            except requests.RequestException as e:
                errors += 1
                print(f"   ✗ API error: {e}")
                if errors > 10:
                    print("\n   Too many errors, stopping...")
                    break
                time.sleep(1)  # Wait before retry
                
    except KeyboardInterrupt:
        print("\n\n3. Stopping stream (Ctrl+C detected)...")
    
    # Final flush
    print("\n4. Flushing remaining buffer...")
    result = pipeline.flush()
    print(f"   ✓ Flushed {result.get('records_processed', 0)} records")
    
    # Show final status
    elapsed = time.time() - start_time
    status = pipeline.get_status()
    print(f"\n5. Final Statistics:")
    print(f"   - Total records ingested: {records_ingested}")
    print(f"   - Total records processed: {status['total_records_processed']}")
    print(f"   - Errors: {errors}")
    print(f"   - Time elapsed: {elapsed:.1f}s")
    print(f"   - Average rate: {records_ingested/elapsed:.1f} records/sec")
    
    # Show classification summary
    print("\n6. Classification Summary:")
    summary = pipeline.get_classification_summary()
    if summary.get('sql_fields'):
        print(f"   SQL fields: {len(summary['sql_fields'])}")
    if summary.get('mongo_fields'):
        print(f"   MongoDB fields: {len(summary['mongo_fields'])}")
    if summary.get('both_fields'):
        print(f"   Both backends: {len(summary['both_fields'])}")
    
    # Cleanup
    print("\n7. Closing connections...")
    pipeline.close()
    print("✓ Connections closed")
    
    print("\n" + "=" * 60)
    print("✓ Streaming completed successfully!")
    print("=" * 60)


def stream_batch(api_url: str = "http://127.0.0.1:8000/record/100", count: int = 100):
    """
    Stream a batch of records from API using the /record/{count} endpoint.
    
    Args:
        api_url: The batch API endpoint URL
        count: Number of records to fetch in the batch
    """
    print("=" * 60)
    print(f"Streaming Batch of {count} Records from API")
    print("=" * 60)
    
    # Initialize pipeline
    print(f"\n1. Initializing pipeline...")
    pipeline = IngestAndClassify()
    print("✓ Pipeline initialized successfully")
    
    print(f"\n2. Fetching batch from {api_url}")
    
    try:
        # The API returns Server-Sent Events (SSE)
        response = requests.get(api_url, stream=True, timeout=60)
        response.raise_for_status()
        
        records_ingested = 0
        start_time = time.time()
        
        # Parse SSE stream
        for line in response.iter_lines():
            if line:
                decoded = line.decode('utf-8')
                if decoded.startswith('data:'):
                    json_str = decoded[5:].strip()
                    try:
                        record = eval(json_str)  # API sends single-quoted JSON
                        pipeline.ingest(record)
                        records_ingested += 1
                        
                        if records_ingested % 10 == 0:
                            print(f"   ✓ {records_ingested} records ingested...")
                    except Exception as e:
                        print(f"   ✗ Parse error: {e}")
        
        # Final flush
        print(f"\n3. Flushing buffer...")
        result = pipeline.flush()
        print(f"   ✓ Flushed {result.get('records_processed', 0)} records")
        
        # Show final status
        elapsed = time.time() - start_time
        status = pipeline.get_status()
        print(f"\n4. Final Statistics:")
        print(f"   - Total records ingested: {records_ingested}")
        print(f"   - Total records processed: {status['total_records_processed']}")
        print(f"   - Time elapsed: {elapsed:.1f}s")
        print(f"   - Average rate: {records_ingested/elapsed:.1f} records/sec")
        
        # Cleanup
        print("\n5. Closing connections...")
        pipeline.close()
        print("✓ Connections closed")
        
        print("\n" + "=" * 60)
        print("✓ Batch streaming completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        pipeline.close()


if __name__ == "__main__":
    import sys
    
    # Parse command line arguments
    mode = sys.argv[1] if len(sys.argv) > 1 else "single"
    
    if mode == "batch":
        # Batch mode: fetch 100 records at once
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        stream_batch(f"http://127.0.0.1:8000/record/{count}", count)
    else:
        # Single record streaming mode (default)
        max_records = int(sys.argv[2]) if len(sys.argv) > 2 else None
        stream_data(max_records=max_records, delay=0.1)
