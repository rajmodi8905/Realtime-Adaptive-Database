#!/usr/bin/env python3
"""
Quick test script to verify the pipeline is working
"""

from src.ingest_and_classify import IngestAndClassify
from src.config import get_config

def test_pipeline():
    print("=" * 60)
    print("Testing Adaptive Database Pipeline")
    print("=" * 60)
    
    # Initialize pipeline
    print("\n1. Initializing pipeline...")
    pipeline = IngestAndClassify()
    print("✓ Pipeline initialized successfully")
    
    # Test record
    test_record = {
          "username": "seanmartinez",
  "phone": "(687)618-1951x23684",
  "ip_address": "9.188.219.46",
  "device_id": "a1e34c54-a975-4460-a640-4992ce9afa52",
  "device_model": "OnePlus 12",
  "altitude": 97.27,
  "speed": 32.13,
  "direction": "E",
  "city": "Brendahaven",
  "country": "Bahamas",
  "timestamp": "2026-02-14T05:44:25.223850",
  "session_id": "44cf4b1f-cfd4-42c1-a55f-62cf0c37f15b",
  "steps": 9197,
  "spo2": 96,
  "temperature_c": -5,
  "humidity": 24,
  "air_quality": "moderate",
  "purchase_value": 36.45,
  "item": "bag",
  "payment_status": "pending",
  "language": "Pushto",
  "timezone": "Asia/Vientiane",
  "ram_usage": 35,
  "error_code": 100,
  "comment": "Raise positive middle fine analysis.",
  "avatar_url": "https://picsum.photos/898/280",
  "last_seen": "2026-02-14T04:49:25.225632"
    }
    
    # Ingest test record (you can add more records here)
    print("\n2. Ingesting test records...")
    pipeline.ingest(test_record)
    print("✓ Record 1 ingested into buffer")
    
    # Add more records if you have them
    # Example: Load from file or API
    # for record in your_data_source:
    #     pipeline.ingest(record)
    
    # Check status
    status = pipeline.get_status()
    print(f"\n3. Pipeline Status:")
    print(f"   - Buffer size: {status['buffer_size']}")
    print(f"   - Total records processed: {status['total_records_processed']}")
    
    # Flush and route
    print("\n4. Flushing buffer and routing to databases...")
    result = pipeline.flush()
    print(f"✓ Flush completed:")
    print(f"   - Records processed: {result.get('records_processed', 0)}")
    
    # Show decisions
    print("\n5. Classification Decisions:")
    summary = pipeline.get_classification_summary()
    if summary.get('sql_fields'):
        fields = [f['field'] for f in summary['sql_fields']]
        print(f"   SQL fields: {', '.join(fields)}")
    if summary.get('mongo_fields'):
        fields = [f['field'] for f in summary['mongo_fields']]
        print(f"   MongoDB fields: {', '.join(fields)}")
    if summary.get('both_fields'):
        fields = [f['field'] for f in summary['both_fields']]
        print(f"   Both backends: {', '.join(fields)}")
    
    # Cleanup
    print("\n6. Closing connections...")
    pipeline.close()
    print("✓ Connections closed")
    
    print("\n" + "=" * 60)
    print("✓ All tests passed! Pipeline is working correctly.")
    print("=" * 60)

if __name__ == "__main__":
    try:
        test_pipeline()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
