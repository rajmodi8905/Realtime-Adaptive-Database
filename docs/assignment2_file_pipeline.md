# Assignment 2 — File Pipeline Blueprint

This document maps each Assignment-2 phase to files in this repository and explains how they connect. Use it to understand the pipeline and divide implementation work.

## Architecture Overview

A2 **reuses the A1 pipeline** (`IngestAndClassify`) for ingestion, analysis, and classification (Phases 2–4). The A2 orchestrator (`Assignment2Pipeline`) consumes A1's classification output via a `ClassifiedField` bridge — classification logic runs once, never duplicated.

```
Records → [A1: IngestAndClassify] → PlacementDecisions + FieldStats
                                            │
                                    ClassifiedField.from_a1_decision()
                                            │
                                            ▼
                               [A2: Assignment2Pipeline]
                              ┌─────────┴─────────┐
                     SqlNormalizationEngine   MongoDecompositionEngine
                              │                    │
                     execute_table_plans()   execute_collection_plans()
                              └─────────┬─────────┘
                                 QueryPlanner
                                     │
                                 CrudEngine
                        (read / insert / update / delete)
```

## Phase 1 — Schema Registration

| File | Role | Status |
|------|------|--------|
| `src/a2/schema_registry.py` | Register versioned JSON schemas, validate records | Stub |
| `src/a2/contracts.py` → `SchemaRegistration` | User-provided schema payload dataclass | Done |

## Phase 2–4 — Ingestion, Analysis, Classification (A1 Reuse)

These phases are handled entirely by A1. A2 calls `a1_pipeline.ingest_batch()` + `a1_pipeline.flush()` then bridges the results.

| File | Role |
|------|------|
| `src/ingest_and_classify.py` | A1 orchestrator (normalize → analyze → classify → route) |
| `src/normalization/type_detector.py` | Detect value types (int, str, array, object, etc.) |
| `src/normalization/record_normalizer.py` | Normalize raw records (add timestamps, flatten keys) |
| `src/analysis/field_analyzer.py` | Accumulate per-field stats (FieldStats) with nesting info |
| `src/analysis/classifier.py` | Classify fields → SQL / MongoDB / BOTH |
| `src/analysis/decision.py` | PlacementDecision dataclass |

## ClassifiedField Bridge

| File | Role | Status |
|------|------|--------|
| `src/a2/contracts.py` → `ClassifiedField` | Converts A1's `PlacementDecision` + `FieldStats` into A2's domain | Done |

`ClassifiedField.from_a1_decision(decision, stats)` carries: `field_path`, `backend`, `canonical_type`, `is_array`, `is_nested`, `nesting_depth`, `parent_path`, `sql_type`, `is_primary_key`, `is_nullable`, `is_unique`.

## Phase 5 — Storage Strategy Generation

| File | Role | Status |
|------|------|--------|
| `src/a2/sql_normalization_engine.py` | Detect repeating groups → generate normalized tables → execute CREATE TABLE | Stub |
| `src/a2/mongo_decomposition_engine.py` | Decide embed vs reference → generate collection plans → execute collection creation | Stub |
| `src/a2/storage_strategy_generator.py` | Combine SQL + Mongo plans into field-level `FieldLocation` map | Stub |

Key methods:
- `SqlNormalizationEngine.generate_table_plans(registration, classified_fields)` → `list[SqlTablePlan]`
- `SqlNormalizationEngine.generate_relationships(tables)` → `list[RelationshipPlan]`
- `SqlNormalizationEngine.execute_table_plans(tables, relationships, mysql_client)` → creates tables
- `MongoDecompositionEngine.generate_collection_plans(registration, classified_fields)` → `list[CollectionPlan]`
- `MongoDecompositionEngine.execute_collection_plans(collections, mongo_client)` → creates collections

## Phase 6 — CRUD Operations

| File | Role | Status |
|------|------|--------|
| `src/a2/query_planner.py` | Generate `QueryPlan` from user JSON request + field locations | Stub |
| `src/a2/crud_engine.py` | Execute QueryPlans on MySQL + MongoDB backends | Dispatch done, operations stub |

CrudEngine operations:
- `_execute_read()` — translate fields → SQL/Mongo queries → merge results
- `_execute_insert()` — split JSON → insert into SQL + MongoDB → maintain join keys
- `_execute_update()` — modify records with schema consistency validation
- `_execute_delete()` — cascade deletion across SQL + MongoDB

## Orchestration + Metadata

| File | Role | Status |
|------|------|--------|
| `src/a2/orchestrator.py` | `Assignment2Pipeline` — ties A1 + A2 together | Integration done, phase methods stub |
| `src/a2/metadata_catalog.py` | Central metadata store for schemas, plans, and field locations | Stub |

## Supporting Files

| File | Role |
|------|------|
| `schemas/assignment2_schema.template.json` | Template schema registration payload |
| `raw_data/assignment2_dataset.template.json` | Template dataset with nested entities |
| `src/a2/contracts.py` | All A2 dataclasses (`SqlTablePlan`, `CollectionPlan`, `FieldLocation`, `QueryPlan`, etc.) |

## Implementation Sequence

1. **Schema registry** — implement `SchemaRegistry.register()` and `SchemaRegistry.get()`.
2. **SQL normalization engine** — implement `generate_table_plans()` (detect repeating groups from `ClassifiedField.is_array` / `parent_path`), `generate_relationships()`, and `execute_table_plans()`.
3. **Mongo decomposition engine** — implement `generate_collection_plans()` (embed vs reference using `ClassifiedField` nesting info) and `execute_collection_plans()`.
4. **Storage strategy generator** — implement `generate_field_locations()` to merge SQL + Mongo plans.
5. **Metadata catalog** — implement persistence for schemas, plans, and field locations.
6. **Query planner** — implement `build_plan()` using field locations to generate SQL/Mongo queries.
7. **CRUD engine** — implement `_execute_read`, `_execute_insert`, `_execute_update`, `_execute_delete`.
