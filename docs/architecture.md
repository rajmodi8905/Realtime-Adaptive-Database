# Architecture

## Overview

The project combines:

- **A1**: ingestion, normalization, adaptive field classification
- **A2**: metadata-driven SQL/Mongo storage planning and CRUD routing
- **A3**: transactional coordination, concurrency control, and logical reconstruction
- **Dashboard**: FastAPI API + React frontend for end-user operations

## High-Level Flow

1. Schema is registered and metadata is initialized.
2. Records are normalized and analyzed.
3. Fields are classified into SQL, MongoDB, or both.
4. Storage strategy is generated and persisted in `metadata/`.
5. CRUD/query requests are planned and executed across both backends.
6. A3 coordinates transactional behavior and reconstructs logical entities.
7. Dashboard exposes a backend-agnostic API and UI.

## Source Layout

- `src/analysis/`: field analysis and classification logic.
- `src/normalization/`: type detection and record normalization.
- `src/storage/`: MySQL/Mongo clients and low-level routing.
- `src/persistence/`: metadata persistence.
- `src/a2/`: schema registry, planner, CRUD engine, and orchestration.
- `src/a3/`: transaction coordinator, lock manager, ACID experiments.
- `dashboard/api_server.py`: FastAPI logical API and static serving.
- `dashboard/frontend/`: Vite + React frontend.
- `metadata/`: generated schema, plans, and query history artifacts.

## Runtime Components

- **MySQL 8.0**: relational storage for structured data.
- **MongoDB 7.0**: document storage for nested/sparse data.
- **FastAPI app**: orchestration and logical endpoint surface.
- **Vite frontend**: interactive dashboard.
- **k6 script (`load_test.js`)**: request-level performance/load validation.
