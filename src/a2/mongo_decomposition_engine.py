"""
MongoDB Document Decomposition Engine
======================================
Phase 5B: Decides embed-vs-reference strategy for every MongoDB-bound field
using the five prioritised heuristics documented in ``Heuristic.md``.

Heuristic priority ladder (first match wins):
  P1-H1  List Cardinality / Growth         – is_array → Reference
  P1-H2  Shared Data Update Frequency      – nesting_depth ≥ 2 → Reference
  P2-H3  Data Volume / Object Size         – is_nested, not array, depth ≥ 1 → Reference
  P2-H4  Data Sparsity                     – not nested, shallow → Embed
  P3-H5  Data Stability / Polymorphism     – primitive canonical type → Embed
  Fallback                                 – Reference (safe default)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .contracts import ClassifiedField, CollectionPlan, SchemaRegistration

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal constants — all directly derived from Heuristic.md
# ---------------------------------------------------------------------------

#: H1 — Reference if list size exceeds this entry count.
H1_LIST_CARDINALITY_LIMIT: int = 100

#: H2 — Reference if nesting depth is at or beyond this value (shared sub-doc).
H2_NESTING_DEPTH_THRESHOLD: int = 2

#: H3 — Reference if the nested object lives at least this deep.
H3_NESTED_OBJECT_MIN_DEPTH: int = 1

#: H5 — Primitive canonical types are stable → safe to embed.
H5_PRIMITIVE_TYPES: frozenset[str] = frozenset(
    {"int", "str", "float", "bool", "date", "null", "string", "integer", "boolean"}
)

#: Backends that this engine handles.
MONGO_BACKENDS: frozenset[str] = frozenset({"MONGODB", "NOSQL", "BOTH"})


# ---------------------------------------------------------------------------
# Internal decision enum
# ---------------------------------------------------------------------------


class _EmbedDecision(str, Enum):
    EMBED = "embed"
    REFERENCE = "reference"


@dataclass
class _FieldVerdict:
    """Internal record produced for every evaluated field."""

    field_path: str
    decision: _EmbedDecision
    heuristic_applied: str          # e.g. "H1_list_cardinality"
    reasoning: str


# ---------------------------------------------------------------------------
# Public engine
# ---------------------------------------------------------------------------


class MongoDecompositionEngine:
    """Phase 5B: MongoDB document decomposition strategy.

    Consumes the ``ClassifiedField`` list produced by the A1 classification
    bridge and applies the five heuristics from ``Heuristic.md`` to decide
    whether each field/sub-document should be **embedded** inside the parent
    document or **referenced** as a separate MongoDB collection.

    Heuristic priority ladder (first match wins)
    --------------------------------------------
    Priority 1 — System Stability (Critical)
      H1  List Cardinality & Growth     : is_array → Reference
      H2  Shared Data Update Frequency  : nesting_depth ≥ 2 → Reference

    Priority 2 — Performance & Efficiency (High)
      H3  Data Volume / Object Size     : is_nested, not array, depth ≥ 1 → Reference
      H4  Data Sparsity (Presence)      : not nested, shallow → Embed

    Priority 3 — Maintenance & Flexibility (Medium)
      H5  Data Stability / Polymorphism : primitive canonical type → Embed

    Fallback  → Reference  (conservative default — avoids 16 MB document cap)
    """

    # ------------------------------------------------------------------
    # Phase 5B public API
    # ------------------------------------------------------------------

    def generate_collection_plans(
        self,
        registration: SchemaRegistration,
        classified_fields: list[ClassifiedField],
    ) -> list[CollectionPlan]:
        """Generate collection decomposition decisions for MongoDB.

        Uses classified_fields carrying A1's nesting analysis to decide
        embedding vs referencing without re-running classification.

        Args:
            registration:       Schema registration metadata (name, root entity).
            classified_fields:  Bridge output from A1's PlacementDecision list.

        Returns:
            A list containing one ``CollectionPlan`` for the root collection.
            Referenced sub-paths each get their own entry in
            ``CollectionPlan.reference_collections``.
        """
        root_collection = self._derive_collection_name(registration.root_entity)
        logger.info(
            "MongoDecompositionEngine: evaluating %d fields for collection '%s'",
            len(classified_fields),
            root_collection,
        )

        plan = CollectionPlan(collection_name=root_collection)
        verdicts: list[_FieldVerdict] = []

        for cf in classified_fields:
            if cf.backend.upper() not in MONGO_BACKENDS:
                logger.debug("Skipping non-Mongo field: %s (backend=%s)", cf.field_path, cf.backend)
                continue

            verdict = self._apply_heuristics(cf)
            verdicts.append(verdict)

            if verdict.decision is _EmbedDecision.EMBED:
                plan.embedded_paths.append(cf.field_path)
                logger.debug("[EMBED] %s — %s", cf.field_path, verdict.heuristic_applied)
            else:
                plan.referenced_paths.append(cf.field_path)
                ref_col = self._derive_reference_collection_name(root_collection, cf.field_path)
                plan.reference_collections[cf.field_path] = ref_col
                logger.debug("[REFERENCE] %s → %s — %s", cf.field_path, ref_col, verdict.heuristic_applied)

        self._log_summary(root_collection, verdicts)
        return [plan]

    def execute_collection_plans(
        self,
        collections: list[CollectionPlan],
        mongo_client,
    ) -> dict[str, Any]:
        """Create MongoDB collections and indexes from CollectionPlans.

        Steps
        -----
        1. For each ``CollectionPlan``, ensure the root collection exists in
           the connected MongoDB database.
        2. For every ``referenced_path``, create the linked reference
           collection and build an ascending index on ``_ref_id`` to support
           efficient ``$lookup`` / ``$eq`` joins.
        3. Embedded paths require no separate collection — data stays inside
           the parent document.

        Args:
            collections:  Collection plans from ``generate_collection_plans()``.
            mongo_client: Connected A1 ``MongoClient`` instance.

        Returns:
            ``{"collections_created": int, "indexes_created": int, "errors": list}``
        """
        db = self._resolve_database(mongo_client)
        existing_collections: set[str] = set(db.list_collection_names())

        collections_created = 0
        indexes_created = 0
        errors: list[dict[str, str]] = []

        for plan in collections:
            # --- 1. Root collection -------------------------------------------
            collections_created += self._ensure_collection(
                db, plan.collection_name, existing_collections, errors
            )

            # --- 2. Reference sub-collections + indexes -----------------------
            for ref_path, ref_col_name in plan.reference_collections.items():
                collections_created += self._ensure_collection(
                    db, ref_col_name, existing_collections, errors
                )
                try:
                    db[ref_col_name].create_index([("_ref_id", 1)], background=True)
                    indexes_created += 1
                    logger.info("Index created: %s._ref_id", ref_col_name)
                except Exception as exc:  # noqa: BLE001
                    msg = f"Index creation failed on {ref_col_name}._ref_id: {exc}"
                    logger.error(msg)
                    errors.append({"collection": ref_col_name, "error": msg})

                # Also index the parent reference key on the root collection
                parent_ref_key = f"{ref_path}._ref_id"
                try:
                    db[plan.collection_name].create_index([(parent_ref_key, 1)], background=True)
                    indexes_created += 1
                    logger.info("Index created: %s.%s", plan.collection_name, parent_ref_key)
                except Exception as exc:  # noqa: BLE001
                    msg = f"Parent ref-key index failed on {plan.collection_name}.{parent_ref_key}: {exc}"
                    logger.warning(msg)
                    # Non-fatal: root-side index is a performance optimisation only

        result = {
            "collections_created": collections_created,
            "indexes_created": indexes_created,
            "errors": errors,
        }
        logger.info("execute_collection_plans complete: %s", result)
        return result

    @staticmethod
    def _resolve_database(mongo_client):
        """Resolve a pymongo database handle from different client shapes."""
        if hasattr(mongo_client, "get_default_database"):
            return mongo_client.get_default_database()

        client = getattr(mongo_client, "client", None)
        database = getattr(mongo_client, "database", None)
        if client is not None and database:
            return client[database]

        raise ValueError(
            "Mongo client is missing a usable database handle. "
            "Expected get_default_database() or client+database attributes."
        )

    # ------------------------------------------------------------------
    # Heuristic engine (private)
    # ------------------------------------------------------------------

    def _apply_heuristics(self, cf: ClassifiedField) -> _FieldVerdict:
        """Apply the prioritised heuristic ladder to a single ClassifiedField.

        Returns the first heuristic that fires; falls back to Reference.
        """

        # ---- Priority 1 — System Stability (Critical) --------------------

        # H1: List Cardinality & Growth
        #   Arrays can grow unboundedly → risk of hitting 16 MB document cap.
        #   Heuristic.md: "Reference if list > 100 entries or grows by >5% monthly."
        #   We use is_array as the signal (A1 already detected array types).
        if cf.is_array:
            return _FieldVerdict(
                field_path=cf.field_path,
                decision=_EmbedDecision.REFERENCE,
                heuristic_applied="H1_list_cardinality",
                reasoning=(
                    f"Field is an array (is_array=True). Unbounded lists risk the 16 MB "
                    f"MongoDB document size cap (threshold: >{H1_LIST_CARDINALITY_LIMIT} entries). "
                    f"Referencing avoids write-amplification and document size violations."
                ),
            )

        # H2: Shared Data Update Frequency
        #   Deeply nested sub-documents are typically shared across many parent
        #   documents (e.g. category, author). Updating them embedded requires
        #   a scatter-write across every parent → write-amplification.
        #   Heuristic.md: "Reference if updated >50 times/day across the collection."
        #   Proxy used: nesting_depth ≥ 2 (detected by A1's nesting analysis).
        if cf.nesting_depth >= H2_NESTING_DEPTH_THRESHOLD:
            return _FieldVerdict(
                field_path=cf.field_path,
                decision=_EmbedDecision.REFERENCE,
                heuristic_applied="H2_shared_data_update_frequency",
                reasoning=(
                    f"nesting_depth={cf.nesting_depth} ≥ {H2_NESTING_DEPTH_THRESHOLD}. "
                    f"Deeply nested sub-documents are likely shared across many parent "
                    f"documents. Updating them embedded causes write-amplification."
                ),
            )

        # ---- Priority 2 — Performance & Efficiency (High) ---------------

        # H3: Data Volume / Object Size
        #   Even a handful of large nested objects bloat the parent document,
        #   wasting RAM on every parent fetch even if the nested data isn't needed.
        #   Heuristic.md: "Reference if related object size > 2KB."
        #   Proxy: is_nested=True, is_array=False, nesting_depth ≥ 1.
        if cf.is_nested and not cf.is_array and cf.nesting_depth >= H3_NESTED_OBJECT_MIN_DEPTH:
            return _FieldVerdict(
                field_path=cf.field_path,
                decision=_EmbedDecision.REFERENCE,
                heuristic_applied="H3_data_volume_object_size",
                reasoning=(
                    f"is_nested=True, is_array=False, nesting_depth={cf.nesting_depth}. "
                    f"Nested objects (> ~2KB) bloat parent documents and waste RAM on "
                    f"every fetch when the nested data is not required."
                ),
            )

        # H4: Data Sparsity (Presence)
        #   Fields absent in most records should stay embedded; referencing them
        #   leads to empty $lookup results (wasted CPU) and complex join logic.
        #   Heuristic.md: "Embed if Presence Count < 40%."
        #   Proxy: field is not nested and is shallow (nesting_depth == 0),
        #   meaning it is a first-class scalar — typically 100% present in its
        #   home document, and cheap to embed.
        if not cf.is_nested and cf.nesting_depth == 0:
            return _FieldVerdict(
                field_path=cf.field_path,
                decision=_EmbedDecision.EMBED,
                heuristic_applied="H4_data_sparsity",
                reasoning=(
                    f"is_nested=False, nesting_depth=0. Flat scalar field — "
                    f"referencing sparse data causes empty $lookups (wasted CPU). "
                    f"Embedding avoids unnecessary collection joins."
                ),
            )

        # ---- Priority 3 — Maintenance & Flexibility (Medium) ------------

        # H5: Data Stability / Polymorphism
        #   Unstable types (String→Int→Object) are best left embedded so that
        #   MongoDB's $type operator can handle them without post-join type checks.
        #   Heuristic.md: "Embed if Type Stability < 90%."
        #   Proxy: canonical_type is a primitive (stable by nature).
        if cf.canonical_type.lower() in H5_PRIMITIVE_TYPES:
            return _FieldVerdict(
                field_path=cf.field_path,
                decision=_EmbedDecision.EMBED,
                heuristic_applied="H5_data_stability_polymorphism",
                reasoning=(
                    f"canonical_type='{cf.canonical_type}' is a primitive/stable type. "
                    f"Embedding avoids complex type-checking logic after $lookup joins."
                ),
            )

        # ---- Fallback ---------------------------------------------------
        # Conservative default: Reference.
        # Referencing is always safer than embedding — it sidesteps the 16 MB
        # cap and write-amplification even if performance may be slightly lower.
        return _FieldVerdict(
            field_path=cf.field_path,
            decision=_EmbedDecision.REFERENCE,
            heuristic_applied="FALLBACK_conservative_reference",
            reasoning=(
                f"No heuristic fired definitively for canonical_type='{cf.canonical_type}', "
                f"is_array={cf.is_array}, is_nested={cf.is_nested}, "
                f"nesting_depth={cf.nesting_depth}. "
                f"Defaulting to Reference — safe against 16 MB cap and write amplification."
            ),
        )

    # ------------------------------------------------------------------
    # Utility helpers (private)
    # ------------------------------------------------------------------

    @staticmethod
    def _derive_collection_name(entity_name: str) -> str:
        """Convert a schema root-entity name to a snake_case MongoDB collection name.

        Examples:
            "UserProfile"  → "user_profiles"
            "BlogPost"     → "blog_posts"
            "order"        → "orders"
        """
        # CamelCase → snake_case
        snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", entity_name).lower()
        # Pluralise naively (handles most English nouns)
        if snake.endswith("y"):
            return snake[:-1] + "ies"
        if not snake.endswith("s"):
            return snake + "s"
        return snake

    @staticmethod
    def _derive_reference_collection_name(root_collection: str, field_path: str) -> str:
        """Build the name for a reference sub-collection from a dot-notation field path.

        Examples:
            root="user_profiles", path="address"          → "user_profiles_addresses"
            root="blog_posts",    path="post.comments"    → "blog_posts_comments"
        """
        # Use only the leaf segment of the path
        leaf = field_path.split(".")[-1].lower()
        leaf = re.sub(r"[^a-z0-9_]", "_", leaf)
        if leaf.endswith("y"):
            leaf = leaf[:-1] + "ies"
        elif not leaf.endswith("s"):
            leaf = leaf + "s"
        return f"{root_collection}_{leaf}"

    @staticmethod
    def _ensure_collection(
        db,
        collection_name: str,
        existing: set[str],
        errors: list[dict[str, str]],
    ) -> int:
        """Create *collection_name* in *db* if it does not already exist.

        Returns 1 if a new collection was created, 0 otherwise.
        """
        if collection_name in existing:
            logger.debug("Collection already exists: %s", collection_name)
            return 0
        try:
            db.create_collection(collection_name)
            existing.add(collection_name)
            logger.info("Collection created: %s", collection_name)
            return 1
        except Exception as exc:  # noqa: BLE001
            msg = f"Failed to create collection '{collection_name}': {exc}"
            logger.error(msg)
            errors.append({"collection": collection_name, "error": msg})
            return 0

    @staticmethod
    def _log_summary(collection_name: str, verdicts: list[_FieldVerdict]) -> None:
        """Emit an INFO-level summary table of all heuristic decisions."""
        embedded = [v for v in verdicts if v.decision is _EmbedDecision.EMBED]
        referenced = [v for v in verdicts if v.decision is _EmbedDecision.REFERENCE]
        logger.info(
            "Decomposition summary for '%s': %d embedded, %d referenced (total=%d fields).",
            collection_name,
            len(embedded),
            len(referenced),
            len(verdicts),
        )
        for v in verdicts:
            tag = "EMBED    " if v.decision is _EmbedDecision.EMBED else "REFERENCE"
            logger.info("  [%s] %-40s via %s", tag, v.field_path, v.heuristic_applied)
