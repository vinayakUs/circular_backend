from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class CircularReference:
    """Input payload for an outgoing reference edge."""
    relationship_type: str
    target_circular_number: str
    target_circular_id: UUID | None = None


class CircularReferenceRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("CircularReferenceRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def replace_references(
        self,
        source_circular_id: UUID,
        references: list[CircularReference],
    ) -> int:
        """Idempotent: delete outgoing refs for `source_circular_id`, then insert new ones.

        Returns the count written. Safe to call repeatedly for the same source —
        UNIQUE INDEX uq_circular_references_edge prevents duplicates within a batch.
        """
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM circular_references WHERE source_circular_id = %s",
                (str(source_circular_id),),
            )
            for ref in references:
                cursor.execute(
                    """
                    INSERT INTO circular_references (
                        source_circular_id, target_circular_id,
                        target_circular_number, relationship_type
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        str(source_circular_id),
                        str(ref.target_circular_id) if ref.target_circular_id else None,
                        ref.target_circular_number,
                        ref.relationship_type,
                    ),
                )
            conn.commit()
        self.logger.info(
            "Replaced references source_circular_id=%s count=%s",
            source_circular_id, len(references),
        )
        return len(references)

    # ─────────────────────────────────────────────────────────────────────────
    # GRAPH TRAVERSAL
    # ─────────────────────────────────────────────────────────────────────────

    # Hard cap on BFS depth as a safety net against runaway recursion in dense graphs.
    _MAX_GRAPH_DEPTH = 50

    def get_neighborhood(
        self, root_id: UUID,
    ) -> tuple[list[UUID], list[dict], int]:
        """Walk the graph from `root_id` in BOTH directions (outgoing + incoming),
        returning (visited_node_ids, unresolved_edges, max_depth).

        Uses two recursive CTEs with cycle guards (`NOT IN (SELECT id FROM walk)`)
        so cyclic reference chains don't loop forever. Each direction is bounded
        by `_MAX_GRAPH_DEPTH` as an additional safety net.

        - visited_node_ids: every UUID reachable via resolved edges (FK non-null)
          from `root_id` in either direction. The root itself is included.
        - unresolved_edges: list of dicts with keys
          `source_circular_id`, `target_circular_number`, `relationship_type`
          for edges whose target FK is NULL (referenced circular not in our DB).
          `source_circular_id` is guaranteed to be in `visited_node_ids`.
        - max_depth: the maximum depth observed across both directions.
        """
        visited_ids: set[UUID] = {root_id}
        unresolved_edges: list[dict] = []
        max_depth = 0

        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()

            # ── Forward BFS (root → its targets → their targets → ...)
            # Cycle guard uses an inline `path` array column (NOT a subquery
            # against the recursive CTE itself, which Postgres forbids).
            cursor.execute(
                """
                WITH RECURSIVE fwd AS (
                    SELECT %s::uuid AS id,
                           0 AS depth,
                           ARRAY[%s]::uuid[] AS path
                  UNION ALL
                    SELECT cr.target_circular_id,
                           fwd.depth + 1,
                           fwd.path || cr.target_circular_id
                    FROM   fwd
                    JOIN   circular_references cr ON cr.source_circular_id = fwd.id
                    WHERE  fwd.depth < %s
                      AND  cr.target_circular_id IS NOT NULL
                      AND  NOT (cr.target_circular_id = ANY(fwd.path))
                )
                SELECT id, MIN(depth) FROM fwd GROUP BY id
                """,
                (str(root_id), str(root_id), self._MAX_GRAPH_DEPTH),
            )
            for row_id, depth in cursor.fetchall():
                visited_ids.add(row_id)
                if depth > max_depth:
                    max_depth = depth

            # ── Backward BFS (root → its sources → their sources → ...)
            cursor.execute(
                """
                WITH RECURSIVE bwd AS (
                    SELECT %s::uuid AS id,
                           0 AS depth,
                           ARRAY[%s]::uuid[] AS path
                  UNION ALL
                    SELECT cr.source_circular_id,
                           bwd.depth + 1,
                           bwd.path || cr.source_circular_id
                    FROM   bwd
                    JOIN   circular_references cr ON cr.target_circular_id = bwd.id
                    WHERE  bwd.depth < %s
                      AND  NOT (cr.source_circular_id = ANY(bwd.path))
                )
                SELECT id, MIN(depth) FROM bwd GROUP BY id
                """,
                (str(root_id), str(root_id), self._MAX_GRAPH_DEPTH),
            )
            for row_id, depth in cursor.fetchall():
                visited_ids.add(row_id)
                if depth > max_depth:
                    max_depth = depth

            # ── Unresolved edges (target FK is NULL) from any visited source
            if visited_ids:
                visited_strs = [str(uid) for uid in visited_ids]
                cursor.execute(
                    """
                    SELECT source_circular_id, target_circular_number, relationship_type
                    FROM   circular_references
                    WHERE  source_circular_id = ANY(%s::uuid[])
                      AND  target_circular_id IS NULL
                    """,
                    (visited_strs,),
                )
                for src_id, target_num, rel_type in cursor.fetchall():
                    unresolved_edges.append({
                        "source_circular_id": src_id,
                        "target_circular_number": target_num,
                        "relationship_type": rel_type,
                    })

        return list(visited_ids), unresolved_edges, max_depth

    def get_edges_for_visited(
        self, visited_ids: list[UUID],
    ) -> list[dict]:
        """Return all resolved edges where BOTH endpoints are in `visited_ids`.

        Each dict has keys: `source_circular_id`, `target_circular_id`,
        `relationship_type`. Unresolved edges (target FK NULL) are NOT included
        here — those come from `get_neighborhood()`'s unresolved_edges list.
        """
        if not visited_ids:
            return []
        visited_strs = [str(uid) for uid in visited_ids]
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT source_circular_id, target_circular_id, relationship_type
                FROM   circular_references
                WHERE  source_circular_id = ANY(%s::uuid[])
                  AND  target_circular_id = ANY(%s::uuid[])
                """,
                (visited_strs, visited_strs),
            )
            edges = [
                {
                    "source_circular_id": src,
                    "target_circular_id": tgt,
                    "relationship_type": rel,
                }
                for src, tgt, rel in cursor.fetchall()
            ]
        return edges
