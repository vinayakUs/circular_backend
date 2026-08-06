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

        DELETE + INSERT loop runs inside an explicit conn.transaction() block —
        if any INSERT fails, the entire operation (including the DELETE)
        rolls back (M9 defense-in-depth fix).
        """
        with self.db_pool.acquire() as conn:
            with conn.transaction():
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
        self.logger.info(
            "Replaced references source_circular_id=%s count=%s",
            source_circular_id, len(references),
        )
        return len(references)

    # ─────────────────────────────────────────────────────────────────────────
    # GRAPH TRAVERSAL
    # ─────────────────────────────────────────────────────────────────────────

    # Hard cap on BFS depth as a safety net against runaway recursion in dense graphs.
    # 5 = 5-hop neighbourhood (root + 4 outward). Tight enough to stay readable,
    # loose enough to capture the meaningful dependency context.
    _MAX_GRAPH_DEPTH = 5

    def get_neighborhood(
        self, root_id: UUID,
    ) -> tuple[list[UUID], list[dict], int, int]:
        """Walk the graph from `root_id` to its CLOSED neighborhood
        (outgoing + incoming edges, alternating until closure), returning
        (visited_node_ids, unresolved_edges, max_depth, cycles_truncated).

        Uses a single recursive CTE that fans out both directions in each
        step, so the visited set is invariant under choice of root within
        a connected component. The cycle guard is an inline `path` array
        column (Postgres forbids subqueries against the recursive CTE
        itself). Depth is bounded by `_MAX_GRAPH_DEPTH` as a safety net;
        if that cap fires while edges remain, cycles_truncated > 0.

        - visited_node_ids: every UUID reachable via resolved edges (FK
          non-null) from `root_id`. The root itself is included.
        - unresolved_edges: list of dicts with keys
          `source_circular_id`, `target_circular_number`, `relationship_type`
          for edges whose target FK is NULL (referenced circular not in our DB).
          `source_circular_id` is guaranteed to be in `visited_node_ids`.
        - max_depth: the maximum depth observed across the closure.
        - cycles_truncated: 1 if the depth cap truncated an in-progress
          expansion (i.e. a real cycle was broken off), else 0.
        """
        visited_ids: set[UUID] = {root_id}
        unresolved_edges: list[dict] = []
        max_depth = 0
        cycles_truncated = 0

        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()

            # ── Iterative bidirectional closure
            # Postgres recursive CTEs allow only ONE FROM-walk reference per
            # recursive arm, so we can't fold forward + backward expansion
            # into a single CTE. Instead we iterate in Python: each iteration
            # runs two single-direction CTEs (forward + backward) seeded by
            # the current frontier, unions the new nodes, and stops when
            # the frontier is empty (closure reached) or `_MAX_GRAPH_DEPTH`
            # is hit (cycle truncated).
            frontier: set[UUID] = {root_id}
            depth_cap_hit = False
            for _ in range(self._MAX_GRAPH_DEPTH):
                if not frontier:
                    break
                frontier_strs = [str(uid) for uid in frontier]
                # psycopg2's set adapter doesn't exist — always pass
                # already-visited as a list of strings for the ANY() guard.
                visited_strs = [str(uid) for uid in visited_ids]

                # Forward: each frontier node's outgoing resolved targets
                cursor.execute(
                    """
                    WITH RECURSIVE fwd AS (
                        SELECT unnest(%s::uuid[]) AS id,
                               0                 AS depth
                      UNION ALL
                        SELECT cr.target_circular_id, fwd.depth + 1
                        FROM   fwd
                        JOIN   circular_references cr ON cr.source_circular_id = fwd.id
                        WHERE  fwd.depth = 0
                          AND  cr.target_circular_id IS NOT NULL
                          AND  NOT (cr.target_circular_id = ANY(%s::uuid[]))
                    )
                    SELECT DISTINCT id FROM fwd WHERE depth > 0
                    """,
                    (frontier_strs, visited_strs),
                )
                fwd_new = {row[0] for row in cursor.fetchall()}

                # Backward: each frontier node's incoming sources
                cursor.execute(
                    """
                    WITH RECURSIVE bwd AS (
                        SELECT unnest(%s::uuid[]) AS id,
                               0                 AS depth
                      UNION ALL
                        SELECT cr.source_circular_id, bwd.depth + 1
                        FROM   bwd
                        JOIN   circular_references cr ON cr.target_circular_id = bwd.id
                        WHERE  bwd.depth = 0
                          AND  NOT (cr.source_circular_id = ANY(%s::uuid[]))
                    )
                    SELECT DISTINCT id FROM bwd WHERE depth > 0
                    """,
                    (frontier_strs, visited_strs),
                )
                bwd_new = {row[0] for row in cursor.fetchall()}

                new_nodes = (fwd_new | bwd_new) - visited_ids
                if not new_nodes:
                    break
                visited_ids |= new_nodes
                frontier = new_nodes
                max_depth += 1
            else:
                # Loop exhausted without `break` — depth cap was reached
                # while the frontier still had work, indicating a real cycle
                # was broken off.
                if frontier:
                    depth_cap_hit = True
            cycles_truncated = 1 if depth_cap_hit else 0

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

        return list(visited_ids), unresolved_edges, max_depth, cycles_truncated

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
