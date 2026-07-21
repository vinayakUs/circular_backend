"""
Reference Graph Service
======================

Builds the complete reachable reference graph for a circular.

Returns a flat `nodes + links` shape suitable for graph visualization
libraries (d3, react-flow, vis.js). Walks both outgoing and incoming
edges from the requested circular — no depth cap, cycle-safe.

Returned shape (dict):
    {
      "root":    {...}                          # the requested circular
      "nodes":   [{id, label, title, exchange, department, issue_date, depth, is_root, unresolved?}]
      "links":   [{source, target, target_label?, label}]    # label = relationship_type
      "stats":   {total_nodes, total_links, resolved_nodes, unresolved_nodes, max_depth, cycles_truncated}
    }

Returns None when `root_id` does not exist.
"""
from __future__ import annotations

from datetime import date
from typing import Any
from uuid import UUID

from ingestion.repository.circular_reference_repository import CircularReferenceRepository
from ingestion.repository.circular_repository import CircularRepository


class ReferenceGraphService:

    def __init__(self, db_pool: Any) -> None:
        self.circular_repo = CircularRepository(db_pool=db_pool)
        self.reference_repo = CircularReferenceRepository(db_pool=db_pool)
        self.logger = __import__("logging").getLogger(__name__)

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────────────────────────────────────

    def get_reference_graph(self, root_id: UUID) -> dict | None:
        """Build the full reachable graph for `root_id`. Returns None if missing."""
        # 1. Verify root exists
        root = self.circular_repo.get_record_by_id(root_id)
        if root is None:
            return None

        # 2. BFS in both directions (cycle-safe)
        visited_ids, unresolved_edges, max_depth = \
            self.reference_repo.get_neighborhood(root_id)

        # 3. Batch-fetch metadata for all visited resolved nodes
        records = self.circular_repo.get_records_by_ids(visited_ids)
        by_id: dict[UUID, Any] = {r.id: r for r in records}

        # 4. Compute BFS depth per node (smallest of any direction)
        depth_map = self._build_depth_map(root_id, visited_ids)
        root_id_str = str(root_id)

        # 5. Build node list (resolved + unresolved stubs)
        nodes: list[dict] = []
        for record in records:
            nodes.append({
                "id": str(record.id),
                "label": record.full_reference or record.circular_id,
                "title": record.title,
                "exchange": record.source,
                "department": record.department,
                "issue_date": record.issue_date.isoformat() if record.issue_date else None,
                "depth": depth_map.get(record.id, 0),
                # Compare strings to be type-safe across UUID/str edge cases.
                "is_root": str(record.id) == root_id_str,
                "unresolved": False,
            })

        for stub in unresolved_edges:
            nodes.append({
                "id": None,
                "label": stub["target_circular_number"],
                "title": None,
                "exchange": None,
                "department": None,
                "issue_date": None,
                "depth": self._infer_stub_depth(stub["source_circular_id"], depth_map),
                "is_root": False,
                "unresolved": True,
            })

        # 6. Build link list (resolved + unresolved)
        resolved_edges = self.reference_repo.get_edges_for_visited(visited_ids)
        links: list[dict] = [
            {
                "source": str(e["source_circular_id"]),
                "target": str(e["target_circular_id"]),
                "label": e["relationship_type"],
            }
            for e in resolved_edges
        ]
        for ue in unresolved_edges:
            links.append({
                "source": str(ue["source_circular_id"]),
                "target": None,
                "target_label": ue["target_circular_number"],
                "label": ue["relationship_type"],
            })

        # 7. Assemble
        # max_depth covers the full output — both resolved and stub nodes —
        # so the UI can render the graph extent correctly even when the
        # recursive CTE saw no resolved neighbors (e.g. all edges unresolved).
        observed_max_depth = max(
            [max_depth]
            + [n["depth"] for n in nodes],
            default=0,
        )
        return {
            "root": self._serialize_root(root),
            "nodes": nodes,
            "links": links,
            "stats": {
                "total_nodes": len(nodes),
                "total_links": len(links),
                "resolved_nodes": len(records),
                "unresolved_nodes": len(unresolved_edges),
                "max_depth": observed_max_depth,
                "cycles_truncated": 0,
            },
        }

    # ─────────────────────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _build_depth_map(
        self, root_id: UUID, visited_ids: list[UUID],
    ) -> dict[UUID, int]:
        """Assign each visited UUID a BFS depth relative to root.

        Runs BFS in BOTH directions over the visited subgraph — forward
        (outgoing) and backward (incoming) — and takes the minimum depth
        for each node. This way, nodes reachable only via incoming edges
        (sources pointing at root) get a sensible positive depth instead of
        being collapsed to 0.

        Acceptable because visited sets are bounded (typically tens to
        hundreds of nodes).
        """
        edges = self.reference_repo.get_edges_for_visited(visited_ids)

        # Forward adjacency: source → targets
        fwd_adj: dict[UUID, set[UUID]] = {}
        # Backward adjacency: target → sources
        bwd_adj: dict[UUID, set[UUID]] = {}
        for e in edges:
            fwd_adj.setdefault(e["source_circular_id"], set()).add(e["target_circular_id"])
            bwd_adj.setdefault(e["target_circular_id"], set()).add(e["source_circular_id"])

        def bfs(adj: dict[UUID, set[UUID]], start: UUID) -> dict[UUID, int]:
            depth = {start: 0}
            frontier = [start]
            while frontier:
                next_frontier: list[UUID] = []
                for node in frontier:
                    for neighbor in adj.get(node, ()):
                        if neighbor not in depth:
                            depth[neighbor] = depth[node] + 1
                            next_frontier.append(neighbor)
                frontier = next_frontier
            return depth

        fwd_depth = bfs(fwd_adj, root_id)
        bwd_depth = bfs(bwd_adj, root_id)

        depth: dict[UUID, int] = {}
        for vid in visited_ids:
            d = min(fwd_depth.get(vid, 10**9), bwd_depth.get(vid, 10**9))
            depth[vid] = d if d < 10**9 else 0
        return depth

    def _infer_stub_depth(
        self, source_id: UUID, depth_map: dict[UUID, int],
    ) -> int:
        """Depth of an unresolved stub = depth of its source + 1, clamped to 0."""
        return depth_map.get(source_id, 0) + 1

    def _serialize_root(self, record: Any) -> dict:
        return {
            "id": str(record.id),
            "circular_id": record.circular_id,
            "title": record.title,
            "source": record.source,
            "department": record.department,
            "issue_date": record.issue_date.isoformat() if isinstance(record.issue_date, date) else None,
        }
