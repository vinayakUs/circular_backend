"""End-to-end integration test against the live Neo4j instance.

Validates the full path: Section + Change extraction → GraphLoader
Cypher writes → status mutation → retrieval-time expansion query.

To keep the test independent of LLM API availability, the Section /
Change payloads here are pre-built to match what the LLM extraction
prompts in ``ingestion/graph/prompts.py`` are expected to produce for
the two NSE sample circulars. The prompts themselves are validated
separately by ``tests/test_graph_module.py``.

This is the proof that the v1 graph schema resolves the
"third-party-product / digital-gold / stand deleted" bug:

  - Circular 1 (NSE/COMP/50957, Jan 2022) lists Point 12 prohibiting
    digital gold / unregulated third-party products.
  - Circular 2 (NSE/INSP/74836, June 2026) modifies Points 1-2 and
    deletes Points 3-12.
  - After loading both, ``Section(NSE/COMP/50957, "12").status`` must
    equal ``deleted``, with a CHANGED_BY edge pointing at Circular 2
    carrying the verbatim legal basis.

Run:
    cd /root/circular_backend && python scripts/test_graph_integration.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ingestion.graph import (
    GraphLoader,
    Neo4jClient,
    Section,
    SectionList,
    ChangeRelation,
    ChangeList,
    fetch_amendments_for_sections,
)


# ---------------------------------------------------------------------
# Synthetic extraction payloads (mirror what the LLM prompts produce)
# ---------------------------------------------------------------------


def build_circular_1() -> SectionList:
    return SectionList(sections=[
        Section(label="1", unit_type="point", text=(
            "Issuing Corporate Guarantees towards credit facilities availed "
            "by any entity, including group companies, where such credit "
            "facilities are not in connection with the member's own "
            "business activities."
        )),
        Section(label="2", unit_type="point", text=(
            "Deposit pledged with the bank for the purpose of availing "
            "credit facilities by group companies."
        )),
        Section(label="3", unit_type="point", text=(
            "Borrowing of funds for the purpose of onward lending to "
            "clients or group companies."
        )),
        Section(label="4", unit_type="point", text=(
            "Lending of funds to clients or group companies against "
            "shares, debentures, or other securities."
        )),
        Section(label="5", unit_type="point", text=(
            "Underwriting or sub-underwriting activities beyond the "
            "limits prescribed by SEBI."
        )),
        Section(label="6", unit_type="point", text=(
            "Acting as a sponsor or trustee for any collective "
            "investment scheme."
        )),
        Section(label="7", unit_type="point", text=(
            "Providing margin trading facilities beyond the limits "
            "specified by the Exchange."
        )),
        Section(label="8", unit_type="point", text=(
            "Offering assured returns on any investment product."
        )),
        Section(label="9", unit_type="point", text=(
            "Acting as a portfolio manager without obtaining separate "
            "registration from SEBI."
        )),
        Section(label="10", unit_type="point", text=(
            "Distribution of unlisted securities to retail clients."
        )),
        Section(label="11", unit_type="point", text=(
            "Acting as a referral agent for any financial product "
            "without a proper agreement."
        )),
        Section(label="12", unit_type="point", text=(
            "Entering into any arrangement/scheme, including but not "
            "limited to digital gold, e-gold, sovereign gold bonds, "
            "or any other arrangement/scheme, for facilitating the "
            "clients to transact in third-party products which are "
            "not regulated by an appropriate financial sector regulator."
        )),
    ])


def build_circular_2_sections() -> SectionList:
    return SectionList(sections=[
        Section(label="1", unit_type="point", text=(
            "Point 1 (modified): Issuing Corporate Guarantees towards "
            "credit facilities availed by any entity, including group "
            "companies."
        )),
        Section(label="2", unit_type="point", text=(
            "Point 2 (modified): Deposit pledged with the bank for "
            "the purpose of availing credit facilities by group companies."
        )),
        Section(label="Question No.1", unit_type="question", text=(
            "Whether members can offer third-party products to their "
            "clients."
        )),
        Section(label="Answer No.1", unit_type="answer", text=(
            "Members can offer third-party products to their clients "
            "only if such products are regulated by an appropriate "
            "financial sector regulator."
        )),
        Section(label="Question No.2", unit_type="question", text=(
            "Whether prior approval from the Exchange is required for "
            "offering a new third-party product."
        )),
        Section(label="Answer No.2", unit_type="answer", text=(
            "Members are required to intimate the Exchange before "
            "launching any new third-party product."
        )),
        Section(label="Question No.3", unit_type="question", text=(
            "Whether digital gold products can be distributed."
        )),
        Section(label="Answer No.3", unit_type="answer", text=(
            "Members may distribute digital gold products only if the "
            "underlying product is regulated by an appropriate financial "
            "sector regulator."
        )),
        Section(label="Question No.4", unit_type="question", text=(
            "Whether members can enter into revenue sharing arrangements."
        )),
        Section(label="Answer No.4", unit_type="answer", text=(
            "Members may enter into revenue sharing arrangements "
            "subject to the limits and conditions specified by SEBI."
        )),
        Section(label="Question No.5", unit_type="question", text=(
            "Whether members can act as a sponsor or trustee for any "
            "collective investment scheme."
        )),
        Section(label="Answer No.5", unit_type="answer", text=(
            "No. Members cannot act as a sponsor or trustee for any "
            "collective investment scheme without obtaining separate "
            "registration from SEBI."
        )),
        Section(label="Question No.6", unit_type="question", text=(
            "Whether members can offer unregulated third-party products."
        )),
        Section(label="Answer No.6", unit_type="answer", text=(
            "No. Members cannot offer third-party products that are not "
            "regulated by an appropriate financial sector regulator. "
            "Refer Exchange Circular NSE/INSP/68566 dated December 12, "
            "2024 for the current framework."
        )),
    ])


def build_circular_2_changes() -> ChangeList:
    """Mirrors the prompt's range-expansion output for
    'Point No. 1 and 2 ... modified' + 'Point No. 3 to 12 stand deleted'."""
    changes: list[ChangeRelation] = []

    # Points 1 and 2 — modified
    modified_sentence = (
        "Point No. 1 and 2 of illustrative list of activities issued vide "
        "Exchange Circular Ref No. NSE/COMP/50957 dated January 07, 2022 "
        "are modified as below."
    )
    for label, new_text in [
        ("1", "Issuing Corporate Guarantees towards credit facilities "
              "availed by any entity, including group companies."),
        ("2", "Deposit pledged with the bank for the purpose of availing "
              "credit facilities by group companies."),
    ]:
        changes.append(ChangeRelation(
            target_circular_id="NSE/COMP/50957",
            target_label=label,
            change_type="modified",
            new_text=new_text,
            effective_date="2026-06-23",
            raw_sentence=modified_sentence,
        ))

    # Points 3 to 12 — deleted (range expansion → 10 separate relations)
    deletion_sentence = (
        "Point No. 3 to 12 of illustrative list of activities as "
        "prescribed in Exchange Circular Ref No. NSE/COMP/50957 dated "
        "January 07, 2022, stand deleted."
    )
    for label in [str(n) for n in range(3, 13)]:
        changes.append(ChangeRelation(
            target_circular_id="NSE/COMP/50957",
            target_label=label,
            change_type="deleted",
            new_text=None,
            effective_date="2026-06-23",
            raw_sentence=deletion_sentence,
        ))

    return ChangeList(changes=changes)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def section_id(circular_id: str, label: str) -> str:
    return f"{circular_id}::{label}"


def fetch_section_status(
    client: Neo4jClient, sec_id: str,
) -> tuple[str | None, str | None]:
    with client.driver.session(database=client.database) as session:
        row = session.run(
            "MATCH (s:Section {section_id: $sid}) "
            "RETURN s.status AS status, s.superseded_text AS superseded",
            sid=sec_id,
        ).single()
    if row is None:
        return (None, None)
    return (row["status"], row["superseded"])


def fetch_section_count(client: Neo4jClient, circular_id: str) -> int:
    with client.driver.session(database=client.database) as session:
        return session.run(
            "MATCH (s:Section {circular_id: $cid}) RETURN count(s) AS n",
            cid=circular_id,
        ).single()["n"]


def fetch_edge_count(client: Neo4jClient, circular_id: str) -> int:
    with client.driver.session(database=client.database) as session:
        return session.run(
            "MATCH (:Section)-[:CHANGED_BY]->(c:Circular {circular_id: $cid}) "
            "RETURN count(*) AS n",
            cid=circular_id,
        ).single()["n"]


def expect(label: str, condition: bool, detail: str = "") -> None:
    mark = "✓" if condition else "✗"
    msg = f"  [{mark}] {label}"
    if detail:
        msg += f"  ({detail})"
    print(msg)
    if not condition:
        raise AssertionError(f"failed: {label}")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------


def main() -> None:
    client = Neo4jClient()
    client.verify_connectivity()
    print(f"[connect] {client.uri}")

    # Clean slate
    with client.driver.session(database=client.database) as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("[reset] graph cleared")

    client.setup_constraints_and_indexes()
    print("[schema] constraints + indexes installed")

    loader = GraphLoader(client)

    # ---- Load Circular 1 (the older circular, no CHANGED_BY edges yet) ----
    print("\n[load] Circular 1 — NSE/COMP/50957 (Jan 2022)")
    sections_1 = build_circular_1()
    written_1, total_1 = loader.upsert_circular_with_sections(
        circular_id="NSE/COMP/50957",
        title="Illustrative list of activities not permitted under third party product distribution",
        issue_date="2022-01-07",
        source="NSE",
        full_reference="NSE/COMP/50957",
        sections=sections_1,
    )
    print(f"  sections_written={written_1}/{total_1}")

    # ---- Load Circular 2 (the amendment circular) ----
    print("\n[load] Circular 2 — NSE/INSP/74836 (June 2026)")
    sections_2 = build_circular_2_sections()
    written_2, total_2 = loader.upsert_circular_with_sections(
        circular_id="NSE/INSP/74836",
        title="Modification and deletion of certain provisions of NSE/COMP/50957",
        issue_date="2026-06-23",
        source="NSE",
        full_reference="NSE/INSP/74836",
        sections=sections_2,
    )
    print(f"  sections_written={written_2}/{total_2}")

    # ---- Apply CHANGED_BY edges from Circular 2 to Circular 1 ----
    changes_2 = build_circular_2_changes()
    edges, targets = loader.upsert_changes(
        source_circular_id="NSE/INSP/74836",
        changes=changes_2,
    )
    print(f"  edges_written={edges} targets_resolved={targets}")

    # ---- Assertions ----
    print("\n[verify] schema state")
    expect(
        "Circular 1 has 12 sections",
        fetch_section_count(client, "NSE/COMP/50957") == 12,
        f"actual={fetch_section_count(client, 'NSE/COMP/50957')}",
    )
    expect(
        "Circular 2 has 14 sections (2 points + 6 Q&A pairs)",
        fetch_section_count(client, "NSE/INSP/74836") == 14,
        f"actual={fetch_section_count(client, 'NSE/INSP/74836')}",
    )
    expect(
        "Circular 2 has 12 CHANGED_BY edges (2 modified + 10 deleted)",
        fetch_edge_count(client, "NSE/INSP/74836") == 12,
        f"actual={fetch_edge_count(client, 'NSE/INSP/74836')}",
    )

    # Status flips on Circular 1
    s1, s1_text = fetch_section_status(
        client, section_id("NSE/COMP/50957", "1")
    )
    s2, _ = fetch_section_status(client, section_id("NSE/COMP/50957", "2"))
    s12, _ = fetch_section_status(client, section_id("NSE/COMP/50957", "12"))
    expect("Point 1 of Circular 1 is modified", s1 == "modified", f"status={s1}")
    expect("Point 1 has superseded_text", bool(s1_text), f"superseded={s1_text[:60] if s1_text else None}...")
    expect("Point 2 of Circular 1 is modified", s2 == "modified", f"status={s2}")
    expect("Point 12 of Circular 1 is deleted", s12 == "deleted", f"status={s12}")

    # ---- THE bug case: query for Point 12 amendments ----
    print("\n[query] fetch_amendments_for_sections(['NSE/COMP/50957::12'])")
    rows = fetch_amendments_for_sections(
        client, [section_id("NSE/COMP/50957", "12")]
    )
    if not rows:
        raise AssertionError("no rows returned — graph not populated")
    for row in rows:
        print(f"    section_id:       {row.get('section_id')}")
        print(f"    current_status:   {row.get('current_status')}")
        print(f"    change_type:      {row.get('change_type')}")
        print(f"    changer_id:       {row.get('changer_id')}")
        print(f"    changer_issue:    {row.get('changer_issue_date')}")
        print(f"    effective_date:   {row.get('effective_date')}")
        print(f"    raw_sentence:     {row.get('raw_sentence')}")
        print()

    expect(
        "Point 12 has a CHANGED_BY edge with change_type=deleted",
        any(r["change_type"] == "deleted" for r in rows),
    )
    expect(
        "Changer circular is NSE/INSP/74836",
        any(r["changer_id"] == "NSE/INSP/74836" for r in rows),
    )
    expect(
        "Raw sentence mentions 'stand deleted'",
        any("stand deleted" in (r["raw_sentence"] or "") for r in rows),
    )

    # ---- Annexure FAQ check ----
    print("\n[query] Annexure-A Answer No.6 of Circular 2")
    a6_id = section_id("NSE/INSP/74836", "Answer No.6")
    with client.driver.session(database=client.database) as session:
        a6_row = session.run(
            "MATCH (s:Section {section_id: $sid}) "
            "RETURN s.unit_type AS unit_type, s.text AS text",
            sid=a6_id,
        ).single()
    expect("Annexure-A A.6 is extracted", a6_row is not None)
    if a6_row:
        expect("Annexure-A A.6 has unit_type='answer'", a6_row["unit_type"] == "answer")
        expect(
            "Annexure-A A.6 mentions NSE/INSP/68566 (current framework)",
            "NSE/INSP/68566" in a6_row["text"],
        )
        print(f"    text: {a6_row['text'][:200]}...")

    # ---- Final summary ----
    print("\n" + "=" * 64)
    print("RESULT: v1 graph schema resolves the third-party-product bug.")
    print("  • Point 12 of NSE/COMP/50957 is correctly flagged 'deleted'")
    print("  • CHANGED_BY edge points at NSE/INSP/74836 with legal basis")
    print("  • Retrieval-time expansion via fetch_amendments_for_sections")
    print("    returns the deletion context before the LLM generates text.")
    print("=" * 64)

    client.close()


if __name__ == "__main__":
    main()
