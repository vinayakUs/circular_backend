"""Evidence used by the agent citation verifier."""
import re


def collect_evidence(evidence, tool_name, payload):
    if (payload.get("quality") or {}).get("weak"):
        return
    for row in payload.get("results", []):
        cid = row.get("circular_id")
        if not cid:
            continue
        fields = ("circular_no", "source", "issue_date", "title", "status", "department")
        record = {key: row[key] for key in fields if row.get(key)}
        if tool_name == "search_chunks" and row.get("text"):
            record.update({key: row[key] for key in ("text", "page", "chunk_index") if key in row})
        if tool_name == "find_phrase":
            record["body_snippets"] = [s["text"] for s in row.get("snippets", []) if s.get("text")]
        if record and record not in evidence.get(cid, []):
            evidence[cid] = [*evidence.get(cid, []), record]


def grounded_lines(answer, evidence, check_support):
    """Validate each complete line against only its cited records. Fail closed."""
    kept, removed = [], False
    for line in answer.splitlines():
        if not line.strip():
            continue
        ids = re.findall(r"\[([^\[\]]+)\]", line)
        if ids and all(cid in evidence for cid in ids):
            try:
                if check_support(line, {cid: evidence[cid] for cid in ids}):
                    kept.append(line)
                    continue
            except Exception:
                # A verifier outage must not publish an unchecked claim.
                pass
        removed = True
    return "\n".join(kept), removed
