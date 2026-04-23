import re

def render_sql(sql, params):
    try:
        return sql % tuple(repr(p) for p in params)
    except Exception:
        return f"{sql} | params={params}"

def multi_snippet(paragraph, query, window_size=120, merge_gap=80, max_snippets=3):
    # Step 0: normalize text
    text = re.sub(r'\s+', ' ', paragraph).strip()
    lower = text.lower()
    keywords = query.lower().split()

    ranges = []
    for word in keywords:
        start = 0
        while True:
            idx = lower.find(word, start)
            if idx == -1:
                break

            # Step 2: create window around match
            left = max(0, idx - window_size // 2)
            right = min(len(text), idx + window_size // 2)

            ranges.append([left, right])
            start = idx + len(word)

    if not ranges:
        return []

    # Step 3: sort windows (important for merging)
    ranges.sort(key=lambda x: x[0])

    # Step 4: merge intervals (CORE DSA)
    merged = [ranges[0]]

    for curr in ranges[1:]:
        last = merged[-1]

        # merge if overlapping OR close enough
        if curr[0] <= last[1] + merge_gap:
            last[1] = max(last[1], curr[1])
        else:
            merged.append(curr)

    # limit number of snippets
    merged = merged[:max_snippets]

    # extract clean snippets
    result = []
    for start, end in merged:
        snippet = text[start:end]

        # remove broken words at edges
        snippet = re.sub(r'^\S*\s', '', snippet)
        snippet = re.sub(r'\s\S*$', '', snippet)

        result.append("..." + snippet.strip() + "...")

    return result




def highlight(snippet: str, query: str) -> str:
    words = query.split()
    
    # build regex: (sebi|kyc)
    pattern = re.compile(
        r'\b(' + '|'.join(map(re.escape, words)) + r')\b',
        re.IGNORECASE
    )
    
    return pattern.sub(r'<mark>\1</mark>', snippet)