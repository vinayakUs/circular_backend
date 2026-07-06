## Design Context

### Users
Mixed audience: compliance officers, legal teams, and back-office staff who need systematic regulatory tracking, alongside traders, brokers, and analysts who need quick access to market-moving circulars. Context ranges from deep-dive compliance research to rapid lookups during trading hours.

### Brand Personality
Professional and authoritative. The interface should convey trustworthiness and reliability—like a regulatory institution or professional service. Users need confidence that the information is accurate and complete.

### Aesthetic Direction
Minimal and refined. Clean, minimal, lots of white space, refined typography—inspired by products like Notion or Linear. The goal is to make regulatory information feel manageable and approachable, not overwhelming.

**Theme**: Light mode only. Clean, professional, easier to read long documents.

### Design Principles

1. **Clarity over density**: Information should be scannable. Use hierarchy, spacing, and typography to guide the eye through complex regulatory content.

2. **Authority through restraint**: A refined, understated aesthetic conveys professionalism. Avoid decorative elements that feel frivolous or "techy" in a way that undermines trust.

3. **Efficiency without clutter**: The mixed audience needs both quick lookups and detailed reading. Progressive disclosure and clear navigation patterns support both use cases.

4. **Typography-first approach**: Since this is a document-heavy application, typography is the primary design element. Choose refined, readable typefaces that work well for extended reading.

5. **Neutral palette with purposeful accents**: Use a restrained color palette—dominant neutrals with sharp, purposeful accents for actions and states. Avoid the "AI color palette" (cyan-on-dark, purple gradients).

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
