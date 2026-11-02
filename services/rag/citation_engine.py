import re

from llama_index.core.base.base_retriever import BaseRetriever
from llama_index.core.node_parser.interface import TextSplitter
from llama_index.core.prompts import PromptTemplate
from llama_index.core.query_engine import CitationQueryEngine
from llama_index.core.response_synthesizers import get_response_synthesizer
from llama_index.core.response_synthesizers.type import ResponseMode
from llama_index.core.schema import MetadataMode, NodeWithScore, QueryBundle, TextNode

from config import Config
from ingestion.indexer.dto import SearchHit
from services.rag.dto import Citation
from utils.llm_providers import get_llm_provider


# CitationQueryEngine re-splits every node into "granular sources" before it
# synthesises. ES already chunked at ingest time, so splitting again would shatter
# quotes mid-sentence and break the 1:1 chunk -> citation mapping.
class PassthroughTextSplitter(TextSplitter):
    def split_text(self, text: str) -> list[str]:
        return [text] if text.strip() else []


class PreRetrievedNodesRetriever(BaseRetriever):
    def __init__(self, nodes: list[NodeWithScore]) -> None:
        super().__init__()
        self._nodes = nodes

    def _retrieve(self, query_bundle: QueryBundle) -> list[NodeWithScore]:
        return self._nodes


_SOURCE_PREFIX_RE = re.compile(r"^Source (\d+):\n")

# Full metadata rides along for the API response, but the synthesizer renders
# node metadata into the prompt (MetadataMode.LLM). URLs and chunk ids are
# noise there, so drop them before they burn tokens.
_EXCLUDED_LLM_METADATA_KEYS = (
    "circular_db_id",
    "full_reference",
    "url",
    "pdf_url",
    "chunk_id",
    "chunk_index",
    "department",
)


def hits_to_nodes(hits: list[SearchHit]) -> list[NodeWithScore]:
    nodes = []
    for hit in hits:
        doc = hit.document
        if not doc.chunk_text.strip():
            continue
        nodes.append(
            NodeWithScore(
                node=TextNode(
                    text=doc.chunk_text,
                    metadata={
                        "circular_id": doc.circular_id,
                        "circular_db_id": doc.circular_db_id,
                        "full_reference": doc.full_reference,
                        "title": doc.title,
                        "source": doc.source,
                        "url": doc.url,
                        "pdf_url": doc.pdf_url,
                        "chunk_id": doc.chunk_id,
                        "chunk_index": doc.chunk_index,
                        "department": doc.department,
                        "issue_date": doc.issue_date.isoformat(),
                    },
                    excluded_llm_metadata_keys=list(_EXCLUDED_LLM_METADATA_KEYS),
                ),
                score=hit.score,
            )
        )
    return nodes


def citation_nodes_to_citations(nodes: list[NodeWithScore]) -> list[Citation]:
    citations = []
    for position, node in enumerate(nodes, start=1):
        # _create_citation_nodes rewrote the content to "Source N:\n{chunk}\n";
        # the prefix is the engine's own numbering, so it wins over order.
        content = node.node.get_content(metadata_mode=MetadataMode.NONE)
        match = _SOURCE_PREFIX_RE.match(content)
        text = _SOURCE_PREFIX_RE.sub("", content, count=1)
        if text.endswith("\n"):
            text = text[:-1]

        meta = node.node.metadata
        citations.append(
            Citation(
                id=int(match.group(1)) if match else position,
                circular_id=meta.get("circular_id") or "",
                circular_db_id=meta.get("circular_db_id"),
                full_reference=meta.get("full_reference"),
                title=meta.get("title") or "",
                source=meta.get("source"),
                url=meta.get("url"),
                chunk_id=meta.get("chunk_id"),
                chunk_index=meta.get("chunk_index"),
                text=text,
                score=node.score,
            )
        )
    return citations


# The 7B RAG model drifts from the prompt's "[n]" citation format and emits
# styles like "(source 1, source 2)", "Source 1", "[1, 2]". The frontend chip
# regex only renders "[n]", so rewrite those styles post-hoc. Bare "(1)" is
# deliberately not converted — it is usually a numbered list, and a missing
# chip is safer than a wrong one. Idempotent: canonical "[1][2]" never matches.
_REF_NOUN = (
    r"(?:clause|sub-paragraph|paragraph|para|sub-section|section|"
    r"sub-regulation|regulation|item|point|article|rule|schedule|part|chapter)"
)
_NUM = r"\d{1,2}"
_NUM_LIST = rf"{_NUM}(?:\s*(?:,|;|and|&)\s*(?:sources?\s+)?{_NUM})*"
_SRC_KEYWORD = r"sources?(?:\s+nos?\.?)?\s*:?\s*"

_CITATION_REF_RE = re.compile(
    rf"""
      (?P<protect>{_REF_NOUN}\s*\(\s*{_NUM}\s*\))
    | \[\s*{_NUM}(?:\s*,\s*{_NUM})+\s*\]
    | \(\s*{_SRC_KEYWORD}{_NUM_LIST}\s*\)
    | (?<![\w(]){_SRC_KEYWORD}{_NUM_LIST}(?!\s*[-–—]\s*\d)
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _expand_ref(m: re.Match) -> str:
    if m.group("protect"):
        return m.group(0)
    nums = list(dict.fromkeys(int(n) for n in re.findall(r"\d+", m.group(0))))
    return "".join(f"[{n}]" for n in nums)


def normalize_citation_markers(text: str) -> str:
    return _CITATION_REF_RE.sub(_expand_ref, text)


_CITATION_RULES = """Please provide an answer based solely on the provided sources. When referencing information from a source, cite the appropriate source(s) using their corresponding numbers in square brackets, like [1] or [2][3]. Every answer should include at least one source citation. Only cite a source when you are explicitly referencing it. If none of the sources are helpful, you should indicate that. For example:

Source 1:
The sky is red in the evening and blue in the morning.
Source 2:
Water is wet when the sky is red.
Query: When is water wet?
Answer: Water will be wet when the sky is red [2], which occurs in the evening [1].

You are a regulatory compliance assistant.

STRICT RULES:
1. Do not use outside knowledge.
2. Do not invent or assume facts.
3. Do not add information that is not present in the provided sources.
4. If the answer is not present in the sources, say:
   "The provided circular excerpts do not contain this information."
5. If only part of the answer is available, answer that part and clearly state what information is missing.
6. Preserve the exact meaning of the circulars. Do not change, broaden, or reinterpret requirements.
7. If circulars contain conflicting information, mention the conflict and cite the relevant sources.
8. Cite only sources that actually support the claim you are making.
9. Write in clear, professional English.
10. Do not make the answer unnecessarily verbose.
11. Prefer using points, bullets or numbered lists for clarity. Prefer using bold, Italics etc.

IMPORTANT:
The numbered sources below are the ONLY source of truth.
Anything not explicitly supported by them must be treated as unknown.
Use each source number at most once per sentence."""

CITATION_QA_TEMPLATE = PromptTemplate(
    _CITATION_RULES
    + """

Now it's your turn. Below are several numbered sources of information:
------
{context_str}
------
Query: {query_str}
Answer: """
)

CITATION_REFINE_TEMPLATE = PromptTemplate(
    _CITATION_RULES
    + """

Now it's your turn. We have provided an existing answer: {existing_answer}

Below are several numbered sources of information. Use them to refine the existing answer. If the provided sources are not helpful, you will repeat the existing answer.

Begin refining!
------
{context_msg}
------
Query: {query_str}
Answer: """
)


def build_citation_query_engine(nodes: list[NodeWithScore]) -> CitationQueryEngine:
    llm = get_llm_provider(Config.LLM_PROVIDER).get_llama_llm(
        Config.RAG_MODEL, max_tokens=Config.RAG_MAX_TOKENS
    )
    # CitationQueryEngine.__init__ takes no template args (from_args requires
    # an index we don't have), so the synthesizer is where the regulatory
    # prompt gets injected.
    synthesizer = get_response_synthesizer(
        llm=llm,
        text_qa_template=CITATION_QA_TEMPLATE,
        refine_template=CITATION_REFINE_TEMPLATE,
        response_mode=ResponseMode.COMPACT,
        use_async=False,
        streaming=False,
    )
    return CitationQueryEngine(
        retriever=PreRetrievedNodesRetriever(nodes),
        # Unused for synthesis (the synthesizer already has the llm) but __init__
        # still evaluates `llm or Settings.llm`, which would fall back to OpenAI.
        llm=llm,
        response_synthesizer=synthesizer,
        text_splitter=PassthroughTextSplitter(),
        metadata_mode=MetadataMode.NONE,
    )
 
