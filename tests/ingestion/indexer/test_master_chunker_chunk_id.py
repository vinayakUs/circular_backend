import hashlib

from ingestion.indexer.master_chunker import MasterCircularChunkingStrategy
from ingestion.indexer.pdf_extractor import Block

LONG_PARAGRAPH = (
    "- Changes in due date/expiry date may be required to be done in the running "
    "contracts in the event of sudden closure of markets on expiry date. Point "
    "6.1.6 (I) of this circular prescribes that Exchange may advance expiry date "
    "of running contract in case physical market is closed in the notified basis "
    "centre on the expiry day of the contract, due to festivals, strikes, erratic "
    "weather conditions, etc. Decision about advancing expiry of running contract "
    "shall be intimated to the trade participants at least 10 days before the "
    "revised expiry date."
)


def _make_chunker(monkeypatch) -> MasterCircularChunkingStrategy:
    # Keep the test hermetic: no LLM abbreviation extraction, no ML sub-splitting.
    monkeypatch.setattr(
        MasterCircularChunkingStrategy,
        "_extract_abbreviations",
        lambda self, blocks: {},
    )
    monkeypatch.setattr(
        MasterCircularChunkingStrategy,
        "_semantic_sub_chunking",
        lambda self, text, **kwargs: [text],
    )
    return MasterCircularChunkingStrategy(embedding_provider=None)


def test_long_heading_node_id_stays_under_es_limit(monkeypatch):
    chunker = _make_chunker(monkeypatch)
    blocks = [Block("text", f"# {LONG_PARAGRAPH}", 0, 0.0)]

    chunks = chunker.chunk(blocks, circular_key="k")

    assert chunks
    for chunk in chunks:
        assert len(chunk.chunk_id.encode("utf-8")) <= 512
        assert LONG_PARAGRAPH not in chunk.chunk_id


def test_short_section_ids_keep_readable_format(monkeypatch):
    chunker = _make_chunker(monkeypatch)
    blocks = [Block("text", "7.1. Reporting obligations\nBanks must report daily.", 0, 0.0)]

    chunks = chunker.chunk(blocks, circular_key="k")

    assert chunks
    payload_hash = hashlib.sha1(chunks[0].text.encode("utf-8")).hexdigest()[:12]
    assert chunks[0].chunk_id == f"k_7.1_0_{payload_hash}"


def test_chunk_ids_are_deterministic(monkeypatch):
    chunker = _make_chunker(monkeypatch)
    blocks = [Block("text", f"# {LONG_PARAGRAPH}\n\n7.1. Reporting\nBody text here.", 0, 0.0)]

    first = [c.chunk_id for c in chunker.chunk(blocks, circular_key="k")]
    second = [c.chunk_id for c in chunker.chunk(blocks, circular_key="k")]

    assert first == second
