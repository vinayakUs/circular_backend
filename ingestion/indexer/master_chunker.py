"""Hierarchical structural chunker with semantic sub-chunking for SEBI Master Circulars.

This module is the SEBI master-circular specialization of the indexer's text
chunker. It builds a heading-aware tree from the document, then chunks each
node by sentence boundaries plus optional embedding-similarity topic shifts.

Designed to:
  * parse Markdown-style headings + SEBI numbering (Chapter, Annexure, 1.1, 1.1.1, etc.)
  * split long node text using spaCy sentences
  * use sentence-transformer cosine similarity (when available) to find topic-shift
    boundaries inside a paragraph
  * expand SEBI / financial abbreviations dynamically from the document text
  * fall back gracefully to regex sentence packing when models are missing
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass, field
from typing import List, Optional

from ingestion.indexer.dto import TextChunk
from ingestion.indexer.embedding_provider import EmbeddingProvider
from ingestion.indexer.pdf_extractor import Block

# Heavy ML dependencies are optional. The master-chunker code paths that use them
# degrade to regex/char-pack fallbacks when the imports aren't available.
try:
    import torch  # noqa: F401  (used by semantic_sub_chunking if installed)
except ImportError:
    torch = None  # type: ignore[assignment]

try:
    import spacy  # noqa: F401
except ImportError:
    spacy = None  # type: ignore[assignment]

try:
    from sentence_transformers import SentenceTransformer  # noqa: F401
except ImportError:
    SentenceTransformer = None  # type: ignore[assignment]

# Global cache for lazily loaded models
_nlp_model = None
_embedding_model = None

def get_embedding_model():
    """Lazily load the SentenceTransformer embedding model."""
    global _embedding_model
    if _embedding_model is None:
        from sentence_transformers import SentenceTransformer
        model_path = os.path.join(os.path.dirname(__file__), "..", "..", "models", "all-MiniLM-L6-v2")
        if os.path.exists(model_path):
            _embedding_model = SentenceTransformer(model_path)
        else:
            _embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    return _embedding_model


@dataclass
class Node: 
    id: str
    label: str
    level: int
    content: str = ""
    children: List["Node"] = field(default_factory=list)
    parent: Optional["Node"] = None

    def is_leaf(self): 
        return len(self.children) == 0

    def ancestor_context(self) -> str: 
        parts = []
        node = self.parent
        while node and node.level > 0: 
            chunk = f"[{node.id}] {node.label}"
            if node.content.strip(): 
                # clip content slice for context readability if its too long 
                ctx_body = node.content.strip()
                if len(ctx_body) > 60: 
                    ctx_body = ctx_body[:57] + "..."
                chunk += ": " + ctx_body
            parts.insert(0, chunk)
            node = node.parent
        return " | ".join(parts)
    
    def full_chunk(self) -> str: 
        ctx = self.ancestor_context()
        own = f"[{self.id}] {self.label}"
        if self.content.strip(): 
            own += ": " + self.content.strip()
        return (ctx + " | " + own) if ctx else own 

# General hierarchy patterns matching Markdown headers and common SEBI numbering systems
PATTERNS = [
    # Level 1: Chapters, Appendices, Annexures, Schedules, and major numbered parts (e.g. 10. Title)
    (1, re.compile(r"^#{1,2}\s+(?:CHAPTER\s+([0-9a-zA-Z]+)|(APPENDIX|ANNEXURE|SCHEDULE|EXHIBIT))\s*[:\-]?\s*(.*)$", re.I)), 
    (1, re.compile(r"^(\d+)\.\s+(.{5,})$")), 
    
    # Level 3: Sub-sections (e.g. 1.1.1 or double-digit typos like 11.1) - matched before Level 2 to avoid shadowing!
    (3, re.compile(r"^#{3,4}\s+(\d+\.\d+\.\d+)\.?\s+(.*)$")),
    (3, re.compile(r"^(\d+\.\d+\.\d+)\.\s+(.{5,})$")),
    (3, re.compile(r"^(\d+\d\.\d+)\.\s+(.{5,})$")), 
    
    # Level 2: Main sections (e.g. 1.1)
    (2, re.compile(r"^#{2,3}\s+(\d+\.\d+)\.?\s+(.*)$")),
    (2, re.compile(r"^(\d+\.\d+)\.\s+(.{5,})$")), 
    
    # Level 5: Roman numeral list items (e.g. (i)) - matched before Level 4 to avoid shadowing by ([a-z])
    (5, re.compile(r"^\((i{1,3}|iv|v[i]{0,4}|ix|x[i]{0,4}|xiv|xv[i]{0,4}|xix|xx[i]{0,4}|xxix|xxx)\)\s+(.{10,})$", re.I)), 

    # Level 4: List clause items (e.g. (a))
    (4, re.compile(r"^\(([a-z])\)\s+(.{10,})$", re.I)), 
    
    # Standard Markdown heading fallbacks (in case of unnumbered headings)
    (1, re.compile(r"^#\s+(.*)$")),
    (2, re.compile(r"^##\s+(.*)$")),
    (3, re.compile(r"^###\s+(.*)$")),
    (4, re.compile(r"^####\s+(.*)$")),
]


class MasterCircularChunkingStrategy:
    """
    Hierarchical structural chunker with semantic sub-chunking
    specifically designed for SEBI Master Circulars.
    """
    def __init__(
        self,
        chunk_size: int = 1200,
        similarity_threshold: float = 0.60,
        embedding_provider: EmbeddingProvider | None = None,
    ):
        self._chunk_size = chunk_size
        self._similarity_threshold = similarity_threshold
        self._embedding_provider = embedding_provider

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @property
    def similarity_threshold(self) -> float:
        return self._similarity_threshold

    def chunk(self, blocks: List[Block], *, circular_key: str) -> List[TextChunk]:
        # Concatenate block contents
        text_parts = []
        for block in blocks:
            text_parts.append(block.content)
        full_text = "\n\n".join(text_parts)
        print(full_text)
        
        # Dynamic abbreviation extraction from document text
        abbrev_dict = self._extract_abbreviations(full_text)
        
        # Parse tree
        root = self._parse_tree(full_text)
        
        # Process and flatten tree
        final_dataset = []
        self._process_and_flatten_tree(root, final_dataset, self._chunk_size, self._similarity_threshold)
        
        # Convert dataset to TextChunk objects, applying abbreviation expansion
        chunks = []
        for item in final_dataset:
            raw_payload = item["final_search_payload"]
            expanded_payload = self._expand_abbreviations(raw_payload, abbrev_dict)
            
            # Create a unique chunk_id
            payload_hash = hashlib.sha1(expanded_payload.encode("utf-8")).hexdigest()
            chunk_id = f"{circular_key}_{item['node_id']}_{item['sub_idx']}_{payload_hash[:12]}"
            chunks.append(
                TextChunk(
                    chunk_id=chunk_id,
                    chunk_index=len(chunks),
                    text=expanded_payload
                )
            )
        return chunks


        
    def _extract_abbreviations(self, text: str) -> dict[str, str]:
        """Dynamically parses lines to find full-name alongside short-name mappings."""
        # Seed default SEBI / financial abbreviations as fallback
        abbrevs = {
            "AUM": "Assets Under Management",
            "NAV": "Net Asset Value",
            "SEBI": "Securities and Exchange Board of India",
            "AMC": "Asset Management Company",
            "AMFI": "Association of Mutual Funds in India",
            "KYC": "Know Your Client",
            "AIF": "Alternative Investment Fund",
            "CFD": "Corporation Finance Department",
            "RBI": "Reserve Bank of India",
            "IPO": "Initial Public Offering",
            "FPI": "Foreign Portfolio Investor",
            "PMS": "Portfolio Management Services",
            "REIT": "Real Estate Investment Trust",
            "InvIT": "Infrastructure Investment Trust",
            "LODR": "Listing Obligations and Disclosure Requirements",
        }
        
        # 1. Matches: AUM - Assets Under Management or AUM: Assets Under Management
        pat1 = re.compile(r"\b([A-Z]{2,7})\b\s*[:\-]\s*([A-Z][a-z]+(?:\s+[A-Za-z][a-z]+){1,5})")
        # 2. Matches: Assets Under Management (AUM)
        pat2 = re.compile(r"([A-Z][a-z]+(?:\s+[A-Za-z][a-z]+){1,5})\s*\(\s*\b([A-Z]{2,7})\b\s*\)")
        # 3. Matches: | AUM | Assets Under Management |
        pat3 = re.compile(r"\|\s*\b([A-Z]{2,7})\b\s*\|\s*([A-Z][a-z]+(?:\s+[A-Za-z][a-z]+){1,5})\s*\|")
        
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
                
            m3 = pat3.search(line)
            if m3:
                short = m3.group(1).strip()
                full = m3.group(2).strip()
                if len(short) >= 2 and len(full) >= 4:
                    abbrevs[short] = full
                continue
                
            m1 = pat1.search(line)
            if m1:
                short = m1.group(1).strip()
                full = m1.group(2).strip()
                if len(short) >= 2 and len(full) >= 4:
                    abbrevs[short] = full
                continue
                
            m2 = pat2.search(line)
            if m2:
                full = m2.group(1).strip()
                short = m2.group(2).strip()
                if len(short) >= 2 and len(full) >= 4:
                    abbrevs[short] = full
                continue
                
        return abbrevs

    def _parse_tree(self, text: str) -> Node:
        root = Node(id="ROOT", label="SEBI Master Circular", level=0)
        stack = [root]
        current_node = root
        pending_lines = []
        
        # Track created nodes to find prefixes for out-of-order hierarchy rebuilding
        id_to_node = {"ROOT": root}

        def flush(): 
            nonlocal pending_lines
            if not pending_lines: 
                return 
            
            # table protection: check if any line looks like a markdown table row containing a pipe
            is_table = any("|" in l for l in pending_lines)

            if is_table: 
                content = "\n".join(l.strip() for l in pending_lines if l.strip())
            else: 
                content = " ".join(l.strip() for l in pending_lines if l.strip())
                content = re.sub(r"\s{2,}", " ", content).strip()
            
            if content: 
                if current_node.content: 
                    current_node.content += ("\n" + content if is_table else " " + content)
                else: 
                    current_node.content = content
            
            pending_lines = []
        
        def find_structural_parent(child_id: str, child_level: int) -> Node:
            # Check if the child ID is a dotted number (e.g. 10.1 or 2.3.1)
            c_parts = [c for c in child_id.split(".") if c.isdigit()]
            if not c_parts:
                # Fall back to stack-based levels for Roman/non-dotted headers
                while len(stack) > 1 and stack[-1].level >= child_level:
                    stack.pop()
                return stack[-1]
                
            # Try to find the closest dotted parent prefix (e.g. for "10.1", try "10")
            for i in range(len(c_parts) - 1, 0, -1):
                parent_candidate_id = ".".join(c_parts[:i])
                if parent_candidate_id in id_to_node:
                    return id_to_node[parent_candidate_id]
                    
            # If it is a dotted number and we did NOT find a prefix parent in id_to_node,
            # it must be attached to the closest stack node matching its major prefix, or ROOT.
            major_prefix = c_parts[0]
            for node in reversed(stack):
                if node.id == "ROOT":
                    return node
                n_parts = [n for n in node.id.split(".") if n.isdigit()]
                if n_parts and n_parts[0] == major_prefix:
                    return node
            return root

        def attach(new_node, level): 
            nonlocal current_node
            flush()

            # Find the structural parent (via prefix routing or level-based fallbacks)
            parent = find_structural_parent(new_node.id, level)
            new_node.parent = parent
            parent.children.append(new_node)
            id_to_node[new_node.id] = new_node
            
            # Rebuild the stack path from ROOT to the new node to keep tracking consistent
            new_stack = []
            curr = new_node
            while curr:
                new_stack.insert(0, curr)
                curr = curr.parent
            stack[:] = new_stack
            
            # Update current_node to the newly attached node
            current_node = new_node
        
        for line in text.split("\n"): 
            stripped = line.strip()
            if not stripped: 
                continue

            matched = False
            for level, pat in PATTERNS: 
                m = pat.match(stripped)
                if m: 
                    groups = [g for g in m.groups() if g is not None]
                    if not groups:
                        continue
                    sec_id = groups[0].strip()
                    label = groups[1].strip() if len(groups) > 1 else sec_id
                    
                    # sanitize the markdown artifacts out of the parse header token 
                    label = re.sub(r"[#\*_`\-\s:]", " ", label).strip()
                    label = re.sub(r"\s+", " ", label)

                    # Initialize content with original matched header line to ensure text is retained
                    attach(Node(id=sec_id, label=label, level=level, content=stripped), level)
                    matched = True
                    break
            
            if not matched: 
                pending_lines.append(stripped)
        
        flush()
        return root



    def _expand_abbreviations(self, text: str, abbrev_dict: dict[str, str]) -> str:
        """Appends full forms beside short forms if the full form is not already present in the text block."""
        for short, full in abbrev_dict.items():
            # Only expand if the full form isn't already present (case-insensitive check) to avoid bloating/redundancy
            if full.lower() in text.lower():
                continue
            # Replace occurrences of short form with short (full)
            pattern = re.compile(rf'\b{short}\b')
            text = pattern.sub(f"{short} ({full})", text)
        return text

    def _process_and_flatten_tree(self, node, dataset, max_chars=1200, similarity_threshold=0.60): 
        """
        recursively walks thru the hierarchial tree. 
        If a node contains text, it applies linguistic semantic processing.
        """
        if node.content.strip(): 
            raw_text = node.content.strip()

            # run linguisitc semantic splitter inside the hierarchical boundary 
            semantic_fragments = self._semantic_sub_chunking(
                text=raw_text, 
                max_chars=max_chars, 
                similarity_threshold=similarity_threshold 
            )

            # package chunks along with their parent lineage string
            ancestors = node.ancestor_context()
            own_header = f"[{node.id}] {node.label}"
            full_lineage = f"{ancestors} | {own_header}" if ancestors else own_header

            for idx, fragment in enumerate(semantic_fragments): 
                dataset.append({
                    "node_id": node.id,  
                    "node_label": node.label, 
                    "sub_idx": idx, 
                    "lineage_context": fragment, 
                    # search context payload for the LLM / Vector DB 
                    "final_search_payload": f"{full_lineage} | Content: {fragment}"
                })
        
        # tail recursion loop to exhaust entire tree structure 
        for child in node.children: 
            self._process_and_flatten_tree(child, dataset, max_chars, similarity_threshold)


    def _get_spacy_nlp(self):
        """Lazily load the spaCy NLP model, falling back if specific path fails."""
        global _nlp_model
        if _nlp_model is None:
            import spacy
            models_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "models"))
            try:
                _nlp_model = spacy.load(os.path.join(models_dir, "en_core_web_md"))
            except OSError:
                try:
                    _nlp_model = spacy.load("en_core_web_md")
                except OSError:
                    # Fallback to small English model if medium is not available
                    _nlp_model = spacy.load("en_core_web_sm")
        return _nlp_model

    def _semantic_sub_chunking(
        self,
        text: str,
        max_chars: int = 1200,
        # overall needs to be implemeted?....
        #
        similarity_threshold: float = 0.65
    ) -> List[str]:
        """
        Blends spacy sentence extraction with sentence transformer distance math 
        to break long node texts into semantically rich, complete sub-blocks. 
        """
        # Normalize formatting artifacts
        # Replace raw bullet symbols (like \uf0b7, \u2022, \u25cf) with standard bullet '*'
        text = re.sub(r'[\uf0b7\u2022\u25cf]', '*', text)
        # Remove chains of dots (3 or more dots)
        text = re.sub(r'\.{3,}', ' ', text)
        # Collapse multiple whitespaces
        text = re.sub(r'\s{2,}', ' ', text).strip()

        if len(text) <= max_chars: 
            return [text]
        
        nlp = self._get_spacy_nlp()

        # use spacy for linguistic sentence boundary mapping
        doc = nlp(text)
        sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]

        if len(sentences) <= 1:
            return [text]

        # generate vector embeddings for all structural sentences
        embeddings = self._embedding_provider.embed_for_similarity(sentences)

        # import torch
        if not isinstance(embeddings, torch.Tensor):
            embeddings = torch.from_numpy(embeddings) if hasattr(embeddings, "ndim") else torch.tensor(embeddings)

        # Vectorized cosine similarity of adjacent sentence embeddings
        norm_embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
        similarities = torch.sum(norm_embeddings[:-1] * norm_embeddings[1:], dim=1).tolist()

        chunks = []
        current_chunk_sents = [sentences[0]]

        for i in range(len(sentences) - 1): 
            sim = similarities[i]
            next_sentence = sentences[i+1]
            
            # Test combined character length constraints
            current_chunk_text = " ".join(current_chunk_sents)
            combined_len = len(current_chunk_text) + 1 + len(next_sentence)

            # split point triggers if similarity drops below threshold OR combined length exceeds max_chars
            # since 1400 > 200 the char section is split into 2 sub fragments.
            if sim < similarity_threshold or combined_len > max_chars: 
                chunks.append(current_chunk_text)
                current_chunk_sents = [next_sentence]
            else: 
                current_chunk_sents.append(next_sentence)
        
        if current_chunk_sents: 
            chunks.append(" ".join(current_chunk_sents))

        # Post-process to merge tiny fragments (< 30 characters or no alphanumeric chars)
        merged_chunks = []
        i = 0
        while i < len(chunks):
            chunk = chunks[i].strip()
            if not chunk:
                i += 1
                continue
                
            has_alphanumeric = any(c.isalnum() for c in chunk)
            # Bullet prefixes (e.g. symbols like '*', '', or short digits/letters like '2', '(a)', '1.')
            # should be merged forward into the next chunk
            is_bullet_prefix = not has_alphanumeric or (len(chunk) < 5 and chunk.strip("().").isalnum())
            
            if is_bullet_prefix and i + 1 < len(chunks):
                chunks[i+1] = chunk + " " + chunks[i+1]
                i += 1
                continue
                
            # Short trailing fragments should be merged backward
            if (len(chunk) < 30 or not has_alphanumeric) and merged_chunks:
                merged_chunks[-1] = (merged_chunks[-1] + " " + chunk).strip()
            else:
                merged_chunks.append(chunk)
            i += 1

        return merged_chunks


