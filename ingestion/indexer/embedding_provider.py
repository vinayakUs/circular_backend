from __future__ import annotations

from typing import Any
from threading import RLock

class EmbeddingProvider:
    """Produces embeddings for chunk and query text."""

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        raise NotImplementedError

    def embed_query(self, query: str) -> list[float] | None:
        return self.embed_texts([query])[0]

    @property
    def dimensions(self) -> int | None:
        return None

    @property
    def is_enabled(self) -> bool:
        return self.dimensions is not None


class NoOpEmbeddingProvider(EmbeddingProvider):
    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        return [None for _ in texts]


class SentenceTransformerEmbeddingProvider(EmbeddingProvider):
    """Embedding provider backed by sentence-transformers."""

    def __init__(
        self,
        model_name: str,
        *,
        query_instruction: str = "",
        device: str | None = None,
    ) -> None:
        self.model_name = model_name
        self.query_instruction = query_instruction
        self.device = device
        self._model: Any | None = None
        self._dimensions: int | None = None
        # RLock because `dimensions` calls `_load_model()` under the same lock.
        self._init_lock = RLock()
        
    @property
    def dimensions(self) -> int | None:
        cached = self._dimensions
        if cached is not None:
            return cached

        with self._init_lock:
            if self._dimensions is None:
                self._dimensions = int(self._load_model().get_embedding_dimension())
            dimensions = self._dimensions
        assert dimensions is not None
        return dimensions

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        if not texts:
            return []
        model = self._load_model()
        embeddings = model.encode(
            texts,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return [embedding.tolist() for embedding in embeddings]

    def embed_query(self, query: str) -> list[float] | None:
        text = query
        if self.query_instruction:
            text = f"{self.query_instruction}{query}"
        return self.embed_texts([text])[0]

    def _load_model(self) -> Any:

        cached = self._model
        if cached is not None:
            return cached
        with self._init_lock:
            if self._model is None:
                self._model = self._construct_model()
            model = self._model
        assert model is not None
        return model


    def _construct_model(self) -> Any:
        """Bare constructor body — runs under the lock. Split off from
          _load_model so the lock is held only for the (slow) constructor,
          not for the (fast, thread-safe) encode() calls that follow."""

        try:
            from sentence_transformers import SentenceTransformer  # type: ignore[import]
        except ImportError as exc:
                raise RuntimeError(
                    "sentence-transformers is not installed. Install dependencies before using semantic embeddings."
                ) from exc
        model_kwargs: dict[str, Any] = {}

        if self.device:
            model_kwargs["device"] = self.device
        return SentenceTransformer(self.model_name, **model_kwargs)


def build_embedding_provider(
    provider_name: str,
    *,
    enabled: bool,
    model_name: str | None = None,
    query_instruction: str = "",
) -> EmbeddingProvider:
    if not enabled:
        return NoOpEmbeddingProvider()
    if provider_name in {"", "none", "disabled"}:
        return NoOpEmbeddingProvider()
    if provider_name in {"sentence-transformers", "sentence_transformers", "sbert"}:
        if not model_name:
            raise ValueError("model_name is required for sentence-transformers provider")
        return SentenceTransformerEmbeddingProvider(
            model_name=model_name,
            query_instruction=query_instruction,
        )
    raise ValueError(f"Unsupported embedding provider: {provider_name}")
