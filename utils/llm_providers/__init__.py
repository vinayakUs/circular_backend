from utils.llm_providers.base import BaseLLMClient
from utils.llm_providers.minmax_llm_client import MinmaxLLMClient
from utils.llm_providers.nvidia_llm_client import NvidiaLLMClient
from utils.llm_providers.ollama_llm_client import OllamaLLMClient

__all__ = ["BaseLLMClient", "MinmaxLLMClient", "NvidiaLLMClient", "OllamaLLMClient", "LLM_PROVIDERS"]

LLM_PROVIDERS: dict[str, type[BaseLLMClient]] = {
    "nvidia": NvidiaLLMClient,
    "minmax": MinmaxLLMClient,
    "ollama": OllamaLLMClient,
}


def get_llm_provider(name: str) -> BaseLLMClient:
    """Get an LLM provider instance by name."""
    if name not in LLM_PROVIDERS:
        raise ValueError(f"Unknown LLM provider: {name}. Available: {list(LLM_PROVIDERS.keys())}")
    return LLM_PROVIDERS[name]()
