import logging
import unittest
from typing import Any
from unittest.mock import MagicMock

from utils.llm_providers.minmax_llm_client import MinmaxLLMClient
from utils.llm_providers.nvidia_llm_client import NvidiaLLMClient
from utils.llm_providers.ollama_llm_client import OllamaLLMClient


class ProviderMaxTokensTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger("test.provider_max_tokens")

    def _wire(self, client: Any) -> list[dict[str, Any]]:
        captured: list[dict[str, Any]] = []
        completions = MagicMock()

        def _create(**kwargs: Any) -> str:
            captured.append(kwargs)
            return f"response-{len(captured)}"

        completions.create.side_effect = _create
        chat = MagicMock()
        chat.completions = completions
        instructor_client = MagicMock()
        instructor_client.chat = chat
        client.get_client = MagicMock(return_value=instructor_client)
        return captured

    def test_ollama_includes_max_tokens_when_supplied(self) -> None:
        client = OllamaLLMClient.__new__(OllamaLLMClient)
        client._client = None
        captured = self._wire(client)

        results = client.create_completions_parallel(
            prompts=["hi"],
            model="apex-coder-max",
            response_model=str,
            max_tokens=512,
        )

        self.assertEqual(results, ["response-1"])
        self.assertEqual(captured[0]["max_tokens"], 512)

    def test_ollama_omits_max_tokens_when_unset(self) -> None:
        client = OllamaLLMClient.__new__(OllamaLLMClient)
        client._client = None
        captured = self._wire(client)

        results = client.create_completions_parallel(
            prompts=["hi"],
            model="apex-coder-max",
            response_model=str,
        )

        self.assertEqual(results, ["response-1"])
        self.assertNotIn("max_tokens", captured[0])

    def test_minimax_includes_max_tokens_when_supplied(self) -> None:
        client = MinmaxLLMClient.__new__(MinmaxLLMClient)
        client._client = None
        captured = self._wire(client)

        results = client.create_completions_parallel(
            prompts=["hi"],
            model="model",
            response_model=str,
            max_tokens=128,
        )

        self.assertEqual(results, ["response-1"])
        self.assertEqual(captured[0]["max_tokens"], 128)

    def test_minimax_omits_max_tokens_when_unset(self) -> None:
        client = MinmaxLLMClient.__new__(MinmaxLLMClient)
        client._client = None
        captured = self._wire(client)

        results = client.create_completions_parallel(
            prompts=["hi"],
            model="model",
            response_model=str,
        )

        self.assertEqual(results, ["response-1"])
        self.assertNotIn("max_tokens", captured[0])

    def test_nvidia_includes_max_tokens_when_supplied(self) -> None:
        client = NvidiaLLMClient.__new__(NvidiaLLMClient)
        client._client = None
        captured = self._wire(client)

        results = client.create_completions_parallel(
            prompts=["hi"],
            model="model",
            response_model=str,
            max_tokens=256,
        )

        self.assertEqual(results, ["response-1"])
        self.assertEqual(captured[0]["max_tokens"], 256)

    def test_nvidia_omits_max_tokens_when_unset(self) -> None:
        client = NvidiaLLMClient.__new__(NvidiaLLMClient)
        client._client = None
        captured = self._wire(client)

        results = client.create_completions_parallel(
            prompts=["hi"],
            model="model",
            response_model=str,
        )

        self.assertEqual(results, ["response-1"])
        self.assertNotIn("max_tokens", captured[0])


if __name__ == "__main__":
    unittest.main()