"""Ollama HTTP 클라이언트 (llama3)"""

import json
import logging
from typing import Optional, AsyncGenerator, List
import httpx
from ..config import OLLAMA_HOST, OLLAMA_MODEL

logger = logging.getLogger(__name__)


class OllamaClient:
    def __init__(self, host: str = OLLAMA_HOST, model: str = OLLAMA_MODEL):
        self.host  = host
        self.model = model
        self._client = httpx.AsyncClient(timeout=120.0)

    async def close(self):
        await self._client.aclose()

    # ── 단순 완성 ─────────────────────────────────────────────
    async def chat(
        self,
        messages: List[dict],
        temperature: float = 0.3,
        max_tokens: int = 4096,
    ) -> str:
        """messages: [{role: system|user|assistant, content: str}]"""
        payload = {
            "model":    self.model,
            "messages": messages,
            "stream":   False,
            "options":  {"temperature": temperature, "num_predict": max_tokens},
        }
        try:
            resp = await self._client.post(f"{self.host}/api/chat", json=payload)
            resp.raise_for_status()
            return resp.json()["message"]["content"]
        except httpx.HTTPStatusError as e:
            logger.error("Ollama HTTP error: %s", e)
            raise
        except Exception as e:
            logger.error("Ollama error: %s", e)
            raise

    # ── 스트리밍 ──────────────────────────────────────────────
    async def chat_stream(
        self,
        messages: List[dict],
        temperature: float = 0.3,
        max_tokens: int = 4096,
    ) -> AsyncGenerator[str, None]:
        payload = {
            "model":    self.model,
            "messages": messages,
            "stream":   True,
            "options":  {"temperature": temperature, "num_predict": max_tokens},
        }
        async with self._client.stream("POST", f"{self.host}/api/chat", json=payload) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line:
                    continue
                try:
                    chunk = json.loads(line)
                    delta = chunk.get("message", {}).get("content", "")
                    if delta:
                        yield delta
                    if chunk.get("done"):
                        break
                except json.JSONDecodeError:
                    continue

    # ── 임베딩 ────────────────────────────────────────────────
    async def embed(self, text: str) -> List[float]:
        payload = {"model": self.model, "prompt": text}
        try:
            resp = await self._client.post(f"{self.host}/api/embeddings", json=payload)
            resp.raise_for_status()
            return resp.json()["embedding"]
        except Exception as e:
            logger.error("Ollama embed error: %s", e)
            raise


# 싱글턴
_client: Optional[OllamaClient] = None


def get_ollama() -> OllamaClient:
    global _client
    if _client is None:
        _client = OllamaClient()
    return _client


async def close_ollama():
    global _client
    if _client:
        await _client.close()
        _client = None
