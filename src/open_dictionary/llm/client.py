from __future__ import annotations

import json
import urllib.error
import urllib.request

from .config import LLMSettings


class LLMClientError(RuntimeError):
    pass


class OpenAICompatLLMClient:
    def __init__(self, settings: LLMSettings):
        self._settings = settings

    def generate_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str:
        body = {
            "model": self._settings.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "response_format": {"type": "json_object"},
        }
        if max_tokens is not None:
            body["max_tokens"] = max_tokens
        payload = json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self._settings.api_base.rstrip("/") + "/chat/completions",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._settings.api_key}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                data = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError) as exc:
            raise LLMClientError(f"LLM request failed: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise LLMClientError(f"LLM response was not valid JSON: {exc}") from exc

        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LLMClientError("LLM response did not contain chat completion content") from exc
