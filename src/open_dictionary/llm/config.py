from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class LLMSettings:
    api_base: str
    api_key: str
    model: str


def load_llm_settings(*, env_file: str | Path | None = ".env") -> LLMSettings:
    env_path = Path(env_file) if env_file else None
    if env_path is not None:
        load_dotenv(env_path, override=True)

    api_base = os.getenv("LLM_API")
    api_key = os.getenv("LLM_KEY")
    model = os.getenv("LLM_MODEL")
    if not api_base:
        raise RuntimeError("Environment variable LLM_API is not set.")
    if api_key is None:
        raise RuntimeError("Environment variable LLM_KEY is not set.")
    if not model:
        raise RuntimeError("Environment variable LLM_MODEL is not set.")
    return LLMSettings(api_base=api_base, api_key=api_key, model=model)
