from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class LanguageSpec:
    code: str
    name: str

    def as_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "name": self.name,
        }


DEFAULT_DEFINITION_LANGUAGE = LanguageSpec(
    code="zh-Hans",
    name="Chinese (Simplified)",
)


def normalize_language_spec(value: LanguageSpec | Mapping[str, Any]) -> LanguageSpec:
    if isinstance(value, LanguageSpec):
        code = value.code
        name = value.name
    elif isinstance(value, Mapping):
        code = value.get("code")
        name = value.get("name")
    else:
        raise TypeError("Language specification must be a LanguageSpec or mapping")

    code_text = str(code or "").strip()
    name_text = str(name or "").strip()
    if not code_text:
        raise ValueError("Language specification code must be a non-empty string")
    if not name_text:
        raise ValueError("Language specification name must be a non-empty string")

    return LanguageSpec(code=code_text, name=name_text)
