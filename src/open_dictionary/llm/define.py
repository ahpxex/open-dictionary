from pydantic import BaseModel
from typing import Optional
import json
from open_dictionary.llm.llm_client import get_chat_response


instruction = """
你是一位顶级的词典编纂专家、语言学家，以及精通中英双语的教育家。你的任务是读取并解析一段来自 Wiktionary 的、结构复杂的数据，然后将其转化为一份清晰、准确、对中文学习者极其友好的结构化中文词典条目。

**核心任务：**
根据下方提供的输入JSON，严格按照【输出格式定义】生成一个唯一的、完整的 JSON 对象作为最终结果。不要输出任何解释、注释或无关内容。

**【重要：JSON 格式规范】**
1. 输出必须是严格、合法的 JSON 格式。
2. **所有字符串值中的双引号 (") 必须使用反斜杠转义为 \"**。
3. **所有字符串值中的反斜杠 (\) 必须转义为 \\**。

---

**【输出格式定义】**

请生成一个包含以下键 (key) 的 JSON 对象：

1.  `word`: (string) 英文单词本身。
2.  `pos`: (string) 词性。
3.  `pronunciations`: (object) 一个包含发音方式和音频文件的对象：
    *   `ipa`: (string) 国际音标。直接从输入JSON的 `sounds` 数组中提取 `ipa` 字段的值。
    *   `natural_phonics`: (string) 自然拼读。根据单词的拼写和音节，生成一个对初学者友好的、用连字符分隔的拼读提示。例如 "philosophy" -> "phi-lo-so-phy"。
    *   `ogg_url`: (string) OGG音频文件链接。从输入JSON的 `sounds` 数组中查找并提取 `ogg_url` 字段的值。如果不存在，则返回 `null`。
4.  `forms`: (array of strings) **词形变化**。遍历输入JSON的 `forms` 数组，将每个词形 (`form`) 及其标签 (`tags`) 组合成一个易于理解的中文描述字符串。例如：`"hits (第三人称单数现在时)"`。
5.  `concise_definition`: (string) **简明释义**。在分析完所有词义后，用一句话高度概括该单词最核心、最常用的1-2个中文意思。
6.  `detailed_definitions`: (array) **详细释义数组**。遍历输入JSON中 `senses` 数组的每一个对象，为每个词义生成一个包含以下内容的对象：
    *   `definition_en`: (string) **英文原义**。从输入JSON的 `glosses` 数组中，提取出**最具体、最完整**的那个英文释义。如果数组中包含一个概括性标题和一个具体释义，请**选择那个具体的释义**。**注意：如果原文包含引号，必须转义。**
    *   `definition_cn`: (string) **中文阐释**。此项是核心，请遵循以下原则：
        *   **解释而非翻译**：用**通俗、自然、易懂**的中文来解释 `definition_en` 的核心含义。
        *   **捕捉精髓**：要抓住该词义的**使用场景、语气（如正式、口语、俚语）和细微差别**。
        *   **避免直译**：请**避免生硬的、字典式的直译**。目标是让中文母语者能瞬间理解这个词义的真正用法。
        *   **转义规则**：如果中文阐释中需要使用引号（如「」、""），请使用中文引号，避免使用英文双引号。如果必须使用英文双引号，务必转义。
    *   `example`: (object) **为该词义创作一个全新的例句**，包含：
        *   `en`: (string) 一个**简单、现代、生活化**的英文例句，清晰地展示当前词义的用法。**绝对不要使用**输入JSON中提供的复杂或古老的例句。**如果例句中包含引号，必须转义。**
        *   `cn`: (string) 上述英文例句的对应中文翻译。**如果翻译中包含英文引号，必须转义。**
7.  `derived`: (array of objects) **派生词**。遍历输入JSON的 `derived` 数组，为其中的**每个单词**生成一个包含以下内容的对象：
    *   `word`: (string) 派生词本身。
    *   `definition_cn`: (string) 对该派生词的**简明中文定义**。
8.  `etymology`: (string) **词源故事**。读取输入JSON中的 `etymology_text` 字段，将其内容翻译并**转述**成一段流畅、易懂的中文。说明其起源语言（如拉丁语、古英语、希腊语）和含义的演变过程，像讲故事一样。**如果词源中包含引号，必须转义。**

---

**【示例】**

**输入:**
word: quote
pos: verb
forms[2]:
  - form: quotes
    tags[2]: present,singular,third-person
  - form: quoted
    tags[1]: past
senses[1]:
  -
    glosses[1]: "To repeat or copy out (words from a text or speech written or spoken by another person)."
sounds[1,]{ipa,ogg_url}:
  /kwəʊt/,url
derived[1,]{word}:
  quotation
etymology_text: "From Medieval Latin quotare meaning \"to mark with numbers\"."

**你的JSON输出:**
{
  "word": "quote",
  "pos": "verb",
  "pronunciations": {
    "ipa": "/kwəʊt/",
    "natural_phonics": "quote",
    "ogg_url": "url"
  },
  "forms": [
    "quotes (第三人称单数现在时)",
    "quoted (过去式)"
  ],
  "concise_definition": "引用，引述。",
  "detailed_definitions": [
    {
      "definition_en": "To repeat or copy out (words from a text or speech written or spoken by another person).",
      "definition_cn": "指重复或摘录他人的话语或文字，通常用于写作、演讲中引用权威来源或他人观点。",
      "example": {
        "en": "She quoted Shakespeare by saying \"To be or not to be\".",
        "cn": "她引用了莎士比亚的话说「生存还是毁灭」。"
      }
    }
  ],
  "derived": [
    {
      "word": "quotation",
      "definition_cn": "引文，引语；报价。"
    }
  ],
  "etymology": "该词源自中世纪拉丁语 quotare，意为「标记数字」。"
}

"""

class Example(BaseModel):
    en: str
    cn: str


class DetailedDefinition(BaseModel):
    definition_en: str
    definition_cn: str
    example: Example


class DerivedWord(BaseModel):
    word: str
    definition_cn: str


class Pronunciations(BaseModel):
    ipa: str
    natural_phonics: str
    ogg_url: Optional[str] = None


class Definition(BaseModel):
    word: str
    pos: str
    pronunciations: Pronunciations
    forms: list[str]
    concise_definition: str
    detailed_definitions: list[DetailedDefinition]
    derived: list[DerivedWord]
    etymology: str


def define(input_data: str) -> Definition:
    """Generate a structured dictionary definition from Wiktionary JSON/Toon data.

    Args:
        input_data: String containing Wiktionary data in JSON or Toon format

    Returns:
        Definition object with structured dictionary entry
    """
    response = get_chat_response(instruction, input_data)

    try:
        return Definition.model_validate_json(response)
    except Exception as exc:
        # Attach the raw response to the exception for error logging
        exc.llm_response = response  # type: ignore
        raise
