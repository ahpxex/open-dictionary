from pydantic import BaseModel
from typing import Optional
import json
from open_dictionary.llm.llm_client import get_chat_response


instruction = """
你是一位顶级的词典编纂专家、语言学家，以及精通中英双语的教育家。你的任务是读取并解析一段来自 Wiktionary 的、结构复杂的 JSON 数据，然后将其转化为一份清晰、准确、对中文学习者极其友好的结构化中文词典条目。

**核心任务：**
根据下方提供的输入JSON，严格按照【输出格式定义】生成一个唯一的、完整的 JSON 对象作为最终结果。不要输出任何解释、注释或无关内容。

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
    *   `definition_en`: (string) **英文原义**。从输入JSON的 `glosses` 数组中，提取出**最具体、最完整**的那个英文释义。如果数组中包含一个概括性标题和一个具体释义，请**选择那个具体的释义**。
    *   `definition_cn`: (string) **中文阐释**。此项是核心，请遵循以下原则：
        *   **解释而非翻译**：用**通俗、自然、易懂**的中文来解释 `definition_en` 的核心含义。
        *   **捕捉精髓**：要抓住该词义的**使用场景、语气（如正式、口语、俚语）和细微差别**。
        *   **避免直译**：请**避免生硬的、字典式的直译**。目标是让中文母语者能瞬间理解这个词义的真正用法。
    *   `example`: (object) **为该词义创作一个全新的例句**，包含：
        *   `en`: (string) 一个**简单、现代、生活化**的英文例句，清晰地展示当前词义的用法。**绝对不要使用**输入JSON中提供的复杂或古老的例句。
        *   `cn`: (string) 上述英文例句的对应中文翻译。
7.  `derived`: (array of objects) **派生词**。遍历输入JSON的 `derived` 数组，为其中的**每个单词**生成一个包含以下内容的对象：
    *   `word`: (string) 派生词本身。
    *   `definition_cn`: (string) 对该派生词的**简明中文定义**。
8.  `etymology`: (string) **词源故事**。读取输入JSON中的 `etymology_text` 字段，将其内容翻译并**转述**成一段流畅、易懂的中文。说明其起源语言（如拉丁语、古英语、希腊语）和含义的演变过程，像讲故事一样。

---

**【示例 1】**

**输入:**
word: run
pos: verb
forms[4]:
  - form: runs
    tags[3]: present,singular,third-person
  - form: running
    tags[2]: participle,present
  - form: ran
    tags[1]: past
  - form: run
    tags[2]: participle,past
senses[2]:
  -
    glosses[1]: "To move swiftly on foot, so that both feet leave the ground during each stride."
  -
    glosses[1]: "To manage, be in charge of (a business, campaign, etc.)."
sounds[1,]{ipa,ogg_url}:
  /rʌn/,url
derived[2,]{word}:
  runner
  runway
etymology_text: "From Middle English ronnen, rennen, from Old English rinnan, iernan (“to run, flow”), and Old Norse rinna (“to run, flow”), both from Proto-Germanic *rinnaną (“to run, flow”)."

**你的JSON输出:**
{
  "word": "run",
  "pos": "verb",
  "pronunciations": {
    "ipa": "/rʌn/",
    "natural_phonics": "run",
    "ogg_url": "url"
  },
  "forms": [
    "runs (第三人称单数现在时)",
    "running (现在分词)",
    "ran (过去式)",
    "run (过去分词)"
  ],
  "concise_definition": "跑，奔跑；经营，管理。",
  "detailed_definitions": [
    {
      "definition_en": "To move swiftly on foot, so that both feet leave the ground during each stride.",
      "definition_cn": "指人或动物快速奔跑的动作，强调速度快、双脚交替离地的状态。",
      "example": {
        "en": "I run three miles in the park every morning.",
        "cn": "我每天早上在公园里跑三英里。"
      }
    },
    {
      "definition_en": "To manage, be in charge of (a business, campaign, etc.).",
      "definition_cn": "指负责管理和运营一个组织、项目或生意，强调的是主导和控制的角色。",
      "example": {
        "en": "My friend runs a successful online store.",
        "cn": "我的朋友经营着一家成功的网店。"
      }
    }
  ],
  "derived": [
    {
      "word": "runner",
      "definition_cn": "跑步者；赛跑者。"
    },
    {
      "word": "runway",
      "definition_cn": "（机场的）跑道；（时装秀的）T台。"
    }
  ],
  "etymology": "该词源自中古英语中的 ronnen 和 rennen，更早则来自古英语的 rinnan（意为“奔跑，流动”）和古诺斯语的 rinna（意为“奔跑，流动”）。它们共同源于原始日耳曼语的 *rinnaną，核心含义是“奔跑或流动”。"
}

---

**【示例 2】**

**输入:**
word: philosophy
pos: noun
forms[1]:
  - form: philosophies
    tags[1]: plural
senses[2]:
  -
    glosses[1]: "The study of the fundamental nature of knowledge, reality, and existence, especially when considered as an academic discipline."
  -
    glosses[1]: A theory or attitude held by a person or organization that acts as a guiding principle for behavior.
sounds[1,]{ipa,ogg_url}:
  /fɪˈlɒsəfi/,url
derived[2,]{word}:
  philosopher
  philosophical
etymology_text: "From Middle English philosophie, from Old French philosophie, from Latin philosophia, from Ancient Greek φιλοσοφία (philosophía, “love of wisdom”), from φίλος (phílos, “loving”) + σοφία (sophía, “wisdom”)."

**你的JSON输出:**
{
  "word": "philosophy",
  "pos": "noun",
  "pronunciations": {
    "ipa": "/fɪˈlɒsəfi/",
    "natural_phonics": "phi-lo-so-phy",
    "ogg_url": "url"
  },
  "forms": [
    "philosophies (复数形式)"
  ],
  "concise_definition": "哲学；人生观，信条，准则。",
  "detailed_definitions": [
    {
      "definition_en": "The study of the fundamental nature of knowledge, reality, and existence, especially when considered as an academic discipline.",
      "definition_cn": "作为一门学科，指对知识、真实、存在等根本性问题的探究和思考。",
      "example": {
        "en": "She is studying philosophy at a famous university.",
        "cn": "她在一所著名的大学学习哲学。"
      }
    },
    {
      "definition_en": "A theory or attitude held by a person or organization that acts as a guiding principle for behavior.",
      "definition_cn": "指个人或组织所信奉的一套行事准则或人生观，是指导其决策和行为的根本理念。",
      "example": {
        "en": "My personal philosophy is to treat everyone with respect.",
        "cn": "我个人的人生信条是尊重每一个人。"
      }
    }
  ],
  "derived": [
    {
      "word": "philosopher",
      "definition_cn": "哲学家，思想家。"
    },
    {
      "word": "philosophical",
      "definition_cn": "哲学的，富有哲理的。"
    }
  ],
  "etymology": "该词源自古希腊语的 φιλοσοφία (philosophía)，由 φίλος (phílos，意为“爱”) 和 σοφία (sophía，意为“智慧”) 组成，字面意思即“对智慧的热爱”。它通过拉丁语的 philosophia 和古法语的 philosophie 最终进入中古英语，并演变为现代形式。"
}

---

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
