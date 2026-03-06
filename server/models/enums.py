from enum import Enum


class LLMEnum(str, Enum):
    """
    Enum representing support Language Models (LLMs).
    """

    chatgpt = "chatgpt"
    deepseek = "deepseek"
    grok = "grok"
    claude = "claude"
    llama = "llama"
    ollama_mistral = "ollama-mistral"
    ollama_tiny_llama = "ollama-tiny-llama"


class StatusEnum(str, Enum):
    """
    Enum representing the status of an entity, such as a Survey.
    """

    active = "active"
    draft = "draft"


class StrategyEnum(str, Enum):
    """
    Enum representing different strategies, typically used for target configurations.
    """

    presence = "presence"
    absence = "absence"
    avoid_on = "avoid_on"
    canned = "canned"
