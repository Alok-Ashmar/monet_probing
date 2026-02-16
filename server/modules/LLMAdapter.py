import os
from openai import OpenAI
from langchain_openai import ChatOpenAI
from langchain_ollama import ChatOllama
from langsmith.wrappers import wrap_openai
from langchain_deepseek import ChatDeepSeek
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate

OPENAI_ORG = os.environ["OPENAI_ORG"]
OPENAI_KEY = os.environ["OPENAI_KEY"]
LLAMA_API_KEY = os.environ["LLAMA_API_KEY"]
DEEPSEEK_API_KEY = os.environ["DEEPSEEK_API_KEY"]
LLAMA_API_URL = os.environ.get("LLAMA_API_URL", "https://api.llmapi.com/")


class LLMAdapter:
    llm = None
    __llm_name = "deepseek"
    __llama_client = None  # For direct API access
    embeddings = None

    def __init__(self, llm_name: str, temperature: float = 0.0, streaming: bool = False):
        self.__llm_name = llm_name
        if llm_name == "chatgpt":
            self.llm = ChatOpenAI(
                organization=OPENAI_ORG,
                api_key=OPENAI_KEY,
                model="gpt-4o-mini",
                temperature=temperature,
                max_retries=2,
                streaming=streaming
            )
        elif llm_name == "deepseek":
            self.llm = ChatDeepSeek(
                api_key=DEEPSEEK_API_KEY,
                model="deepseek-chat",
                temperature=temperature,
                max_tokens=None,
                timeout=None,
                max_retries=2,
                streaming=streaming
            )
        elif llm_name == "llama":
            # Wrap the raw client to enable LangSmith tracing
            self.__llama_client = wrap_openai(OpenAI(
                api_key=LLAMA_API_KEY, 
                base_url=LLAMA_API_URL,
                streaming=streaming
            ))
        elif llm_name == "ollama-mistral":
            self.llm = ChatOllama(model="dolphin-mistral", temperature=temperature, streaming=streaming)
        elif llm_name == "ollama-tiny-llama":
            self.llm = ChatOllama(model="dolphin-mistral", temperature=temperature, streaming=streaming)
        else:
            raise ValueError(f"Unsupported LLM: {llm_name}")

    def invoke(self, prompt: PromptTemplate | ChatPromptTemplate, dependencies: dict[str, str]) -> str:
        if self.__llm_name == "llama":
            # Handle Llama API directly
            messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt.format(**dependencies)},
            ]

            response = self.__llama_client.chat.completions.create(
                messages=messages,
                model="llama-2-70b-chat",  # Adjust model name as needed
                temperature=0.7,
                stream=False,
            )
            return response.choices[0].message.content
        elif self.__llm_name == "ollama-mistral":
            chain = prompt | self.llm
            response = chain.invoke(dependencies)
            return response
        else:
            # Handle other LLMs through LangChain
            chain = prompt | self.llm
            response = chain.invoke(dependencies)
            return response.content