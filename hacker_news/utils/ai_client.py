from openai import OpenAI
import tiktoken
import os
from typing import Union


class AIClient:
    def __init__(self, gpt_model: str, embedding_model: str) -> None:
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.embedding_model = embedding_model
        self.gpt_model = gpt_model

    def embed(self, texts: Union[str, list[str]]) -> list[float]:
        if type(texts) is str:
            texts = texts.replace("\n", " ")
            embeddings = (
                self.client.embeddings.create(input=texts, model=self.embedding_model)
                .data[0]
                .embedding
            )
        else:
            texts = [text.replace("\n", " ") for text in texts]
            embeddings = [
                data.embedding
                for data in self.client.embeddings.create(
                    input=texts, model=self.embedding_model
                ).data
            ]
        return embeddings

    def generate_answer(
        self, system: str, context: str, prompt: str, max_tokens: int = 1000
    ) -> str:
        completion = self.client.chat.completions.create(
            model=self.gpt_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "assistant", "content": context},
                {"role": "user", "content": prompt},
            ],
            max_tokens=max_tokens,
        )
        answer = completion.choices[0].message.content
        return answer

    def _get_tokens(self, text: str) -> list[int]:
        return tiktoken.encoding_for_model(self.gpt_model).encode(text)

    def calculate_embedding_cost(self, text: str):
        cost_per_1k_tokens = {
            "text-embedding-3-small": 0.00002,
            "text-embedding-3-large": 0.00013,
        }
        num_tokens = len(self._get_tokens(text))
        cost = num_tokens / 1000 * cost_per_1k_tokens[self.embedding_model]
        return cost

    def calculate_generation_cost(self, text: str, num_tokens_out: int = 1000):
        cost_per_1k_tokens_in = {"GPT-3.5-turbo-0125": 0.0005}
        cost_per_1k_tokens_out = {"GPT-3.5-turbo-0125": 0.0015}
        num_tokens_in = len(self._get_tokens(text))
        cost_in = num_tokens_in / 1000 * cost_per_1k_tokens_in[self.gpt_model]
        cost_out = num_tokens_out / 1000 * cost_per_1k_tokens_out[self.gpt_model]
        return cost_in + cost_out
