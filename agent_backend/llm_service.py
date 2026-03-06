"""
llm_service.py — Groq LLM client wrapper.

Encapsulates all interactions with the Groq API:
streaming responses, JSON (eval) calls, and text-to-SQL translation.
"""

import asyncio
import json
import logging
from typing import AsyncGenerator

from groq import AsyncGroq

log = logging.getLogger(__name__)


class LLMService:
    """Wraps the async Groq client for streaming, JSON, and SQL translation calls."""

    def __init__(self, api_key: str, model: str = "llama-3.3-70b-versatile") -> None:
        self.client = AsyncGroq(api_key=api_key)
        self.model = model

    # ── Streaming (SSE) ───────────────────────────────────────────

    async def stream_response(
        self,
        system_prompt: str,
        user_prompt: str,
        max_retries: int = 3,
    ) -> AsyncGenerator[str, None]:
        """Stream tokens from Groq as Server-Sent Events with retry."""
        for attempt in range(max_retries):
            try:
                stream = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ],
                    temperature=0.001,
                    max_tokens=4096,
                    stream=True,
                )

                async for chunk in stream:
                    delta = chunk.choices[0].delta.content
                    if delta:
                        yield f"data: {json.dumps({'token': delta})}\n\n"

                yield "data: [DONE]\n\n"
                return

            except Exception as e:
                log.warning(f"Groq API Error on attempt {attempt + 1}/{max_retries}: {e}")
                if attempt == max_retries - 1:
                    error_msg = "\\n\\n**Error:** LLM generation failed after retries."
                    yield f"data: {json.dumps({'token': error_msg})}\n\n"
                    yield "data: [DONE]\n\n"
                else:
                    await asyncio.sleep(2 ** attempt)

    # ── JSON (non-streaming, for evals) ───────────────────────────

    async def call_json(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> dict:
        """Non-streaming Groq call that returns parsed JSON."""
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0.1,
            max_tokens=4096,
            stream=False,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]
            raw = raw.rsplit("```", 1)[0]
        return json.loads(raw)

    # ── Text-to-SQL translation ───────────────────────────────────

    async def translate_prompt_to_sql(
        self,
        prompt: str,
        system_prompt: str,
    ) -> str:
        """Convert a natural language prompt to a PostgreSQL WHERE clause."""
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.0,
            max_tokens=200,
            stream=False,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]
            raw = raw.rsplit("```", 1)[0]
        if raw.lower().startswith("where "):
            raw = raw[6:]
        return raw.strip()
