# Copyright 2025 Michael Bungenstock
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import asyncio
from asyncio import Queue, Lock
import sys
import time
from typing import Iterable
import vertexai
from dataclasses import dataclass
from vertexai.preview.generative_models import (
    GenerativeModel,
    GenerationConfig,
    HarmCategory,
    HarmBlockThreshold,
)
from google import auth
from google.cloud import bigquery


class BatchProcessor:
    def __init__(self, model: GenerativeModel):
        self.model = model
        self.lock = Lock()

    async def process(self, rows: Iterable, prompt: str, workers: int = 8) -> list:
        prompts = Queue()
        # fill the queue with prompts
        for row in rows:
            prompts.put_nowait(prompt.format(data=row))

        tasks = []
        result = []
        # start the workers
        for _ in range(workers):
            task = asyncio.create_task(self.worker(prompts, result))
            tasks.append(task)

        # block until all requests are processed
        await prompts.join()

        # cancel all workers
        for task in tasks:
            task.cancel()
        # Wait until all workers are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)

        return result

    async def worker(self, prompts: Queue, result: list) -> None:
        while True:
            prompt = await prompts.get()
            start = time.monotonic()
            response = await self.model.generate_content_async(prompt)
            end = time.monotonic()
            print(f"Job executed in {end-start:.2f} seconds")
            async with self.lock:
                result.append(
                    Result(prompt=prompt, response=response, start=start, end=end)
                )
            prompts.task_done()


@dataclass()
class Result:
    prompt: str
    response: str
    start: float
    end: float


async def main():
    import time

    # change values here
    credentials, project_id = auth.default()
    REGION = "us-central1"
    # limits the query to # rows
    BQ_ROW_LIMIT = 50
    # number of parallel calls
    WORKERS = 50

    # create genai model
    vertexai.init(project=project_id, location=REGION, credentials=credentials)

    generation_config = GenerationConfig(
        max_output_tokens=1200, temperature=0.0, top_p=0.0
    )

    safety_settings = {
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    }

    system_instruction = """
You are a friendly and helpful assistant.
Ensure your answers are complete, unless the user requests a more concise approach.
When generating code, offer explanations for code segments as necessary and maintain good coding practices.
When presented with inquiries seeking information, provide answers that reflect a deep understanding of the field, guaranteeing their correctness.
For any non-english queries, respond in the same language as the prompt unless otherwise specified by the user.
For prompts involving reasoning, provide a clear explanation of each step in the reasoning process before presenting the final answer.
    """

    model = GenerativeModel(
        model_name="gemini-1.5-pro-002",  # gemini-1.5-pro-002
        system_instruction=system_instruction,
        generation_config=generation_config,
        safety_settings=safety_settings,
    )

    print(f"Loading {BQ_ROW_LIMIT} rows from BigQuery")
    client = bigquery.Client(project=project_id, credentials=credentials)
    job = client.query(
        f"SELECT body FROM `bigquery-public-data.stackoverflow.posts_questions` LIMIT {BQ_ROW_LIMIT}"
    )

    rows = [row.body for row in job.result()]
    print("done")

    started_at = time.monotonic()
    bp = BatchProcessor(model=model)
    results = await bp.process(
        rows=rows,
        prompt="Summarize the follwoing question in two sentences: {data}",
        workers=WORKERS,
    )
    total_seconds = time.monotonic() - started_at

    # calculate some statistics
    size = len(results)
    total_time = 0.0
    min_time = sys.float_info.max
    max_time = 0.0
    for result in results:
        time = result.end - result.start
        min_time = min(min_time, time)
        max_time = max(max_time, time)
        total_time += time
    average_time = total_time / size

    print("====")
    print(f"Min time: {min_time:.2f} seconds")
    print(f"Max time: {max_time:.2f} seconds")
    print(f"Average : {average_time:.2f} seconds")
    print(f"{WORKERS} workers in parallel ran for {total_seconds:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
