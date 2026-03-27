import json
import logging
import os
import random
import time
import uuid

import requests

try:
    import extensions.telegram_bot.source.utils as utils
except ImportError:
    import source.utils as utils


class ComfyUIApi:
    def __init__(
        self,
        url="",
        workflow_file_path="",
        prompt_node_id="",
        prompt_field="text",
        negative_prompt="",
        negative_prompt_node_id="",
        negative_prompt_field="text",
        seed_node_id="",
        seed_field="seed",
        timeout_sec=120,
        poll_interval_sec=1.0,
    ):
        if url.startswith("http"):
            self.url = url
        else:
            self.url = "http://127.0.0.1:8188"

        self.workflow_file_path = workflow_file_path
        self.prompt_node_id = str(prompt_node_id or "").strip()
        self.prompt_field = str(prompt_field or "text").strip() or "text"
        self.negative_prompt = negative_prompt or ""
        self.negative_prompt_node_id = str(negative_prompt_node_id or "").strip()
        self.negative_prompt_field = str(negative_prompt_field or "text").strip() or "text"
        self.seed_node_id = str(seed_node_id or "").strip()
        self.seed_field = str(seed_field or "seed").strip() or "seed"
        self.timeout_sec = int(timeout_sec or 120)
        self.poll_interval_sec = float(poll_interval_sec or 1.0)
        self.client_id = uuid.uuid4().hex

        self.workflow = self._load_workflow(self.workflow_file_path)
        logging.info("### ComfyUIApi INIT DONE ###")

    async def get_image(self, prompt: str):
        return await self.txt_to_image(prompt)

    @utils.async_wrap
    def txt_to_image(self, prompt: str):
        if not self.workflow:
            raise ValueError("ComfyUI workflow is not loaded. Check comfyui_workflow_file_path.")

        workflow = json.loads(json.dumps(self.workflow))
        self._apply_prompt(workflow, prompt)
        self._apply_negative_prompt(workflow)
        self._apply_seed(workflow)

        payload = {
            "prompt": workflow,
            "client_id": self.client_id,
        }
        response = requests.post(url=f"{self.url}/prompt", json=payload, timeout=30)
        response.raise_for_status()
        prompt_id = response.json().get("prompt_id")
        if not prompt_id:
            raise RuntimeError("ComfyUI did not return prompt_id.")

        history = self._wait_for_history(prompt_id)
        return self._download_images(history)

    def _load_workflow(self, workflow_file_path: str):
        if not workflow_file_path:
            logging.error("ComfyUI workflow file path is empty.")
            return None
        if not os.path.exists(workflow_file_path):
            logging.error("ComfyUI workflow file not found: %s", workflow_file_path)
            return None
        with open(workflow_file_path, "r", encoding="utf-8") as workflow_file:
            data = json.load(workflow_file)

        if isinstance(data, dict) and isinstance(data.get("prompt"), dict):
            return data["prompt"]
        if self._looks_like_prompt_graph(data):
            return data

        logging.error("ComfyUI workflow must be exported in API prompt format.")
        return None

    @staticmethod
    def _looks_like_prompt_graph(data) -> bool:
        if not isinstance(data, dict) or not data:
            return False
        sample = next(iter(data.values()))
        return isinstance(sample, dict) and "class_type" in sample and "inputs" in sample

    def _resolve_prompt_node_id(self, workflow) -> str | None:
        if self.prompt_node_id:
            if self.prompt_node_id in workflow:
                return self.prompt_node_id
            logging.warning("ComfyUI prompt_node_id not found in workflow: %s", self.prompt_node_id)

        candidates = []
        for node_id, node in workflow.items():
            class_type = str(node.get("class_type", ""))
            inputs = node.get("inputs", {})
            if "CLIPTextEncode" in class_type and isinstance(inputs, dict) and "text" in inputs:
                candidates.append(node_id)
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            logging.warning(
                "ComfyUI has multiple CLIPTextEncode nodes; set comfyui_prompt_node_id. Using %s",
                candidates[0],
            )
            return candidates[0]
        return None

    def _apply_prompt(self, workflow, prompt: str):
        prompt_node_id = self._resolve_prompt_node_id(workflow)
        if not prompt_node_id:
            raise ValueError("ComfyUI prompt node not resolved. Set comfyui_prompt_node_id.")
        node = workflow.get(prompt_node_id, {})
        inputs = node.get("inputs", {})
        inputs[self.prompt_field] = prompt
        node["inputs"] = inputs
        workflow[prompt_node_id] = node

    def _apply_negative_prompt(self, workflow):
        if not self.negative_prompt or not self.negative_prompt_node_id:
            return
        node = workflow.get(self.negative_prompt_node_id)
        if not node:
            logging.warning(
                "ComfyUI negative_prompt_node_id not found in workflow: %s",
                self.negative_prompt_node_id,
            )
            return
        inputs = node.get("inputs", {})
        inputs[self.negative_prompt_field] = self.negative_prompt
        node["inputs"] = inputs
        workflow[self.negative_prompt_node_id] = node

    def _apply_seed(self, workflow):
        if not self.seed_node_id:
            return
        node = workflow.get(self.seed_node_id)
        if not node:
            logging.warning("ComfyUI seed_node_id not found in workflow: %s", self.seed_node_id)
            return
        inputs = node.get("inputs", {})
        inputs[self.seed_field] = random.randint(0, 2**31 - 1)
        node["inputs"] = inputs
        workflow[self.seed_node_id] = node

    def _wait_for_history(self, prompt_id: str):
        deadline = time.time() + self.timeout_sec
        last_exception = None
        while time.time() < deadline:
            try:
                response = requests.get(url=f"{self.url}/history/{prompt_id}", timeout=15)
                response.raise_for_status()
                history = response.json()
                if prompt_id in history and history[prompt_id].get("outputs"):
                    return history[prompt_id]
            except Exception as exc:
                last_exception = exc
            time.sleep(self.poll_interval_sec)
        raise TimeoutError(f"ComfyUI prompt did not finish in {self.timeout_sec}s") from last_exception

    def _download_images(self, history):
        output_files = []
        outputs = history.get("outputs", {})
        for output in outputs.values():
            images = output.get("images", [])
            for image_info in images:
                filename = image_info.get("filename")
                if not filename:
                    continue
                params = {
                    "filename": filename,
                    "subfolder": image_info.get("subfolder", ""),
                    "type": image_info.get("type", "output"),
                }
                response = requests.get(url=f"{self.url}/view", params=params, timeout=30)
                response.raise_for_status()
                ext = os.path.splitext(filename)[1] or ".png"
                output_file = f"{random.random()}{ext}"
                with open(output_file, "wb") as out_file:
                    out_file.write(response.content)
                output_files.append(output_file)
        return output_files
