import logging
from google import genai
import yaml
import time
import json
import re
import os

CURENT_DIR = os.path.dirname(os.path.abspath(__file__))


class Gemini:
    def __init__(
        self, model="gemini-2.0-flash", max_attempts=5, json_return=False
    ) -> None:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment")

        self.client = genai.Client(api_key=api_key)
        self.model = model
        self.max_attempts = max_attempts
        self.json_return = json_return
        with open(f"{CURENT_DIR}/.promts.yml", "r", encoding="utf-8") as file:
            self.promt_data = yaml.safe_load(file)

    def _safe_parse_ai_json(self, response_text: str):
        try:
            response_text = response_text.strip()

            json_match = re.search(r"``````", response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1).strip()
            else:
                lines = [
                    line
                    for line in response_text.split("\n")
                    if not line.strip().startswith("```")
                ]
                json_str = "\n".join(lines).strip()

            # Заменяем Python-значения на JSON-совместимые для корректного парсинга
            json_str = (
                json_str.replace("None", "null")
                .replace("True", "true")
                .replace("False", "false")
            )

            # Парсим JSON в словарь Python
            data = json.loads(json_str)

            return data

        except json.JSONDecodeError as e:
            logging.error(f"Ошибка парсинга JSON: {e}")
            logging.warning(f"Проблемный фрагмент: {repr(json_str[:200])}")
            return None
        except Exception as e:
            logging.error(f"Неожиданная ошибка: {e}")
            return None

    def get_promt(self, type_promt: str):
        logging.info(f"Используется промт: {self.promt_data}")
        prompt = self.promt_data.get(type_promt, None).get("prompt", None)
        if prompt is None:
            raise ValueError("Нет такого промта")
        else:
            return prompt

    def generate_content(self, prompt: str):
        try:
            for attempt in range(self.max_attempts):
                try:
                    response = self.client.models.generate_content(
                        model=self.model, contents=prompt
                    )

                    if response.text:
                        gem_response = response.text
                        if self.json_return is False:
                            return {
                                "success": True,
                                "format": "text",
                                "result": gem_response,
                            }
                        else:
                            result = self._safe_parse_ai_json(gem_response)
                            if result is not None:
                                return {
                                    "success": True,
                                    "format": "json",
                                    "result": result,
                                }
                            return {
                                "success": False,
                                "format": "text",
                                "result": gem_response,
                            }

                    else:
                        return {"success": False, "error": "Нет ответа от Gemini"}

                except Exception as e:
                    if "429" in str(e):
                        match = re.search(r"Please retry in ([\d.]+)s\.", str(e))
                        if match:
                            seconds = float(match.group(1))  # 14.234234
                            seconds = seconds + 60
                            print(seconds)
                            time.sleep(seconds)
                    elif "503" in str(e) or attempt < self.max_attempts - 1:
                        wait_time = 3**attempt  # 1, 2, 4, 8, 16 секунд
                        print(
                            f"Попытка {attempt + 1} не удалась. Ждём {wait_time} сек..."
                        )
                        time.sleep(wait_time)
                    else:
                        raise e

        except Exception as error:
            return {"success": False, "error": str(error)}
        return {"success": False, "error": "Кончились попытки"}
