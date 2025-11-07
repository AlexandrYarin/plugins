import os
import yaml
import requests
import logging
import time
import io
import zipfile

config_path = os.path.join(os.path.dirname(__file__), ".config.yaml")
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

BITRIX_DATA = config["bitrix_data"]
DOMAIN, USER_ID, WH_CODE = (
    BITRIX_DATA["DOMAIN"],
    BITRIX_DATA["USER_ID"],
    config["wh_code"],
)
WEBHOOK_URL = f"https://{DOMAIN}/rest/{USER_ID}/"


def parsing_fields(fields: dict, needed_fields: dict) -> bool | dict:
    data = {}
    for field in needed_fields.keys():
        if needed_fields[field] is False:
            continue
        raw_dict: list = fields[field]["items"]
        if raw_dict:
            new_values = []
            for value in needed_fields[field]:
                for field_raw in raw_dict:
                    if field_raw["ID"] == str(value):
                        new_values.append(field_raw["VALUE"])
            data[field] = new_values
    if len(data.keys()) < len(needed_fields) and any(
        value == [] for value in data.values()
    ):
        logging.error("Не удалось распарсить все необходимые поля")
        return False
    return data


def download_file(download_url) -> list:
    # 1. Конфигурация
    LOGIN_URL = f"https://{DOMAIN}/auth/?backurl=%2F"
    USERNAME = config["bitrix_data"]["BITRIX_SESSION"]["username"]
    PASSWORD = config["bitrix_data"]["BITRIX_SESSION"]["password"]

    # 2. Авторизация и получение cookie
    session = requests.Session()
    # Получаем страницу логина для получения csrf-токена
    login_page = session.get(LOGIN_URL)
    # Bitrix требует POST с полями: USER_LOGIN, USER_PASSWORD, AUTH_FORM, TYPE
    payload = {
        "USER_LOGIN": USERNAME,
        "USER_PASSWORD": PASSWORD,
        "AUTH_FORM": "Y",
        "TYPE": "AUTH",
    }
    # Отправляем POST-запрос для авторизации
    auth_response = session.post(LOGIN_URL, data=payload)
    if "Авторизация" in auth_response.text or auth_response.status_code != 200:
        logging.error("Ошибка авторизации")
        raise ValueError("Ошибка авторизации")

    full_url = f"https://{DOMAIN}{download_url}"

    # 4. Скачиваем файл с авторизацией через session (актуальные cookie)
    response = session.get(full_url)
    # Проверка, что response.content — это XLS (ZIP-архив)
    #
    if response.content.startswith(
        b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
    ) or response.content.startswith(b"<div"):
        return [response.content, "xls"]
    else:
        if not response.content.startswith(b"PK\x03\x04"):
            raise ValueError("Файл не является XLS-документом!")
        else:
            return [response.content, "xlsx"]


def download_file_mode(download_url) -> list:
    # 1. Конфигурация
    LOGIN_URL = f"https://{DOMAIN}/auth/?backurl=%2F"
    USERNAME = config["bitrix_data"]["BITRIX_SESSION"]["username"]
    PASSWORD = config["bitrix_data"]["BITRIX_SESSION"]["password"]

    # 2. Авторизация и получение cookie
    session = requests.Session()
    # Получаем страницу логина для получения csrf-токена
    login_page = session.get(LOGIN_URL)
    # Bitrix требует POST с полями: USER_LOGIN, USER_PASSWORD, AUTH_FORM, TYPE
    payload = {
        "USER_LOGIN": USERNAME,
        "USER_PASSWORD": PASSWORD,
        "AUTH_FORM": "Y",
        "TYPE": "AUTH",
    }
    # Отправляем POST-запрос для авторизации
    auth_response = session.post(LOGIN_URL, data=payload)
    if "Авторизация" in auth_response.text or auth_response.status_code != 200:
        logging.error("Ошибка авторизации")
        raise ValueError("Ошибка авторизации")

    full_url = f"https://{DOMAIN}{download_url}"

    # 4. Скачиваем файл с авторизацией через session (актуальные cookie)
    response = session.get(full_url)
    # Проверка формата
    if response.content.startswith(b"%PDF"):
        return [response.content, "pdf"]
    elif response.content.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return [response.content, "doc_or_xls"]
    elif response.content.startswith(b"PK\x03\x04"):
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            namelist = zf.namelist()
            if any(name.startswith("word/") for name in namelist):
                return [response.content, "docx"]
            elif any(name.startswith("xl/") for name in namelist):
                return [response.content, "xlsx"]
            else:
                raise ValueError("Unknown ZIP-based Office file format!")
    else:
        raise ValueError("Unknown file format!")


def _b24_request(code, method, params=None):
    """
    Функция для запросов к Bitrix24 API

    :param webhook_url: URL вебхука
    :param method: Метод API
    :param params: Параметры запроса (словарь)
    :return: Ответ сервера в формате JSON
    """
    if params is None:
        params = {}

    url = WEBHOOK_URL + code + "/" + method + ".json"
    count = 0
    while count < 3:
        try:
            response = requests.post(url, json=params, timeout=60)

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            logging.exception("error in b24_request")
            count += 1
            time.sleep(2)
            continue
        except Exception as error:
            print(f"error in b24: {error}")
            count += 1


def query_to_bitrix(query_name, raw_result=False, **kwargs):
    params = {}
    if kwargs:
        for key, value in kwargs.items():
            params[key] = value
    code = WH_CODE[query_name]["code"]
    query = WH_CODE[query_name]["query"]
    result = _b24_request(code, query, params)
    time.sleep(1)
    if result and result.get("result"):
        if raw_result:
            return result
        return result["result"]
    else:
        logging.warning(f"Ошибка при запросe {query_name}: {result}")
        return None


def get_all_pages(method, params={}) -> list | None:
    """
    Получает все страницы данных из API, используя пагинацию.

    :param method: str - Название метода API для запроса.
    :param params: dict - Параметры запроса.
    :return: list - Список, содержащий все данные со всех страниц.
    """
    data = []
    start = 0
    while True:
        params["start"] = start
        result = query_to_bitrix(method, raw_result=True, **params)
        if result is None or "result" not in result:
            break

        items = result.get("result", [])
        data.extend(items)

        next_start = result.get("next")
        # Если нет next или список пуст — выходим
        if not next_start or not items:
            break

        start = next_start
    return data
