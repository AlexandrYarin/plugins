from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread
from functools import wraps
from datetime import date, datetime
from decimal import Decimal
import io
import os
import logging
import time

current_dir = os.path.dirname(os.path.abspath(__file__))


SERVICE_ACCOUNT_INFO = f"{current_dir}/service_account.json"
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/spreadsheets",
]


def retry_on_network_error(max_retries=5, initial_delay=2, backoff=2):
    """
    Декоратор для повторных попыток при сетевых ошибках

    Args:
        max_retries: максимальное количество попыток
        initial_delay: начальная задержка в секундах
        backoff: множитель для экспоненциальной задержки
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (
                    socket.gaierror,
                    httplib2.ServerNotFoundError,
                    ConnectionError,
                    TimeoutError,
                ) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logging.warning(
                            f"Попытка {attempt + 1}/{max_retries} не удалась: {e}. "
                            f"Повтор через {delay} сек..."
                        )
                        time.sleep(delay)
                        delay *= backoff
                    else:
                        logging.error(
                            f"Все {max_retries} попыток исчерпаны. Последняя ошибка: {e}"
                        )

            raise last_exception

        return wrapper

    return decorator


class GoogleAccountOAuth:
    def __init__(self):
        self.OAUTH_CREDENTIALS = f"{current_dir}/oauth_credentials.json"  # OAuth файл
        self.TOKEN_FILE = f"{current_dir}/token.json"
        self.SCOPES = SCOPES
        self._services = []
        self.credentials = self._get_oauth_credentials()

        logging.info("✅ OAuth авторизация успешна")

    def _get_oauth_credentials(self):
        """Получение OAuth credentials"""
        creds = None

        # Проверяем сохраненные токены
        if os.path.exists(self.TOKEN_FILE):
            creds = Credentials.from_authorized_user_file(self.TOKEN_FILE, self.SCOPES)

        # Если токенов нет или они истекли
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logging.info("🔄 Обновляем токены...")
                creds.refresh(Request())
            else:
                logging.info("🌐 Открываем браузер для авторизации...")
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.OAUTH_CREDENTIALS, self.SCOPES
                )
                creds = flow.run_local_server(port=0)

            # Сохраняем токены
            with open(self.TOKEN_FILE, "w") as token:
                token.write(creds.to_json())

            logging.info("✅ Токены сохранены")

        return creds

    @retry_on_network_error()
    def create_docs_service(self):
        """Создание сервиса для работы с Google Docs"""
        docs_service = build("docs", "v1", credentials=self.credentials)
        self._services.append(docs_service)
        return docs_service

    @retry_on_network_error()
    def create_drive_service(self):
        drive_service = build("drive", "v3", credentials=self.credentials)
        self._services.append(drive_service)
        return drive_service

    @retry_on_network_error()
    def create_sheet_service(self):
        sheets_service = build("sheets", "v4", credentials=self.credentials)
        self._services.append(sheets_service)
        return sheets_service

    @retry_on_network_error()
    def create_gspread_client(self):
        return gspread.authorize(self.credentials)

    def close_all_services(self):
        """Закрыть все активные сервисы"""
        for service in self._services:
            try:
                service.close()
            except Exception as e:
                logging.warning(f"Ошибка при закрытии сервиса: {e}")
        self._services.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all_services()

    def __del__(self):
        self.close_all_services()


def export_gdoc_as_bytes(google_auth: GoogleAccountOAuth, file_id, mime_type):
    service = google_auth.create_drive_service()
    request = service.files().export_media(fileId=file_id, mimeType=mime_type)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()


def create_google_doc_from_binary(
    google_auth: GoogleAccountOAuth, binary_data: bytes, filename: str, folder_id: str
):
    drive_service = google_auth.create_drive_service()
    media = MediaIoBaseUpload(
        io.BytesIO(binary_data),
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        resumable=True,
    )
    file_metadata = {
        "name": filename,
        "mimeType": "application/vnd.google-apps.document",
        "parents": [folder_id],
    }
    file = (
        drive_service.files()
        .create(body=file_metadata, media_body=media, fields="id, webViewLink")
        .execute()
    )
    return file["id"], file["webViewLink"]


def upload_pil_image_to_drive(pil_img, folder_id, file_name):
    oauth = GoogleAccountOAuth()
    drive_service = oauth.create_drive_service()

    buf = io.BytesIO()
    pil_img.save(buf, format="PNG")
    buf.seek(0)

    file_metadata = {"name": file_name, "parents": [folder_id], "mimeType": "image/png"}

    media = MediaIoBaseUpload(buf, mimetype="image/png")
    file = (
        drive_service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )
    buf.close()
    return file.get("id")


def get_data_from_sheet(range_name: str, spreadsheet_id: str):
    # Создание экземпляра класса
    oauth = GoogleAccountOAuth()
    # Создание Sheets сервиса
    sheets_service = oauth.create_sheet_service()

    # Чтение данных
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )

    # Получение значений
    values = result.get("values", [])

    if not values:
        print("Данные не найдены")
    else:
        return values


def create_folder(service, folder_name, parent_folder_id=None, params=None):
    """Создание папки в Google Drive"""

    file_metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
    }

    # Если указана родительская папка
    if parent_folder_id:
        file_metadata["parents"] = [parent_folder_id]

    folder = service.files().create(body=file_metadata, fields="id").execute()
    logging.info(f"Папка создана с ID: {folder.get('id')}")
    folder_id = folder.get("id")

    if params is None:
        permission = {
            "type": "anyone",
            "role": "reader",
        }

        service.permissions().create(
            fileId=folder_id, body=permission, fields="id"
        ).execute()
    else:
        for user in params:
            permission = {
                "type": "user",
                "role": user.get("role", "reader"),
                "emailAddress": user["email"],
            }

            service.permissions().create(
                fileId=folder_id, body=permission, fields="id"
            ).execute()

    print(
        f"Папка доступна всем по ссылке: https://drive.google.com/drive/folders/{folder_id}"
    )
    return folder_id


def create_google_doc(
    drive_service, doc_service, doc_name, folder_id, content, permissions=None
) -> bool:
    """Создание Google документа в указанной папке с записью текста"""
    try:
        # 1. Создаем документ через Docs API
        body = {"title": doc_name}
        doc = doc_service.documents().create(body=body).execute()
        doc_id = doc.get("documentId")
        logging.info(f"✅ Документ создан: {doc_name} (ID: {doc_id})")

        # 2. Перемещаем документ в нужную папку через Drive API
        # Сначала получаем текущих родителей
        file = drive_service.files().get(fileId=doc_id, fields="parents").execute()
        previous_parents = ",".join(file.get("parents", []))

        # Перемещаем файл используя addParents и removeParents
        drive_service.files().update(
            fileId=doc_id,
            body={},  # Пустое тело
            addParents=folder_id,
            removeParents=previous_parents,
            fields="id, parents",
        ).execute()
        logging.info(f"✅ Документ перемещен в папку: {folder_id}")

        # 3. Записываем контент в документ через Docs API
        if content:
            requests = [
                {
                    "insertText": {
                        "location": {"index": 1},  # Начало документа
                        "text": content,
                    }
                }
            ]

            doc_service.documents().batchUpdate(
                documentId=doc_id, body={"requests": requests}
            ).execute()
            logging.info("✅ Контент записан в документ")

        doc_permission = {
            "type": "user",
            "role": "owner",
            "emailAddress": "aya@s3t.art",
        }

        drive_service.permissions().create(
            fileId=doc_id, body=doc_permission, transferOwnership=True
        ).execute()
        logging.info("✅ Владение передано")

        if permissions is not None:
            for users in permissions:
                user_permission = {
                    "type": "user",
                    "role": users.get("role", "reader"),
                    "emailAddress": users["email"],
                }
                drive_service.permissions().create(
                    fileId=doc_id, body=user_permission, fields="id"
                ).execute()

        logging.info(
            f"🔗 Ссылка на документ: https://docs.google.com/document/d/{doc_id}"
        )

        return True

    except Exception as error:
        logging.error(f"❌ Ошибка при создании документа: {error}")
        return False


def create_google_sheet(
    drive_service, sheet_name, folder_id, creds, data, permissions=None
) -> bool | None:
    """Создание Google таблицы в указанной папке с заполнением данными"""
    try:
        # 1. Создаем gspread клиент
        client = gspread.authorize(creds)

        # 2. Создаем таблицу через gspread
        spreadsheet = client.create(sheet_name)
        sheet_id = spreadsheet.id
        logging.info(f"✅ Таблица создана: {sheet_name} (ID: {sheet_id})")

        # Сначала получаем текущих родителей
        file = drive_service.files().get(fileId=sheet_id, fields="parents").execute()
        previous_parents = ",".join(file.get("parents", []))

        # Перемещаем файл используя addParents и removeParents
        drive_service.files().update(
            fileId=sheet_id,
            body={},  # Пустое тело
            addParents=folder_id,
            removeParents=previous_parents,
            fields="id, parents",
        ).execute()
        logging.info(f"✅ Таблица перемещена в папку: {folder_id}")

        # 4. Передаем владение пользователю
        sheet_permission = {
            "type": "user",
            "role": "owner",
            "emailAddress": "aya@s3t.art",
        }
        drive_service.permissions().create(
            fileId=sheet_id, body=sheet_permission, transferOwnership=True
        ).execute()
        logging.info("✅ Владение передано")

        if permissions is not None:
            for users in permissions:
                user_permission = {
                    "type": "user",
                    "role": users.get("role", "reader"),
                    "emailAddress": users["email"],
                }
                drive_service.permissions().create(
                    fileId=sheet_id, body=user_permission, fields="id"
                ).execute()

        logging.info("Роли розданы")

        # 5. Заполняем таблицу данными
        worksheet = spreadsheet.get_worksheet(0)
        if data:
            rows = len(data)
            cols = len(data[0]) if data else 0
            range_name = f"A1:{chr(64 + cols)}{rows}"
            worksheet.update(range_name, data)
            print(f"✅ Данные записаны в диапазон {range_name}")

        logging.info(
            f"🔗 Ссылка на таблицу: https://docs.google.com/spreadsheets/d/{sheet_id}"
        )
        return True

    except Exception as error:
        logging.error(f"❌ Ошибка при создании таблицы: {error}")
        return None


def send_to_google(service, *data, **kwargs):
    """Отправка данных в Google Sheets"""
    SPREADSHEET_ID = kwargs["data"].get("spreadsheetId")
    RANGE_NAME = kwargs["data"].get("rangeName")

    if not SPREADSHEET_ID or not RANGE_NAME:
        raise ValueError("Missing spreadsheetId or rangeName")

    def to_jsonable(x):
        if isinstance(x, (date, datetime)):
            return x.isoformat()
        if isinstance(x, Decimal):
            return float(x)
        if isinstance(x, list):
            return " ".join(map(str, x))
        return x

    actual_data = (
        data[0] if len(data) == 1 and isinstance(data[0], list) else list(data)
    )

    def _is_list_of_lists(lst):
        return isinstance(lst, list) and all(isinstance(item, list) for item in lst)

    if _is_list_of_lists(actual_data):
        insert_data = [
            [to_jsonable(elem) for elem in sublist] for sublist in actual_data
        ]
    else:
        insert_data = [to_jsonable(elem) for elem in actual_data]

    try:
        body = (
            {"values": insert_data}
            if _is_list_of_lists(actual_data)
            else {"values": [insert_data]}
        )

        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()

        logging.info(f"✅ Данные отправлены в таблицу: {SPREADSHEET_ID}")
        return True

    except Exception as error:
        logging.critical(f"Ошибка отправки в Google Sheets: {error}")
        raise Exception(f"Ошибка отправки в Google Sheets: {error}")


def clear_table(service, **kwargs) -> bool:
    """
    Очистка данных в Google Sheets
    Формат RANGE_NAME: "Sheet1!A2:Z"
    """
    SPREADSHEET_ID = kwargs["data"].get("spreadsheetId")
    RANGE_NAME = kwargs["data"].get("rangeName")

    if not SPREADSHEET_ID or not RANGE_NAME:
        raise ValueError("Missing spreadsheetId or rangeName")

    try:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            body={},
        ).execute()

        logging.info(f"✅ Таблица очищена: {SPREADSHEET_ID}, диапазон: {RANGE_NAME}")
        return True

    except Exception as error:
        logging.critical(f"Ошибка очистки таблицы: {error}")
        return False
