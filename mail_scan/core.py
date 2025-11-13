import ssl
import shlex
import imaplib
import re
import time
import socket
from datetime import datetime
import logging

try:
    from mail_scan.utilities import parse_email_message
except ModuleNotFoundError:
    from .utilities import parse_email_message

IMAP_SERVER = "imap.yandex.ru"
PORT = 993
SKIP_FOLDERS = ["Drafts", "Drafts|template", "Spam", "Trash", "Archive"]


def decode_imap_folder_name(encoded_name):
    """Декодирует имя папки IMAP из модифицированной UTF-7"""
    # Заменяем символы согласно IMAP UTF-7
    decoded = encoded_name.replace("&-", "\x00")  # временная замена
    decoded = decoded.replace("&", "+")
    decoded = decoded.replace("\x00", "&")  # возвращаем &
    decoded = decoded.replace(",", "/")

    try:
        # Декодируем UTF-7
        result = decoded.encode("ascii").decode("utf-7")
        return result
    except Exception:
        return encoded_name


class YandexMailScanner:
    def __init__(self, account: dict, last_date, last_timestamp):
        self.email = account["email"]
        self.password = account["password"]
        self.imap_client = None
        self.last_date = last_date
        self.last_timestamp = last_timestamp

        # Настройки надежности
        self.max_retries = 3
        self.retry_delay = 2  # секунды
        self.connection_timeout = 120
        self.last_activity = None
        self.keepalive_interval = 300  # 5 минут

    def connect_to_account(self):
        """Подключение с повышенной надежностью"""
        for attempt in range(self.max_retries):
            try:
                # Закрываем старое соединение если есть
                if self.imap_client:
                    self._safe_close()

                # Настройка SSL контекста
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE  # Для проблемных серверов

                # Устанавливаем таймаут для сокета
                socket.setdefaulttimeout(self.connection_timeout)

                # Подключаемся к серверу
                self.imap_client = imaplib.IMAP4_SSL(
                    host=IMAP_SERVER,
                    port=PORT,
                    ssl_context=ssl_context,
                    timeout=self.connection_timeout,
                )

                # Авторизация
                login_response = self.imap_client.login(self.email, self.password)

                if login_response[0] == "OK":
                    self.last_activity = time.time()
                    logging.info(f"Успешное подключение: {self.email}")
                    return True
                else:
                    logging.error(f"Ошибка авторизации: {login_response}")

            except (
                imaplib.IMAP4.abort,
                socket.timeout,
                socket.gaierror,
                ConnectionResetError,
                OSError,
            ) as e:
                logging.warning(
                    f"Сетевая ошибка подключения (попытка {attempt + 1}): {e}"
                )

            except Exception as e:
                logging.error(f"Неожиданная ошибка подключения: {e}")

            # Пауза перед повторной попыткой
            if attempt < self.max_retries - 1:
                wait_time = self.retry_delay * (attempt + 2)  # Увеличиваем задержку
                logging.info(f"Повтор через {wait_time} секунд...")
                time.sleep(wait_time)

        logging.error(
            f"Не удалось подключиться к {self.email} после {self.max_retries} попыток"
        )
        return False

    def _check_connection(self):
        """Проверка состояния соединения"""
        try:
            if not self.imap_client:
                return False

            # Проверяем keep-alive
            current_time = time.time()
            if (
                self.last_activity
                and current_time - self.last_activity > self.keepalive_interval
            ):
                self.imap_client.noop()  # Keep-alive команда
                self.last_activity = current_time

            return True

        except (imaplib.IMAP4.abort, OSError, AttributeError):
            logging.warning("Соединение потеряно, требуется переподключение")
            return False

    def _reconnect_if_needed(self):
        """Автоматическое переподключение при необходимости"""
        if not self._check_connection():
            logging.info("Переподключаемся...")
            return self.connect_to_account()
        return True

    def _safe_operation(self, operation_func, *args, **kwargs):
        """Безопасное выполнение IMAP операций с автоматическим переподключением"""
        for attempt in range(self.max_retries):
            try:
                # Проверяем/восстанавливаем соединение
                if not self._reconnect_if_needed():
                    continue

                # Выполняем операцию
                result = operation_func(*args, **kwargs)
                self.last_activity = time.time()
                return result
            except (
                imaplib.IMAP4.abort,
                socket.timeout,
                socket.error,
                ConnectionResetError,
                BrokenPipeError,
                OSError,
            ) as e:
                logging.warning(f"IMAP операция прервана (попытка {attempt + 1}): {e}")
                # ОБЯЗАТЕЛЬНО закрываем битое соединение
                self._safe_close()
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))  # Увеличиваем задержку
                else:
                    raise

            # except imaplib.IMAP4.abort as e:
            #     logging.warning(f"IMAP операция прервана (попытка {attempt + 1}): {e}")
            #     if attempt < self.max_retries - 1:
            #         time.sleep(self.retry_delay)
            #     else:
            #         raise

            except Exception as e:
                logging.error(f"Ошибка в IMAP операции: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

        raise Exception("Превышено количество попыток выполнения операции")

    def _safe_close(self):
        """Безопасное закрытие соединения"""
        if self.imap_client:
            try:
                self.imap_client.logout()
            except (imaplib.IMAP4.abort, OSError, AttributeError):
                pass  # Соединение уже закрыто
            finally:
                self.imap_client = None

    def close_connection(self):
        """Публичный метод для закрытия соединения"""
        logging.info("Закрываем IMAP соединение")
        self._safe_close()

    def get_folders_list(self) -> list:
        """Получение списка папок с обработкой ошибок"""

        def _get_folders():
            if not self.imap_client:
                raise ValueError("IMAP клиент не подключен")

            status, list_response = self.imap_client.list()
            if status != "OK":
                raise Exception(f"Ошибка получения списка папок: {status}")

            folders = []
            for folder_line in list_response:
                if folder_line is None:
                    continue

                try:
                    folder_str = folder_line.decode("UTF-8")
                    folder_name = self._parse_folder_line(folder_str)
                    if folder_name and folder_name not in SKIP_FOLDERS:
                        folders.append(folder_name)
                except Exception as e:
                    logging.warning(f"Ошибка обработки папки: {e}")

            return folders

        try:
            return self._safe_operation(_get_folders)
        except Exception as e:
            logging.error(f"Критическая ошибка получения папок: {e}")
            return []

    def select_folder(self, folder_name):
        """Выбор папки с надежной обработкой"""

        def _select_folder():
            if not self.imap_client:
                raise ValueError("IMAP клиент не подключен")

            quoted_folder_name = f'"{folder_name}"'
            normal_name = decode_imap_folder_name(folder_name)

            status, data = self.imap_client.select(quoted_folder_name)

            if status == "OK":
                logging.info(f"Выбрана папка: {normal_name}")
                return True
            else:
                raise Exception(f"Ошибка выбора папки {folder_name}: {data}")

        try:
            return self._safe_operation(_select_folder)
        except Exception as e:
            logging.error(f"Не удалось выбрать папку {folder_name}: {e}")
            return False

    def _fetch_message(self, num):
        """Получение сообщения с обработкой ошибок"""

        def _fetch():
            if self.imap_client is None:
                raise ValueError("IMAP клиент не подключен")

            # Используем BODY.PEEK[] чтобы не менять флаги прочитанности
            status, msg_data = self.imap_client.fetch(num, "(BODY.PEEK[] FLAGS)")

            if status != "OK":
                raise Exception(f"Ошибка получения сообщения {num}")

            return msg_data

        return self._safe_operation(_fetch)

    def scan_messages(self, folders_list, search_criteria=None) -> list:
        """Сканирование сообщений"""
        if search_criteria is None:
            search_criteria = f'SINCE "{self.last_date}"'
        search_criteria = 'SINCE "25-Mar-2025" BEFORE "26-Jun-2025"'
        # logging.info(f"Поиск писем от {self.last_date}")
        logging.info(f"Поиск писем от {search_criteria}")
        print(f"Поиск писем от {search_criteria}")

        emails, scan_end_stamp = [], None

        for folder in folders_list:
            # NOTE: Верменная хрень
            if folder != "INBOX":
                continue
            try:
                if not self.select_folder(folder):
                    continue

                # Поиск сообщений
                def _search_messages():
                    if self.imap_client is None:
                        return
                    status, message_ids = self.imap_client.search(None, search_criteria)
                    if status != "OK":
                        raise Exception(f"Ошибка поиска в папке {folder}")
                    return message_ids

                message_ids = self._safe_operation(_search_messages)

                if message_ids is None:
                    continue
                if not message_ids[0]:
                    logging.info(f"В папке '{folder}' новых сообщений не найдено")
                    continue

                message_list = message_ids[0].split()
                logging.info(
                    f"Найдено {len(message_list)} сообщений в папке '{folder}'"
                )

                # Обработка сообщений
                lengh_sms = len(message_list)
                for i, num in enumerate(message_list):
                    print(f"SMS {i + 1}/{lengh_sms}")
                    if i == 1001:
                        break
                    try:
                        msg_data = self._safe_operation(self._fetch_message, num)

                        if msg_data and msg_data[0] and isinstance(msg_data[1], bytes):
                            try:
                                raw_email = msg_data[1]
                                # Логируем первые 200 символов письма
                                email_preview = raw_email.decode(
                                    "utf-8", errors="ignore"
                                )[:200]
                                logging.info(
                                    f"Сообщение {num} preview: {email_preview}"
                                )

                                email_info = parse_email_message(
                                    msg_data, self.last_timestamp
                                )
                                if email_info:
                                    email_info["folder"] = folder
                                    emails.append(list(email_info.values()))
                                else:
                                    logging.warning(
                                        f"Сообщение {num}: parse_email_message вернул None"
                                    )

                            except UnicodeDecodeError:
                                logging.error(f"Сообщение {num}: ошибка декодирования")
                                continue

                        else:
                            logging.warning(f"Неверные данные сообщения {num}")

                    except Exception as e:
                        logging.error(f"Ошибка обработки сообщения {num}: {e}")
                        continue

            except Exception as e:
                logging.error(f"Ошибка сканирования папки {folder}: {e}")
                continue

        scan_end_stamp = datetime.now()
        return [emails, scan_end_stamp]

    def _parse_folder_line(self, folder_line) -> str | None:
        """Парсинг строки с информацией о папке (без изменений)"""
        try:
            match = re.match(r'\(([^)]*)\)\s+"([^"]*)"\s+(.+)', folder_line)
            if match:
                folder_name = match.group(3)
                if folder_name.startswith('"') and folder_name.endswith('"'):
                    folder_name = folder_name[1:-1]
                return folder_name
            else:
                parts = shlex.split(folder_line)
                if len(parts) >= 3:
                    return parts[-1]
        except Exception as e:
            logging.warning(f"Ошибка парсинга строки '{folder_line}': {e}")
        return None

    def __enter__(self):
        """Поддержка контекстного менеджера"""
        if self.connect_to_account():
            return self
        raise Exception("Не удалось подключиться к IMAP серверу")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Автоматическое закрытие соединения"""
        self.close_connection()
