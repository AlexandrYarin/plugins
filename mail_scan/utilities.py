from email_reply_parser import EmailReplyParser
from email.header import decode_header
from bs4 import BeautifulSoup
from datetime import datetime
from pandas import read_excel
import email.utils
import hashlib
import logging
import base64
import email
import io
import re
import os
from plugins.postgres import check_exist_file, insert_file_to_files


def _decode_part_content(part, part_type):
    """Декодирование содержимого части письма"""
    try:
        payload = part.get_payload(decode=True)
        if not payload:
            return None

        # Пробуем различные кодировки
        charset = part.get_content_charset() or "utf-8"

        try:
            return payload.decode(charset, errors="ignore")
        except (UnicodeDecodeError, LookupError):
            # Если указанная кодировка не работает, пробуем стандартные
            for encoding in ["utf-8", "cp1251", "windows-1251"]:
                try:
                    return payload.decode(encoding, errors="ignore")
                except (UnicodeDecodeError, LookupError):
                    continue

            # Если ничего не помогло
            logging.warning(f"Не удалось декодировать содержимое части {part_type}")
            return payload.decode("utf-8", errors="replace")

    except Exception as e:
        logging.error(f"Ошибка декодирования части {part_type}: {e}")
        return None


def decode_filename(filename: str) -> str:
    """Декодирует имя файла из MIME-заголовка."""
    if not filename:
        return "unknown_file"

    fallback_encodings = [
        "utf-8",
        "cp1251",
        "windows-1251",
        "koi8-r",
        "iso-8859-1",
        "cp866",
        "mac_cyrillic",
        "iso-8859-5",
        "latin1",
    ]

    try:
        # ВАЖНО: заменяем переносы строк и табуляции на пробелы
        # decode_header ожидает пробелы между закодированными частями
        cleaned_filename = (
            filename.replace("\n", " ").replace("\t", " ").replace("\r", " ").strip()
        )

        decoded_parts = decode_header(cleaned_filename)
        decoded_filename = ""

        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                # Пробуем использовать указанную кодировку, затем fallback варианты
                if encoding:
                    try:
                        decoded_filename += part.decode(encoding)
                    except (UnicodeDecodeError, LookupError):
                        # Пробуем распространённые кодировки
                        for fallback_enc in fallback_encodings:
                            try:
                                decoded_filename += part.decode(fallback_enc)
                                break
                            except (UnicodeDecodeError, LookupError):
                                continue
                        else:
                            # Если ничего не подошло, используем замену ошибочных символов
                            decoded_filename += part.decode("utf-8", errors="replace")
                else:
                    # Кодировка не указана, пробуем варианты
                    for fallback_enc in fallback_encodings:
                        try:
                            decoded_filename += part.decode(fallback_enc)
                            break
                        except (UnicodeDecodeError, LookupError):
                            continue
                    else:
                        decoded_filename += part.decode("utf-8", errors="replace")
            else:
                decoded_filename += str(part)

        return decoded_filename

    except Exception as e:
        logging.warning(f"Ошибка декодирования имени файла: {e}")
        return filename


def _is_attachment(part):
    """Определяет, является ли часть письма вложением"""
    content_disposition = part.get("Content-Disposition")
    content_disposition = (
        str(content_disposition).lower() if content_disposition else ""
    )
    content_type = part.get_content_type()
    filename = part.get_filename()

    # Явное указание на вложение
    if "attachment" in content_disposition:
        return True, filename or f"attachment_{hash(part)}"

    # Есть имя файла
    if filename:
        return True, filename

    # Не текстовый тип с большим содержимым
    if (
        content_type
        and not content_type.startswith("text/")
        and not content_type.startswith("multipart/")
    ):
        payload = part.get_payload(decode=True)
        if payload and len(payload) > 100:
            return True, filename or f"attachment_{len(payload)}"

    return False, None


def _is_excel_file(file_bytes: bytes) -> tuple[bool, str]:
    """Проверяет, является ли файл Excel и определяет формат"""
    if not file_bytes or len(file_bytes) < 8:
        return False, "empty"

    # Новый формат Excel (.xlsx) - ZIP архив
    if file_bytes.startswith(b"PK\x03\x04") or file_bytes.startswith(b"PK"):
        return True, "xlsx"

    # Старый формат Excel (.xls) - OLE2 документ
    if file_bytes.startswith(b"\xd0\xcf\x11\xe0"):
        return True, "xls"

    return False, "unknown"


def _validate_excel_content(file_bytes: bytes, excel_format: str) -> bool:
    """Дополнительная проверка - пытается прочитать файл как Excel"""
    try:
        byte_stream = io.BytesIO(file_bytes)

        if excel_format == "xlsx":
            read_excel(byte_stream, nrows=1, engine="openpyxl")
        elif excel_format == "xls":
            read_excel(byte_stream, nrows=1, engine="xlrd")
        else:
            return False

        return True
    except Exception as e:
        logging.debug(f"Файл не прошел валидацию Excel: {e}")
        return False


def _process_attachment(part, filename: str) -> dict | None:
    """Обрабатывает одно вложение с проверкой на Excel формат"""
    logging.info(f"Начинаем обработку вложения: {filename}")

    try:
        decoded_filename = decode_filename(filename)
        # Быстрая проверка по расширению
        safe_filename = (
            str(datetime.now().time()).split(".")[-1] + "_" + decoded_filename
        )

        # Читаем данные файла
        payload = part.get_payload(decode=True)
        if not payload:
            logging.warning(f"Пустые данные для файла: {filename}")
            return None

        # ПРОВЕРЯЕМ ФОРМАТ ФАЙЛА
        is_excel, excel_format = _is_excel_file(payload)

        if not is_excel:
            logging.info(
                f"Файл {filename} не является Excel (формат: {excel_format}), пропускаем"
            )
            return None

        logging.info(f"Найден Excel файл {excel_format}: {filename}")

        # Дополнительная валидация - пытаемся прочитать как Excel
        if not _validate_excel_content(payload, excel_format):
            logging.warning(
                f"Файл {filename} не удалось прочитать как {excel_format}, пропускаем"
            )
            return None

        # Если все проверки пройдены, создаем данные вложения
        file_content = base64.b64encode(payload).decode("utf-8")

        attachment_data = {
            "filename": safe_filename,
            "original_filename": decoded_filename,
            "payload": payload,
            "file_content": file_content,
            "content_type": part.get_content_type(),
            "excel_format": excel_format,  # Добавляем информацию о формате
            "size": len(payload),
        }

        logging.debug(
            f"Успешно обработан Excel файл ({excel_format}): {safe_filename} ({len(payload)} байт)"
        )
        return attachment_data

    except Exception as e:
        logging.error(f"Ошибка при обработке файла {filename}: {e}")
        return None


def _decode_text_part(part) -> str:
    """Декодирует текстовую часть письма"""
    encodings = ["utf-8", "cp1251"]

    for encoding in encodings:
        try:
            decoded_text = part.get_payload(decode=True).decode(encoding)
            logging.debug(f"Успешно декодирован текст ({encoding})")
            return decoded_text
        except Exception as e:
            logging.debug(f"Ошибка декодирования {encoding}: {e}")
            continue

    # Если не удалось декодировать, возвращаем как есть
    logging.warning("Не удалось декодировать текст, используем raw данные")
    return str(part.get_payload())


def get_body_from_text_parts(text_parts: list) -> str:
    """Извлекает тело письма из текстовых частей с приоритетом plain text"""
    logging.debug("Начинаем обработку текстовых частей")

    # Сначала ищем plain text
    for text_type, part in text_parts:
        if text_type == "plain":
            body = _decode_text_part(part)
            if body:
                logging.debug("Использован plain text")
                return body

    # Если plain text не найден, используем HTML
    for text_type, part in text_parts:
        if text_type == "html":
            body = _decode_text_part(part)
            if body:
                logging.debug("Использован HTML text")
                return body

    logging.warning("Не найдено текстовых частей")
    return ""


def extract_parts_from_email(email_message) -> tuple[list, list]:
    """Извлекает текстовые части и Excel вложения из письма"""
    text_parts, attachments, skipped_files = [], [], []
    total_files = 0

    if not email_message.is_multipart():
        logging.debug("Письмо не многочастное")
        text_parts.append(("plain", email_message))
        return text_parts, attachments

    logging.debug("Письмо многочастное, обрабатываем части")

    for part in email_message.walk():
        content_type = part.get_content_type()

        is_attachment, filename = _is_attachment(part)

        if not filename:
            filename = "Входная_таблица"

        if is_attachment:
            total_files += 1
            attachment_data = _process_attachment(part, filename)

            if attachment_data:
                attachments.append(attachment_data)
                logging.info(f"✅ Добавлен Excel файл: {filename}")
            else:
                skipped_files.append(filename)
                logging.info(f"❌ Пропущен файл: {filename}")

        elif content_type == "text/plain":
            text_parts.append(("plain", part))
        elif content_type == "text/html":
            text_parts.append(("html", part))

    # Логируем статистику
    logging.info(f"Всего файлов найдено: {total_files}")
    logging.info(f"Excel файлов добавлено: {len(attachments)}")
    logging.info(f"Файлов пропущено: {len(skipped_files)}")

    if skipped_files:
        logging.info(f"Пропущенные файлы: {skipped_files}")

    return text_parts, attachments


def extract_email_body_universal(email_body) -> str:
    """
    Универсальная функция для извлечения основного содержимого письма
    Удаляет HTML теги, цитируемый текст и служебную информацию
    """
    logging.debug("Начало обработки тела письма для извлечения основного содержимого")
    # Сначала удаляем HTML теги с помощью BeautifulSoup
    try:
        soup = BeautifulSoup(email_body, "html.parser")
        # ---------------------------------
        # Заменяем HTML теги на переносы строк ПЕРЕД извлечением текста
        for tag in soup.find_all(["br", "p", "div"]):
            if tag.name == "br":
                tag.replace_with("\n")
            elif tag.name in ["p", "div"]:
                # Добавляем перенос после блочных элементов
                tag.insert_after("\n")
        # ---------------------------------
        text = soup.get_text()
    except Exception as e:
        logging.warning(f"BeautifulSoup не сработал, используем regex. Ошибка: {e}")
        # Если BeautifulSoup не работает, используем регулярные выражения
        text = re.sub(r"&lt;[^&gt;]+&gt;", "", email_body)

    # Декодируем HTML сущности
    text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").strip()

    logging.debug("Обработка тела письма завершена")
    return text


def extract_email_body_universal_mode(email_body):
    """
    Универсальная функция для извлечения основного содержимого письма
    Удаляет HTML теги, цитируемый текст и служебную информацию
    """
    logging.debug("Начало обработки тела письма для извлечения основного содержимого")
    # Сначала удаляем HTML теги с помощью BeautifulSoup
    try:
        soup = BeautifulSoup(email_body, "html.parser")
        text = soup.get_text()
    except Exception as e:
        logging.warning(f"BeautifulSoup не сработал, используем regex. Ошибка: {e}")
        # Если BeautifulSoup не работает, используем регулярные выражения
        text = re.sub(r"&lt;[^&gt;]+&gt;", "", email_body)

    # Декодируем HTML сущности
    text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    # СНАЧАЛА извлекаем подпись
    signature = extract_signature_from_text(text)

    # Удаляем цитируемый текст и служебную информацию
    lines = text.split("\n")
    filtered_lines = []
    skip_mode = False

    for line in lines:
        line = line.strip()
        # Пропускаем пустые строки
        if not line:
            continue
        # Проверяем на разделители цитат
        if re.match(r"^-{5,}$", line):
            skip_mode = True
            continue
        # Пропускаем строки с цитируемым текстом (начинающиеся с >)
        if line.startswith(">"):
            continue
        # Пропускаем строки с информацией о пересылке
        if re.match(r"^(Кому:|Тема:|От:|From:|To:|Subject:|Date:)", line):
            skip_mode = True
            continue
        # Пропускаем строки с датой и временем ответа
        if re.match(r"^\d{2}\.\d{2}\.\d{4},\s+\d{2}:\d{2}", line):
            skip_mode = True
            continue
        # Пропускаем строки типа "On ... wrote:"
        if re.match(
            r"^On\s+\w+,\s+\w+\s+\d+,\s+\d+\s+at\s+\d+:\d+\s+[AP]M.*?wrote:$", line
        ):
            skip_mode = True
            continue
        # Если мы не в режиме пропуска, добавляем строку
        if not skip_mode:
            filtered_lines.append(line)

    # Объединяем отфильтрованные строки
    result = "\n".join(filtered_lines)
    # Удаляем лишние переводы строк
    result = re.sub(r"\n{3,}", "\n\n", result)
    result = result.strip()

    logging.debug("Обработка тела письма завершена")
    return result, signature


def _is_allowed_attachment(filename, content_type) -> bool:
    """Проверка допустимости вложения по типу и расширению"""
    ALLOWED_EXTENSIONS = [".pdf", ".doc", ".docx", ".xls", ".xlsx"]
    ALLOWED_MIME_TYPES = [
        "application/pdf",
        "application/msword",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.ms-word.document.macroEnabled.12",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel.sheet.macroEnabled.12",
        "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
        "application/octet-stream",
    ]

    # Проверка по MIME-типу
    if content_type in ALLOWED_MIME_TYPES:
        if content_type == "application/octet-stream" and filename:
            ext = os.path.splitext(filename.lower())[1]
            if ext in ALLOWED_EXTENSIONS:
                return True
        else:
            return True

    return False


def update_db_file(file_id, file_data) -> int:
    if file_id is None:
        new_file_id: int = insert_file_to_files(file_data)
        return new_file_id
    return file_id


def _check_file_name(filename: str, reserv: list) -> str:
    if "�" in filename:
        logging.warning(
            "Ошибка декодирования: имя файла содержит некорректные символы."
        )

        ext_file = filename.split(".")[-1] if "." in filename else "txt"
        for elem in reserv:
            if elem and type(elem) is str:
                return f"{elem[:40]}.{ext_file}"
        return f"unknown.{ext_file}"

    else:
        return filename


def _return_body_mail(email_message, reserv_fn: list) -> dict:
    """Извлечение тела письма и подсчет вложений"""
    logging.debug("Начало извлечения тела письма")

    plain_text_body = None
    html_body = None
    attachments_list = []

    if email_message.is_multipart():
        logging.debug("Письмо многочастное, начинаем обработку частей")

        part_count = 0
        for part in email_message.walk():
            part_count += 1
            content_type = part.get_content_type()
            content_disposition = part.get("Content-Disposition", "")
            content_disposition = (
                str(content_disposition).lower() if content_disposition else ""
            )

            logging.debug(f"Обработка части {part_count}: content_type={content_type}")

            # Проверка на вложение
            is_attachment = (
                "attachment" in content_disposition
                or part.get_filename()
                or (
                    content_type
                    and not content_type.startswith("text/")
                    and not content_type.startswith("multipart/")
                    and part.get_payload(decode=True)
                    and len(part.get_payload(decode=True)) > 100
                )
            )

            if is_attachment:
                filename = decode_filename(part.get_filename())
                filename = _check_file_name(filename, reserv_fn)
                logging.debug(f"filename: {filename}")
                # Фильтрация по типу файла
                if not _is_allowed_attachment(filename, content_type):
                    continue

                # Извлечение данных вложения
                attachment_data = part.get_payload(decode=True)
                if attachment_data:
                    # Вычисление хэша
                    file_hash = hashlib.blake2b(attachment_data).hexdigest()

                    raw_file_id = check_exist_file(file_hash)

                    # Получение информации о файле
                    file_type = content_type
                    file_size = len(attachment_data)

                    # Добавление информации о вложении
                    attachment_info = {
                        "filename": filename,
                        "content_type": file_type,
                        "size": file_size,
                        "hash": file_hash,
                        "content": attachment_data,
                    }

                    file_id = update_db_file(raw_file_id, attachment_info)
                    logging.warning(f"file_id: {file_id}")
                    attachment_info["file_id"] = file_id

                    attachments_list.append(attachment_info)

                    logging.info(
                        f"Найдено вложение: {filename}, тип: {file_type}, "
                        f"размер: {file_size} байт, blake2b: {file_hash[:16]}..."
                    )

                continue

            # Обработка текстовых частей
            if content_type == "text/plain" and not plain_text_body:
                plain_text_body = _decode_part_content(part, "plain")
                logging.debug("Найдена и обработана текстовая часть (plain)")

            elif content_type == "text/html" and not html_body:
                html_body = _decode_part_content(part, "html")
                logging.debug("Найдена и обработана текстовая часть (html)")

        logging.info(f"Обработано частей письма: {part_count}")
        logging.info(f"Найдено вложений: {len(attachments_list)}")

        # Приоритет text/plain над text/html
        body = plain_text_body or html_body

    else:
        logging.debug("Письмо не многочастное, обрабатываем как единое целое")
        body = _decode_part_content(email_message, "simple")

    logging.info("---Начинаем извлечение основного содержимого")
    final_body, signature = extract_email_body_universal_mode(body)
    logging.info("---Завершена обработка письма")

    return {
        "body": final_body,
        "signature": signature,
        "attachments": len(attachments_list),
        "attachments_data": attachments_list,
    }


# ----------------------------------------------------------------


def _parse_date(date_header):
    """Парсинг даты отправки письма"""
    if not date_header:
        return None

    try:
        parsed_date = email.utils.parsedate_tz(date_header)
        if parsed_date:
            timestamp = email.utils.mktime_tz(parsed_date)
            return datetime.fromtimestamp(timestamp)
    except Exception:
        pass

    return None


def convert_subject(msg_subject: str) -> str:
    new_subject = (
        msg_subject.replace("Re:", "")
        .replace("Fwd:", "")
        .replace("RE:", "")
        .replace("FWD:", "")
        .strip()
    )

    if len(new_subject) > 500:
        new_subject = new_subject[:500]
    return new_subject


def convert_email(email: str) -> str:
    new_email = ""
    pattern = re.compile(r"[\w\.-]+@[\w\.-]+")
    result = pattern.search(email)
    if result:
        new_email = result.group(0)
    if new_email == "":
        raise ValueError("Не найден email")
    return new_email


def convert_receivers(msg) -> list:
    receivers = []
    for header in ["To", "CC", "BCC"]:
        receiver = decode_mime_words(msg.get(header, ""))
        if receiver:
            receiver = convert_email(receiver)
            receivers.append(receiver)
    if len(receivers) == 0:
        raise ValueError("Не может в поле получателей быть ноль получателей")

    return receivers


def convert_reference(msg) -> list | None:
    refs = decode_mime_words(msg.get("References", ""))
    if refs is not None:
        result_refs = list([ref.strip() for ref in refs.split(" ")])
        return result_refs
    return []


def decode_mime_words(s):
    """Декодирование MIME-заголовков"""
    if s is None:
        return ""

    parts = decode_header(s)
    decoded_fragments = []

    for part, encoding in parts:
        if isinstance(part, bytes):
            try:
                decoded_fragments.append(
                    part.decode(encoding or "utf-8", errors="ignore")
                )
            except Exception:
                decoded_fragments.append(part.decode("utf-8", errors="ignore"))
        else:
            decoded_fragments.append(str(part))

    return "".join(decoded_fragments)


def _is_valid_signature(signature_text):
    """
    Проверяет, является ли текст валидной подписью
    """
    # Убираем начальные "--"
    clean_signature = signature_text.lstrip("-").strip()

    # Проверяем длину (разумные пределы для подписи)
    if len(clean_signature) < 10 or len(clean_signature) > 500:
        logging.warning(
            f"Подпись не валидна, размер подписи не корректен: {clean_signature}"
        )
        return False

    # Проверяем, что это не содержит признаки переписки
    conversation_keywords = [
        "кому:",
        "от:",
        "тема:",
        "wrote:",
        "отправлено:",
        "переслано:",
        "re:",
        "fwd:",
    ]

    if any(keyword in clean_signature.lower() for keyword in conversation_keywords):
        logging.warning(
            f"Подпись не валидна, лишние слова в подписи: {clean_signature}"
        )
        return False

    # Проверяем количество строк (подпись обычно не очень длинная)
    lines = clean_signature.split("\n")
    if len(lines) > 10:  # Слишком много строк для подписи
        logging.warning(
            f"Подпись не валидна,слишком много строчек для подписи: {clean_signature}"
        )
        return False

    return True


def extract_signature_from_text(text: str) -> str | None:
    """
    Извлекает подпись из текста письма.
    Ищет разделитель '-- ' и валидирует наличие CHECKER URL.
    """
    CHECKER = "https://str-art.ru"

    if not text:
        logging.warning("Текст пустой")
        return None

    # Ищем позицию разделителя '-- ' в тексте
    separator_pos = text.find(" -- ")

    if separator_pos == -1:
        # Пытаемся найти только '--'
        separator_pos = text.find("--")
        if separator_pos == -1:
            logging.warning("Разделитель подписи '-- ' не найден")
            return None
    else:
        # Если нашли ' -- ', начинаем с позиции после пробела
        separator_pos += 1

    # Берем текст от разделителя и далее
    potential_signature = text[separator_pos:]

    # Проверяем наличие CHECKER
    checker_pos = potential_signature.find(CHECKER)
    if checker_pos == -1:
        logging.warning(f"CHECKER '{CHECKER}' не найден в подписи")
        return None

    # Конец подписи = позиция после CHECKER
    checker_end = checker_pos + len(CHECKER)

    # Извлекаем подпись от разделителя до конца CHECKER
    signature_text = potential_signature[:checker_end]
    signature_text = signature_text.lstrip("-- ").strip()

    # Валидация
    if _is_valid_signature(signature_text):
        return signature_text
    else:
        logging.warning("Подпись не прошла валидацию")
        return None


def extract_last_message(text):
    """
    Простейший подход - ищем первый "--" как начало подписи/разделителя
    """
    # Ищем первое вхождение "--"
    separator_pos = text.find("--")

    clean_text = ""
    if separator_pos != -1:
        # Берем все до первого "--"
        clean_text = text[:separator_pos].strip()
    else:
        clean_text = text.strip()

    # Извлекаем подпись

    result = EmailReplyParser.parse_reply(clean_text)
    if len(result) == 0:
        return "Пустое сообщение"

    return result


def parse_email_message(msg_data, date_filter):
    """Извлечение всех нужных полей из msg_data полученного по IMAP"""

    # msg_data[0][1] содержит сырые байты email-сообщения
    raw_email = msg_data[0][1]
    msg = email.message_from_bytes(raw_email)
    msg_date = _parse_date(msg.get("Date"))
    if date_filter is not None:
        if msg_date < date_filter:
            logging.warning(
                f"Это пиьсмо уже сканировали. msg_date: {msg_date}, date_filter: {date_filter}"
            )
            return

    subject = convert_subject(decode_mime_words(msg.get("Subject", "")))
    receivers = convert_receivers(msg)
    convert_from = convert_email(decode_mime_words(msg.get("From", "")))
    reference = convert_reference(msg)

    msg_content: dict = _return_body_mail(msg, [subject, convert_from])
    (text, signature, attachments_val, attachments_data) = (
        msg_content["body"],
        msg_content["signature"],
        msg_content["attachments"],
        msg_content["attachments_data"],
    )

    convert_text = extract_last_message(text)

    email_data = {
        "message_id": decode_mime_words(msg.get("Message-ID", "")),
        "in_reply_to": decode_mime_words(msg.get("In-Reply-To", "")),
        "references": reference,
        "from": convert_from,
        "to": receivers,
        "date_sent": _parse_date(msg.get("Date")),
        "attachments": attachments_val,
        "attachments_data": [file_data["file_id"] for file_data in attachments_data]
        if attachments_data
        else [],
        "subject": subject,
        "text_body": convert_text,
        "signature": signature,
    }

    return email_data
