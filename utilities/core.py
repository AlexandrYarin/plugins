import hashlib
import logging

try:
    from postgres.core import insert_file_to_files, check_exist_file
except Exception:
    import sys
    from pathlib import Path

    project_root = Path(__file__).parents[1]
    sys.path.insert(0, str(project_root))
    from postgres.core import insert_file_to_files, check_exist_file


def _detect_file_format(data: bytes) -> dict:
    """
    Определяет формат файла и MIME-тип по магическим байтам.

    Args:
        data: Байтовая строка с содержимым файла

    Returns:
        dict: Словарь с ключами 'mime_type', 'extension', 'description'
    """

    # Словарь магических байтов (file signatures)
    signatures = [
        # Изображения
        {
            "magic": b"\x89PNG\r\n\x1a\n",
            "mime_type": "image/png",
            "extension": "png",
            "description": "PNG image",
        },
        {
            "magic": b"\xff\xd8\xff",
            "mime_type": "image/jpeg",
            "extension": "jpg",
            "description": "JPEG image",
        },
        {
            "magic": b"GIF87a",
            "mime_type": "image/gif",
            "extension": "gif",
            "description": "GIF image (87a)",
        },
        {
            "magic": b"GIF89a",
            "mime_type": "image/gif",
            "extension": "gif",
            "description": "GIF image (89a)",
        },
        # Документы
        {
            "magic": b"%PDF",
            "mime_type": "application/pdf",
            "extension": "pdf",
            "description": "PDF document",
        },
        {
            "magic": b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",
            "mime_type": "application/vnd.ms-excel",
            "extension": "xls",
            "description": "Microsoft Excel (XLS/DOC/PPT)",
        },
        # Архивы
        {
            "magic": b"\x1f\x8b\x08",
            "mime_type": "application/gzip",
            "extension": "gz",
            "description": "GZIP archive",
        },
        {
            "magic": b"Rar!\x1a\x07",
            "mime_type": "application/x-rar-compressed",
            "extension": "rar",
            "description": "RAR archive",
        },
        # Аудио/Видео
        {
            "magic": b"ID3",
            "mime_type": "audio/mpeg",
            "extension": "mp3",
            "description": "MP3 audio",
        },
        {
            "magic": b"RIFF",
            "mime_type": "audio/wav",
            "extension": "wav",
            "description": "WAV audio",
        },
    ]

    # Проверяем каждую сигнатуру
    for sig in signatures:
        if data.startswith(sig["magic"]):
            return {
                "mime_type": sig["mime_type"],
                "extension": sig["extension"],
                "description": sig["description"],
            }

    # КРИТИЧНО: Проверка Office Open XML ПЕРЕД возвратом ZIP
    if data.startswith(b"PK\x03\x04"):
        # Проверяем первые 2048 байт для надежности
        content_chunk = data[:2048]

        # XLSX содержит xl/ или workbook
        if b"xl/" in content_chunk or b"workbook" in content_chunk:
            return {
                "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "extension": "xlsx",
                "description": "Microsoft Excel (XLSX)",
            }
        # DOCX содержит word/
        elif b"word/" in content_chunk or b"document.xml" in content_chunk:
            return {
                "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "extension": "docx",
                "description": "Microsoft Word (DOCX)",
            }
        # PPTX содержит ppt/
        elif b"ppt/" in content_chunk or b"presentation" in content_chunk:
            return {
                "mime_type": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                "extension": "pptx",
                "description": "Microsoft PowerPoint (PPTX)",
            }

        # Если не Office документ, то это обычный ZIP
        return {
            "mime_type": "application/zip",
            "extension": "zip",
            "description": "ZIP archive",
        }

    # Проверка на текстовые файлы
    try:
        data[:1024].decode("utf-8")
        return {
            "mime_type": "text/plain",
            "extension": "txt",
            "description": "Plain text",
        }
    except UnicodeDecodeError:
        pass

    # Если формат не определен
    return {
        "mime_type": "application/octet-stream",
        "extension": "bin",
        "description": "Unknown binary data",
    }


def get_file_id_from_db(content: bytes, file_name: str | None, verbose=False) -> dict:
    """
    result = {
        "result": None,
        "data": {"id": None, "len": None, "extension": None, "mime_type": None},
    }
    """
    result = {"result": None, "data": {}}

    format = _detect_file_format(content)
    mime_type, extension = format["mime_type"], format["extension"]
    file_name = f"{file_name}.{extension}" if file_name else f"Table.{extension}"

    try:
        file_hash = hashlib.blake2b(content).hexdigest()
        is_exists: None | str | tuple = check_exist_file(file_hash, data=True)

        result["data"] = {
            "len": len(content),
            "extension": extension,
            "mime_type": mime_type,
        }

        if is_exists is None:
            # Добавление информации о вложении
            attachment_info = {
                "filename": file_name,
                "content_type": result["data"]["mime_type"],
                "size": result["data"]["len"],
                "hash": file_hash,
                "content": content,
            }

            file_id: int = insert_file_to_files(attachment_info)
            if file_id is None:
                raise ValueError("Не вернул file_id после insert в таблицу files")

            result["data"]["id"] = file_id
            if verbose:
                result["data"]["attachment_info"] = attachment_info

        else:
            if isinstance(is_exists, tuple) and len(is_exists) == 6:
                file_id, hash, content_type, size, content, file_name = is_exists

                attachment_info = {
                    "filename": file_name,
                    "content_type": content_type,
                    "size": size,
                    "hash": hash,
                    "content": content,
                }
                result["data"]["id"] = file_id
                if verbose:
                    result["data"]["attachment_info"] = attachment_info
            else:
                logging.error(
                    f"is_exists не является tuple или его длина != 6 is_exists: {is_exists}"
                )
                raise ValueError

        result["result"] = "success"

    except Exception as error:
        result["result"] = "error"
        result["error"] = str(error)
    finally:
        return result
