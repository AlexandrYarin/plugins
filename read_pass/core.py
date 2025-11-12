from cryptography.fernet import Fernet
import base64
import csv
import logging
import hashlib
import os

try:
    from postgres.core import read_empl_passwords, write_new_employees, PstgCursor
    from google_auth.core import GoogleAccountOAuth
except Exception:
    import sys
    from pathlib import Path

    project_root = Path(__file__).parents[1]
    sys.path.insert(0, str(project_root))
    from postgres.core import PstgCursor
    from google_auth.core import GoogleAccountOAuth

TMP_PATH = os.path.join(os.path.dirname(__file__), "tmp")
FILE_PATH_CSV = f"{TMP_PATH}/employees_pass.csv"
# DOC_ID = "1LJFWnYzLsQShbzUwn4i9r1AtIWIrKy2PFc1nEz9E7Qo"
DOC_ID = "104SDVFmdYO07T0zhpbJTjNZk-hu1TXBpoHzpRpDPwRY"


def _read_pass_from_nowere():
    oauth = GoogleAccountOAuth()
    service = oauth.create_docs_service()
    doc = service.documents().get(documentId=DOC_ID).execute()
    content = []

    def read_elements(elements):
        for el in elements:
            if "paragraph" in el:
                for elem in el["paragraph"]["elements"]:
                    text_run = elem.get("textRun")
                    if text_run:
                        content.append(text_run.get("content"))

    read_elements(doc.get("body", {}).get("content", []))

    return "".join(content)


def _generate_key_from_phrase(phrase):
    """
    Генерирует ключ для Fernet из заготовленной фразы
    """

    # Создаем хеш из фразы и берем первые 32 байта для ключа
    try:
        hash_object = hashlib.sha256(phrase.encode())
        key = base64.urlsafe_b64encode(hash_object.digest())
        return key
    except Exception:
        logging.error("Ключ не сгенирировался")
        return None


def _decrypt_password_fernet(encrypted_password, phrase):
    """
    Дешифрует пароль
    """
    try:
        key = _generate_key_from_phrase(phrase)
        if key is None:
            raise ValueError
        fernet = Fernet(key)
        decrypted_password = fernet.decrypt(encrypted_password.encode())
        return decrypted_password.decode()
    except Exception as e:
        return f"Ошибка дешифровки: {e}"


def read_pass_site() -> dict | None:
    def read_usrs_passwords():
        query = """
            SELECT username, pswd FROM site.users
            """
        try:
            with PstgCursor() as db:
                result = db.execute(query)
                if result.rowcount > 0:
                    employees_data = result.fetchall()
                    return employees_data
                else:
                    raise ValueError("Ошибка при чтении паролей")

        except Exception as error:
            logging.error("Ошибка при работе с PostgreSQL:", error)
            raise

    data_users = {}
    phrase = _read_pass_from_nowere()
    data_mails_from_db = read_usrs_passwords()

    for login, password in data_mails_from_db:
        data_users[login] = _decrypt_password_fernet(password, phrase)
    if data_users != {}:
        logging.debug("данные успешно загружены")
        return data_users
    else:
        logging.warning("данные не найдены")
        return None


def read_pass(manager_email=None) -> list | None | dict:
    data_mails = []
    phrase = _read_pass_from_nowere()
    data_mails_from_db = read_empl_passwords()

    for email, password in data_mails_from_db:
        if manager_email and email == manager_email:
            return {
                "email": email,
                "password": _decrypt_password_fernet(password, phrase),
            }

        data_mails.append(
            {"email": email, "password": _decrypt_password_fernet(password, phrase)}
        )
    if data_mails != {}:
        logging.warning("Почтовые данные успешно загружены")
        return data_mails
    else:
        logging.warning("Почтовые данные не найдены")
        return None


def encrypt_password_fernet(password):
    """
    Шифрует пароль
    """
    key = _generate_key_from_phrase(_read_pass_from_nowere())
    if key is None:
        raise ValueError
    fernet = Fernet(key)

    # Шифруем пароль
    encrypted_password = fernet.encrypt(password.encode())
    return encrypted_password.decode()


def add_new_employees_to_db():
    """
    Обработка CSV с использованием Fernet шифрования
    """
    try:
        with open(FILE_PATH_CSV, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=";")

            # Сохраняем заголовки для последующей записи
            headers = reader.fieldnames
            # Обрабатываем каждую строку
            empl_data = []
            for row in reader:
                email = row["email"]
                password = row["password"]
                encrypted_password = encrypt_password_fernet(password)
                empl_data.append(tuple([email, encrypted_password]))
            write_new_employees(empl_data)

        # Обнуляем файл, оставляя только заголовки
        with open(FILE_PATH_CSV, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=headers, delimiter=";")
            writer.writeheader()

    except FileNotFoundError:
        logging.error(f"Файл '{FILE_PATH_CSV}' не найден")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
