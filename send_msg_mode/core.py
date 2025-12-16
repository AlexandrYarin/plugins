import logging
from email.utils import make_msgid
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.header import Header
from email import encoders
from datetime import date, datetime
import requests
import smtplib
import yaml
import sys
from pathlib import Path

try:
    from read_pass.core import read_pass
    from postgres.core import (
        upload_file,
        get_employee_info,
    )
except Exception:
    project_root = Path(__file__).parents[1]
    sys.path.insert(0, str(project_root))
    from read_pass.core import read_pass
    from postgres.core import (
        upload_file,
        get_employee_info,
    )


logging.basicConfig(
    level=logging.DEBUG,  # Уровень логирования
    filename="logs.log",  # Имя файла для логов
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

SMTP_SERVER = "smtp.yandex.ru"
SMTP_PORT = 465


@dataclass
class YandexSendMsg:
    password: str
    template: str
    values: dict
    mandatory_attach: bool = False
    attachment_name = None
    html_body_text = None
    html_body = None
    required_lst = ["Subject", "From", "To", "Message-ID", "text/html"]

    def __post_init__(self):
        self.msg_id: str = self.values.get("msg_id", make_msgid(domain="print-1"))
        self.msg: MIMEMultipart = MIMEMultipart("mixed")
        self.subject = self.values.get("subject", None)
        self.sender = self.values.get("sender", None)
        self.receiver = self.values.get("receiver", None)

        if self.mandatory_attach:
            self.required_lst.append("attachment")

    def _prepare_body_with_format(self):
        """
        Формирует HTML с использованием str.format()

        Плейсхолдеры в шаблоне: {key} или {key:format}
        """
        try:
            # Предобработка значений
            processed_values = self._preprocess_values()
            logging.warning(processed_values)
            logging.warning(self.template)
            html_part = self.template.format(**processed_values)
            self.html_body_text = html_part
            return MIMEText(html_part, "html")

        except KeyError as e:
            logging.error(f"Отсутствует ключ в значениях: {e}")
            raise ValueError(f"Отсутствует обязательное значение: {e}")
        except Exception as e:
            logging.error(f"Ошибка форматирования: {e}")
            raise

    def _preprocess_values(self):
        """Предобработка значений для шаблона"""
        processed = self.values.copy()

        # Обработка даты
        if processed.get("date") and isinstance(processed["date"], (datetime, date)):
            processed["date_str"] = processed["date"].strftime("%d-%m-%Y")

        # Обработка сотрудника
        employee_info = processed.get("employee_info", {})
        if employee_info:
            processed.update(employee_info)

            # Условные строки
            processed["phone_line"] = (
                f"<p style='margin: 0; padding: 0; line-height: 1.2;'>Телефон: {employee_info['phone']}</p>"
                if employee_info.get("phone")
                else ""
            )
            processed["extra_line"] = (
                f"<p style='margin: 0; padding: 0; line-height: 1.2;'>{employee_info['extra_field']}</p>"
                if employee_info.get("extra_field")
                else ""
            )
            processed["post_line"] = (
                f"<p style='margin: 0; padding: 0; line-height: 1.2;'>{employee_info['post']}</p>"
                if employee_info.get("post")
                else ""
            )

        return processed

    def _embed_image_in_email(self, image_url):
        """
        Встраивает изображение в email по URL
        """
        try:
            # Скачиваем изображение
            response = requests.get(image_url, stream=True, timeout=10)
            response.raise_for_status()

            # Создаем изображение для встраивания
            image_part = MIMEImage(response.content)
            image_part.add_header("Content-ID", "<embedded_image>")
            image_part.add_header(
                "Content-Disposition", "inline", filename="bitrix_image.png"
            )

            logging.info("Изображение успешно встроено в сообщение")
            return image_part

        except requests.RequestException as e:
            logging.error(f"Ошибка при загрузке изображения: {e}")
            return False
        except Exception as e:
            logging.error(f"Ошибка при встраивании изображения: {e}")
            return False

    def _upload_file(
        self,
        dock_content,
        attachment_name="Table",
        mime_type="application/vnd.ms-excel",
    ):
        if dock_content:
            logging.debug("Файл не пустой")
            try:
                # Принудительно устанавливаем .xls расширение
                attachment_name = f"{attachment_name}.xls"

                logging.debug(f"mime_type: {mime_type.split('/')}")
                attachment = MIMEBase(*mime_type.split("/"))
                # attachment.set_payload(xls_binary_data)
                attachment.set_payload(dock_content)

                # Кодируем в base64
                encoders.encode_base64(attachment)

                # Добавляем заголовок
                try:
                    attachment_name.encode("ascii")
                    attachment.add_header(
                        "Content-Disposition", "attachment", filename=attachment_name
                    )

                except UnicodeEncodeError:
                    encoded_filename = Header(attachment_name, "utf-8").encode()
                    attachment.add_header(
                        "Content-Disposition",
                        "attachment",
                        filename=encoded_filename,
                        **{"filename*": f"utf-8''{attachment_name}"},
                    )

                logging.info("attachment готов")
                return attachment

            except Exception as e:
                logging.error(f"Ошибка при добавлении вложения: {e}")
                return False
        else:
            logging.warning("Файл не загрузился и не прикрепился к письму")
            return False

    def _check_msg_elements(self):
        check_lst = []

        for element in self.required_lst:
            if element == "attachment":
                result = [
                    part.get_filename()
                    for part in self.msg.walk()
                    if part.get("Content-Disposition", "").startswith("attachment")
                ]
                result_attach = True if result else None
                logging.info(f"result for attachment: {result_attach}")
                check_lst.append(result_attach)
            elif element == "text/html":
                html_body = any(
                    part.get_content_type() == "text/html" for part in self.msg.walk()
                )
                logging.info(f"result for html_body: {html_body}")
                check_lst.append(html_body)
            else:
                check_lst.append(self.msg.get(element, None))

        if all(elem not in [None, "", [], False] for elem in check_lst) and len(
            self.required_lst
        ) == len(check_lst):
            return True

        return False

    def building_msg(self) -> bool:
        logging.warning("Проверка заголовков")
        all_from = self.msg.get_all("From")
        if all_from and len(all_from) > 1:
            logging.error(f"ПРОБЛЕМА: Найдено {len(all_from)} заголовков From!")
            for i, f in enumerate(all_from):
                logging.error(f"  From[{i}]: {f}")
        try:
            self.msg["Subject"] = Header(self.subject, "utf-8")
            self.msg["From"] = self.sender
            self.msg["To"] = self.receiver
            self.msg["Message-ID"] = self.msg_id

            self._add_body()

            if self.html_body is None:
                logging.error("Не получилось создать html_body")
                return False
            self.msg.attach(self.html_body)
            return True

        except Exception as error:
            logging.error(f"Ошибка в _building_msg: {error}")
            return False

    def send_msg(self) -> str | bool | None:
        """
        Отправляет письмо с уникальным Message-ID
        """
        try:
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
                logging.debug(f"Подключение к SMTP серверу {SMTP_SERVER}:{SMTP_PORT}")
                smtp.login(self.sender, self.password)
                logging.debug(f"Успешная аутентификация для {self.sender}")
                smtp.send_message(self.msg)

            logging.info(f"Письмо успешно отправлено. Message-ID: {self.msg_id}")
            logging.debug(f"text: {self.html_body_text}")
            return self.html_body_text

        except smtplib.SMTPAuthenticationError as e:
            logging.error(f"Ошибка аутентификации SMTP для {self.sender}: {e}")
            logging.error("Проверьте правильность логина и пароля приложения")
            return False
        except smtplib.SMTPException as e:
            logging.error(f"SMTP ошибка при отправке письма: {e}")
            return False
        except Exception as e:
            logging.error(f"Неожиданная ошибка при отправке письма: {e}")
            return False

    def add_attach(self, type_attach, **kwargs):
        # ---------------------------------
        if type_attach == "file":
            dock_content = kwargs.get("dock_content", None)
            dock_name = kwargs.get("dock_name", None)
            if dock_content:
                if dock_name is not None:
                    attachment = self._upload_file(dock_content, dock_name)
                else:
                    attachment = self._upload_file(dock_content)
                if attachment is not False:
                    self.msg.attach(attachment)
                    logging.info("attachment add")
                    return True
                else:
                    logging.error("файл не приложился к письму")
                    return False
            else:
                logging.warning("dock_content пустой")

        # ---------------------------------
        elif type_attach == "image":
            image_url = kwargs.get("image_url", None)
            if image_url:
                content = self._embed_image_in_email(image_url)
                if content:
                    logging.warning("Проверка заголовков в image_attach")
                    all_from = self.msg.get_all("From")
                    if all_from and len(all_from) > 1:
                        logging.error(
                            f"ПРОБЛЕМА: Найдено {len(all_from)} заголовков From!"
                        )
                        for i, f in enumerate(all_from):
                            logging.error(f"  From[{i}]: {f}")

                    self.msg.attach(content)
                else:
                    logging.warning(
                        "Не удалось встроить изображение, отправляем без него"
                    )
            else:
                logging.warning("image_url пустой")
        # ---------------------------------
        else:
            logging.warning("Неверный параметр type_attach функции add_attach")
            return False

    def _add_body(self):
        html_body = self._prepare_body_with_format()

        logging.warning("Проверка заголовков в _add_body")
        all_from = self.msg.get_all("From")
        if all_from and len(all_from) > 1:
            logging.error(f"ПРОБЛЕМА: Найдено {len(all_from)} заголовков From!")
            for i, f in enumerate(all_from):
                logging.error(f"  From[{i}]: {f}")

        if html_body:
            self.html_body = html_body
        else:
            logging.error(f"html_body NONE: {html_body}")


def _get_pass(values, users_data):
    password = ""
    for manager in users_data:
        if manager["email"] == values["sender"]:
            password = manager["password"]
    if password == "":
        raise ValueError(f"Пароль для отправителя {values['sender']} не найден")

    return password


def _building_msg_data(msg) -> dict | None:
    # msg =  sender, receiver, contact_name, subject
    values_keys = [
        "sender",
        "receiver",
        "contact_name",
        "subject",
    ]
    if msg and len(msg) == len(values_keys):
        values = dict(zip(values_keys, msg))
    else:
        logging.error("Запаковка values не произошла")
        return None

    employee_info = {}
    empl_keys = ["name", "second_name", "phone", "extra_field", "post"]
    empl_values = get_employee_info(
        values["sender"]
    )  # emp_name, emp_second_name, phone, extra_field, post
    if isinstance(empl_values, tuple) and len(empl_keys) == len(empl_values):
        employee_info = dict(zip(empl_keys, empl_values))
    else:
        logging.error("Запаковка employee_info не произошла")
        return None

    values["employee_info"] = employee_info

    return values


def send_tracked_email(
    users_data, prepared_values: list, config, conf_const
) -> bool | str | None:
    dock_data = {
        "dock_name": config["DOCK_NAME_DEFAULT"],
        "dock_content": None,
        "image_url": conf_const["IMAGE_DEFAULT"],
    }

    values = _building_msg_data(prepared_values)
    if values is None:
        logging.error("Ошибка в _building_msg_data")
        return False

    msg_operator = YandexSendMsg(
        _get_pass(values, users_data), config["HTML_TEMPLATE"], values
    )
    is_msg_building = msg_operator.building_msg()
    if is_msg_building is False:
        return
    image_attach = msg_operator.add_attach("image", **dock_data)

    if image_attach is False:
        logging.warning("картинка не приложилась к письму")

    if config["DOC_ID"] is not None and is_msg_building:
        dock_id = config["DOC_ID"]

        dock_content = upload_file(dock_id)
        if dock_content is False:
            logging.error("Файл не загрузился из БД")

            return False

        dock_data["dock_content"] = dock_content

        file_attach = msg_operator.add_attach("file", **dock_data)

        if file_attach is False:
            raise ValueError("Вложение не приложилось к письму")

    is_sending = msg_operator.send_msg()
    return is_sending


def read_config(promt_type: str, new_values):
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / "send_msg_mode" / ".config.yml"

    with open(config_path, "r", encoding="utf-8") as file:
        raw_config = yaml.safe_load(file)

    config = raw_config["FORMS"][promt_type]
    if new_values is not None:
        for key, values in new_values.items():
            config[key] = values

    return config, raw_config["CONST"]


def sending_main(promt_type, values=None):
    logging.info("--Запуск процесса чтения и отправки сообщений")
    users_data = read_pass()
    if users_data is None:
        raise ValueError("Не удалось получить данные пользователей. Процесс остановлен")

    config, conf_const = read_config(promt_type, values)
    try:
        for ind, target_email in enumerate(config["TARGET_EMAILS"]):
            logging.info(f"Отправка письма {ind + 1} из {len(config['TARGET_EMAILS'])}")
            reciever_email, contanct_name = target_email
            is_sending = send_tracked_email(
                users_data,
                [config["SENDER"], reciever_email, contanct_name, config["SUBJECT"]],
                config,
                conf_const,
            )

            if is_sending is False:
                logging.error(f"Не удалось отправить сообщение {target_email}")
                continue

            if not isinstance(is_sending, str):
                logging.error(
                    f"Не удалось отправить сообщение {target_email}, html body пустое"
                )
                continue
            logging.info("Отправлено")

    except Exception as error:
        logging.error(f"Критическая ошибка в процессе чтения сообщений: {error}")
        raise error


# sending_main("PROMT_1")
