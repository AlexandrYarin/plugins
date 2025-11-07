from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import make_msgid
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.header import Header
from email import encoders
from datetime import date, datetime
import requests
import logging
import smtplib

logging.basicConfig(
    level=logging.DEBUG,  # Уровень логирования
    filename="logs.log",  # Имя файла для логов
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)


# Серверы Яндекса
SMTP_SERVER = "smtp.yandex.ru"
SMTP_PORT = 465


class YandexSendMsg:
    def __init__(
        self,
        password,
        template,
        values,
        mandatory_attach=True,
    ) -> None:
        self.msg = MIMEMultipart("mixed")
        self.subject = values.get("subject", None)
        self.sender = values.get("sender", None)
        self.receiver = values.get("receiver", None)
        self.msg_id = values.get("msg_id", None)
        self.password = password
        self.template = template
        self.values = values
        self.required_lst = ["Subject", "From", "To", "Message-ID", "text/html"]
        self.attachment_name = None
        self.html_body_text = None
        self.html_body = None

        if self.msg_id is None:
            self.msg_id = make_msgid(domain="print-1")

        if mandatory_attach:
            self.required_lst.append("attachment")

    def _prepare_body_with_format(self):
        """
        Формирует HTML с использованием str.format()

        Плейсхолдеры в шаблоне: {key} или {key:format}
        """
        try:
            # Предобработка значений
            processed_values = self._preprocess_values()
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
        else:
            pass
            # TODO: отработать else

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
                # Конвертируем в xls независимо от исходного формата
                # xls_binary_data = self._convert_to_xls(dock_content)
                # if xls_binary_data is None:
                #     return False

                # Принудительно устанавливаем .xls расширение
                attachment_name = f"{attachment_name}.xls"

                print(f"mime_type: {mime_type.split('/')}")
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
            print(f"text: {self.html_body_text}")
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
        if html_body:
            self.html_body = html_body
        else:
            logging.error(f"html_body NONE: {html_body}")
