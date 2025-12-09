import psycopg
from psycopg import Error, OperationalError, InterfaceError

import yaml
import logging
import os


TMP_PATH = os.path.join(os.path.dirname(__file__), "tmp")


class PstgCursor:
    def __init__(self, retries=2) -> None:
        config_path = os.path.join(os.path.dirname(__file__), ".config.yaml")
        with open(config_path, "r", encoding="utf-8") as file:
            self.config = yaml.safe_load(file)

        self.conn = psycopg.connect(**self.config["postgres"])
        self.cursor = self.conn.cursor()
        self.retries = retries

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logging.warning(f"Ошибка: {exc_type.__name__}: {exc_val} : {exc_tb}")
        self.close_connection()

    def close_connection(self) -> None:
        self.cursor.close()
        self.conn.close()
        logging.debug("PostgreSQL connection closed")

    def reconnect(self):
        if self.conn:
            self.conn.close()
        self.conn = psycopg.connect(**self.config["postgres"])
        self.cursor = self.conn.cursor()
        logging.debug("PostgreSQL reconnected")

    def copy_expert(self, sql_copy_command, file):
        """
        Совместимый метод для psycopg3, эмулирующий copy_expert из psycopg2.
        """
        attempt = 0
        while attempt <= self.retries:
            try:
                # В psycopg3 используется метод copy()
                with self.cursor.copy(sql_copy_command) as copy:
                    # Читаем весь файл или блоками
                    while True:
                        data = file.read(8192)  # Читаем блоками по 8KB
                        if not data:
                            break
                        copy.write(data)

                logging.debug("Запись успешно добавлена в таблицу deals")
                return
            except (OperationalError, InterfaceError) as e:
                logging.warning(
                    f"DB error: {e}, retrying ({attempt + 1}/{self.retries})"
                )
                # Сбрасываем позицию файла перед повтором
                file.seek(0)
                self.reconnect()
                attempt += 1
            except Exception as e:
                logging.error(f"Query failed: {e}")
                self.conn.rollback()
                raise
        raise Exception("Failed to execute query after retries")

    # def copy_expert(self, sql_copy_command, file):
    #     attempt = 0
    #     while attempt <= self.retries:
    #         try:
    #             self.cursor.copy_expert(sql_copy_command, file)
    #             logging.debug("Запись успешно добавлена в таблицу deals")
    #             return
    #         except (OperationalError, InterfaceError) as e:
    #             logging.warning(
    #                 f"DB error: {e}, retrying ({attempt + 1}/{self.retries})"
    #             )
    #             self.reconnect()
    #             attempt += 1
    #         except Exception as e:
    #             logging.error(f"Query failed: {e}")
    #             self.conn.rollback()
    #             raise
    #     raise Exception("Failed to execute query after retries")

    def execute(self, query, params=None, autocommit=False):
        attempt = 0
        while attempt <= self.retries:
            try:
                self.cursor.execute(query, params)
                if autocommit:
                    self.conn.commit()
                return self.cursor
            except (OperationalError, InterfaceError) as e:
                logging.warning(
                    f"DB error: {e}, retrying ({attempt + 1}/{self.retries})"
                )
                self.reconnect()
                attempt += 1
            except Exception as e:
                logging.error(f"Query failed: {e}")
                self.conn.rollback()
                raise
        raise Exception("Failed to execute query after retries")

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()


def insert_bitrix_deals_mode(data):
    """
    Функция для подключения к PostgreSQL и вставки одной записи в таблицу messages.
    """
    query = """
        INSERT INTO deals_document (deal_id, files)
        VALUES (%s, %s)
    """
    try:
        with PstgCursor() as db:
            db.cursor.executemany(query, data)
            db.commit()

    except Exception as e:
        logging.exception(f"Ошибка при работе с PostgreSQL: {e}")
        raise e


def insert_bitrix_deals():
    """
    Функция для подключения к PostgreSQL и вставки записей из CSV в таблицу deals.
    """
    csv_file_path = f"{TMP_PATH}/bitrix_deals.csv"
    table_name = "deals"
    columns_to_insert = (
        "deal_id",
        "deal_title",
        "type_deal",
        "type_nmn",
        "who_created",
        "created_ts",
        "deadline",
        "dock_id",
        "regions",
    )
    sql_copy_command = f"""
        COPY {table_name} ({", ".join(columns_to_insert)}) 
        FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ';')
    """
    try:
        with PstgCursor() as db:
            with open(csv_file_path, "r", encoding="utf-8") as file:
                # Используем новый метод copy() вместо copy_expert()
                with db.cursor.copy(sql_copy_command) as copy:
                    # Читаем файл блоками или целиком
                    copy.write(file.read())
            db.commit()

    except Exception as e:
        logging.exception(f"Ошибка при работе с PostgreSQL: {e}")
        raise e


# XXX: DEPRECATED
def insert_file(unique_file_id, deal_id, filetype, document, msg_id):
    """
    Функция для вставки файла в таблицу docs в PostgreSQL.
    """
    query = """
        INSERT INTO docs (id,  deal_id, filetype, document, msg_id) 
        VALUES (%s, %s, %s, %s, %s)
        """

    try:
        with PstgCursor() as db:
            db.execute(
                query,
                (unique_file_id, deal_id, filetype, document, msg_id),
                autocommit=True,
            )
            logging.info("Файл запсиан в БД")
    except Exception:
        logging.critical("Ошибка в записи файла")
        raise


def upload_file_mode(file_id):
    query = """
            SELECT content 
            FROM files
            WHERE id=%s
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, (file_id,))
            result = result.fetchone()
            result = bytes(result[0]) if result else False
            logging.debug("Файл извлечен из БД")
            return result
    except Exception:
        logging.critical("error in upload_file")
        raise


# XXX: DEPRECATED
#
# def check_dock_id(doc_id):
#     """
#     Проверяет наличие записи с заданным file_id в таблице files.
#     Args:
#         doc_id: Идентификатор файла для поиска в базе данных.
#     Returns:
#         True: если запись с таким file_id отсутствует в базе данных.
#         False: если запись с таким file_id существует в базе данных.
#     """
#     query = """
#             SELECT * FROM docs
#             WHERE file_id = %s
#             """
#     try:
#         with PstgCursor() as db:
#             result = db.execute(query, (doc_id,))
#             rows = result.fetchone()
#             if rows is None:
#                 logging.debug("Файла нет в БД")
#                 return True
#             else:
#                 logging.debug("Файл существует в БД")
#                 return False
#     except Exception:
#         logging.error("Ошибка при работе с PostgreSQL в ставке файла")
#         raise


def update_table_msgs_send(msg_id, html_body) -> bool:
    """
    Обновляет таблицу 'msgs', отмечая сообщение как отправленное
    args:msg_id (int): Идентификатор сообщения для обновления.
    """
    query_send = """
                UPDATE msgs
                SET ts_send = NOW(), is_send = true, html_body = %s
                WHERE msg_id = %s
                """
    db = PstgCursor()
    try:
        db.execute(
            query_send,
            (
                html_body,
                msg_id,
            ),
            autocommit=True,
        )
        return True
    except Exception as e:
        logging.error(f"Не удалось обновить таблицу отправленных писем в БД, error{e}")
        db.rollback()
        raise


def update_table_msgs_reply(msg_id, body=None, file_id=None) -> None:
    query_to_msgs = """
                    UPDATE msgs 
                    SET is_answered = true, ts_answer = NOW(), body_answer = %s, dock_id = %s
                    WHERE msg_id = %s
                    """
    if body is None:
        body = "Пустое сообщение"
    else:
        body = body[:1000]

    try:
        with PstgCursor() as db:
            db.execute(
                query_to_msgs,
                (
                    body,
                    file_id,
                    msg_id,
                ),
                autocommit=True,
            )
            logging.info("Запрос выполнен")
    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL в работе с msgs")
        raise error


def read_mails_from_db(param="send_read", *args):
    query_types = {
        "send_read": """
                    SELECT m.msg_id, m.sender, m.receiver, m.contact_name, m.dock_id, m.deadline, d.deal_title
                    FROM msgs as m
                    join deals as d
                        on m.deal_id = d.deal_id
                    WHERE m.is_send = false
                        AND d.is_closed = false
                    """,
        "answer_read": """
                        SELECT msg_id, sender, ts_send, m.deal_id
                        FROM msgs AS m
                        JOIN deals AS d
                            ON m.deal_id = d.deal_id
                        WHERE is_answered = false
                            AND ts_send IS NOT NULL 
                            AND d.is_closed = false
                        """,
        "resend_email": """
                        SELECT contact_name, deadline
                        FROM msgs
                        WHERE id = %s
                        """,
        "check_resend_email": """
                        SELECT m.msg_id, m.sender, m.receiver, m.contact_name, m.dock_id, m.deadline, d.deal_title 
                        FROM msgs as m
                        JOIN deals as d
                            ON m.deal_id = d.deal_id
                        WHERE is_answered = false
                        AND d.is_closed = false
                        AND resend = false
                        AND m.deadline::timestamp > ts_send
                        AND NOW() >= ts_send + (m.deadline::timestamp - ts_send) / 2;
                            """,
    }

    """
    текущая дата >= дата отправки + (дата дэдлайна - дата отправки) / 2
    """

    query = query_types[param]
    try:
        with PstgCursor() as db:
            if args:
                result = db.execute(query, args)
            else:
                result = db.execute(query)

            result = result.fetchall()
            logging.info("Запрос выполнен")
            return result

    except (Exception, Error) as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_dock_ids(deal_id):
    query = """
            SELECT msg_id, doc_id
            FROM rpls 
            WHERE is_answered = true
                AND doc_id IS NOT NUll 
                AND msg_id IN (select msg_id from msgs where deal_id = %s)
            """

    try:
        with PstgCursor() as db:
            result = db.execute(query, (deal_id,))
            ids = result.fetchall()
            logging.info("ЗАпрос на поиск документов выполнен")
            return ids

    except (Exception, Error) as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def insert_new_company(**data: list) -> bool:
    query = """
            INSERT INTO cmps (cmp_id, cmp_name, cmp_types, cmp_nmn, contact_name, contact_email, regions) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cmp_id) DO NOTHING
                """
    if len(data) == 6:
        cmp_id, cmp_name, cmp_types, cmp_nmn, contact_name, contact_email = (
            data.values()
        )
        regions = ["Без региона"]
    elif len(data) == 7:
        cmp_id, cmp_name, cmp_types, cmp_nmn, contact_name, contact_email, regions = (
            data.values()
        )
    else:
        raise ValueError(
            "Data must contain exactly 6 or 7 elements: cmp_id, cmp_name, cmp_types, cmp_nmn, contact_name, contact_email, *regions"
        )

    if type(cmp_types) is list and type(cmp_nmn) is list and type(regions) is list:
        cmp_types = "{" + ",".join(str(elem) for elem in cmp_types) + "}"
        cmp_nmn = "{" + ",".join(str(elem) for elem in cmp_nmn) + "}"
        regions = "{" + ",".join(str(elem) for elem in regions) + "}"

    try:
        with PstgCursor() as db:
            db.execute(
                query,
                (
                    cmp_id,
                    cmp_name,
                    cmp_types,
                    cmp_nmn,
                    contact_name,
                    contact_email,
                    regions,
                ),
                autocommit=True,
            )
            logging.info("Вставка новой компании")
            return True

    except (Exception, Error) as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_company_info(target_search: str) -> list:
    query = """
            SELECT cmp_id, contact_email
            FROM cmps 
            WHERE cmp_types = %s
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, (target_search,))
            data = result.fetchall()
            logging.info("Запрос на поиск данных о компании завершен")
            return data

    except (Exception, Error) as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_companies(ids: list):
    query = """
            SELECT cmp_id, contact_email 
            FROM cmps
            WHERE cmp_id IN %s
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, (ids))
            ids = result.fetchall()
            logging.info("Зaпрос на поиск документов выполнен")
            return ids

    except (Exception, Error) as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_deals_ids(table_name="deals") -> list | bool:
    query = """
            SELECT deal_id 
            FROM {table_name}
            """
    query = query.format(table_name=table_name)

    try:
        with PstgCursor() as db:
            result = db.execute(query)
            ids = result.fetchall()
            logging.info("ЗАпрос на поиск всех id сделок")
            return list([id_cmp[0] for id_cmp in ids])

    except (Exception, Error) as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        return False


def find_contractors(cmp_types: list, cmp_nmn: list, regions=None) -> list | bool:
    query = """
        SELECT cmp_id, contact_name, contact_email
        FROM cmps
        WHERE cmp_types && %s AND cmp_nmn && %s
        """
    query_with_regions = """
        SELECT cmp_id, contact_name, contact_email
        FROM cmps
            WHERE cmp_types && %s
                AND cmp_nmn && %s
                AND regions && %s
        """

    try:
        with PstgCursor() as db:
            if regions is None or regions == ["Без региона"] or regions == []:
                result = db.execute(query, (cmp_types, cmp_nmn))
            else:
                result = db.execute(query_with_regions, (cmp_types, cmp_nmn, regions))

            if result.rowcount > 0:
                contractor_data = result.fetchall()
                return contractor_data
            else:
                print("check")
                return False

    except (Exception, Error) as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def for_google() -> list | bool:
    query = """
            SELECT d.deal_title, d.type_deal, d.type_nmn, d.deadline,  c.cmp_name, c.contact_name, c.contact_email, d.regions
            FROM msgs AS m
            JOIN deals AS d
                ON d.deal_id = m.deal_id
            JOIN cmps AS c
                on c.cmp_id = m.company_id
        """

    try:
        with PstgCursor() as db:
            result = db.execute(query)

            if result.rowcount > 0:
                data_for_google = result.fetchall()
                return data_for_google
            else:
                return False

    except (Exception, Error) as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def create_msgs(*data):
    query = """
            INSERT INTO msgs (msg_id, deal_id, sender, company_id, receiver, contact_name, dock_id, deadline)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (msg_id) DO NOTHING
            """

    if len(data) != 8:
        raise ValueError("Не хватает данных для создания сообщения")
    try:
        with PstgCursor() as db:
            _ = db.execute(query, tuple(data), autocommit=True)
            return True

    except Exception as error:
        logging.exception("Ошибка при работе с PostgreSQL:", error)
        raise


def get_reply_files_mode(deal_id) -> list | None:
    query = """
            SELECT c.cmp_name, f.id, f.content
            FROM files AS f
            JOIN msgs AS m ON f.id = m.dock_id
            JOIN cmps AS c ON m.company_id = c.cmp_id
            WHERE m.deal_id = %s AND m.is_answered = true
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, (deal_id,))
            if result.rowcount > 0:
                data_dock = result.fetchall()
                return data_dock
            return

    except Exception as error:
        logging.exception("Ошибка при работе с PostgreSQL:", error)
        return


def get_reply_text(deal_id) -> list | None:
    query = """
            SELECT c.cmp_name, m.body_answer
            FROM msgs as m
            JOIN cmps as c ON m.company_id = c.cmp_id
            WHERE m.deal_id = %s AND m.is_answered = true AND body_answer is not null
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, (deal_id,))
            if result.rowcount > 0:
                data_dock = result.fetchall()
                return data_dock
            return

    except Exception as error:
        logging.exception("Ошибка при работе с PostgreSQL:", error)
        return


def get_company_max_id() -> int | bool | None:
    query = """
            SELECT MAX(cmp_id) FROM cmps
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query)
            max_id = result.fetchone()
            logging.info("Запрос на поиск максимального id компаний")
            if len(max_id) != 0:
                return max_id[0]
            else:
                return None

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_company_ids_and_modearate() -> list | None:
    query = """
            SELECT cmp_id, date_modify FROM cmps
            ORDER BY cmp_id
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query)
            ids = result.fetchall()
            logging.info("Запрос на все id и дату последнего редактирования")
            if ids:
                return ids
            else:
                return None

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def update_or_insert_company(operation="insert", **data):
    query_update = """
            UPDATE cmps
            SET cmp_name = %s,
                cmp_types = %s,
                cmp_nmn = %s,
                contact_name = %s, 
                contact_email = %s,
                regions = %s,
                date_modify = %s
            WHERE cmp_id = %s
                """

    query_insert = """
            INSERT INTO cmps (cmp_id, cmp_name, cmp_types, cmp_nmn, contact_name, contact_email, regions, date_modify) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cmp_id) DO NOTHING
                """
    if len(data) != 8:
        raise ValueError(
            "Data must contain exactly 8 elements: cmp_id, cmp_name, cmp_types, cmp_nmn, contact_name, contact_email, date_modify, *regions"
        )
    (
        cmp_id,
        cmp_name,
        cmp_types,
        cmp_nmn,
        contact_name,
        contact_email,
        regions,
        date_modify,
    ) = data.values()

    try:
        with PstgCursor() as db:
            if operation == "insert":
                db.execute(
                    query_insert,
                    (
                        cmp_id,
                        cmp_name,
                        cmp_types,
                        cmp_nmn,
                        contact_name,
                        contact_email,
                        regions,
                        date_modify,
                    ),
                    autocommit=True,
                )
                logging.info("Запрос на вставку компании прошел успешно")
                return True
            else:
                db.execute(
                    query_update,
                    (
                        cmp_name,
                        cmp_types,
                        cmp_nmn,
                        contact_name,
                        contact_email,
                        regions,
                        date_modify,
                        cmp_id,
                    ),
                    autocommit=True,
                )
                logging.info("Запрос на обновление компании прошел успешно")
                return True

    except (Exception, Error) as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_info_ready_deal(deal_id: int) -> bool:
    """Возвращает true, если сделка готова"""
    query = """
            SELECT deal_id 
            FROM msgs
            WHERE deal_id = %s
            GROUP BY deal_id
            HAVING BOOL_AND(is_answered) = true;
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, (deal_id,))
            if result.rowcount == 0:
                return False
            if result.fetchone()[0] == deal_id:
                logging.info("Все документы собраны")
                return True
        return False

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_active_deals() -> list | None:
    query = """
            SELECT deal_id, deal_title, who_created FROM deals
            WHERE is_closed = false
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query)
            if result.rowcount > 0:
                active_deals = result.fetchall()
                logging.info("Запрос на наличие активных сделок исполнен")
                return [deal for deal in active_deals]
            else:
                return None

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def update_deal(deal_id: int):
    query = """
            UPDATE deals SET is_closed = true
            WHERE deal_id = %s
            """
    try:
        with PstgCursor() as db:
            _ = db.execute(query, (deal_id,), autocommit=True)
            logging.info(f"Сделка {deal_id} обновлена(закрыта)")

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_hot_deals(deal_ids: list, deadline_interval: int) -> list | None:
    query = """
            SELECT deal_id, deal_title, who_created FROM deals
            WHERE deal_id IN %s
                AND  (NOW() > (deadline - INTERVAL '%s hours'))
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, (tuple(deal_ids), deadline_interval))
            if result.rowcount > 0:
                logging.info("Сделка {deal_id} обновлена(закрыта)")
                return result.fetchall()
            return None

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def read_empl_passwords():
    query = """
        SELECT email, pass_email FROM employees
        WHERE is_active = true
            and pass_email is not null
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


def rollup_deal(deal_id):
    # Удаление сделки если произошла ошибка
    query = """
            DELETE FROM TABLE deals
            WHERE deal_id = %s
            """
    try:
        with PstgCursor() as db:
            db.execute(query, (deal_id,), autocommit=True)

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def update_resend_msg(msg_id, resend_html_body) -> bool:
    # Удаление сделки если произошла ошибка
    query = """
            UPDATE msgs
            SET resend = true, html_body=%s
            WHERE msg_id = %s
            """
    try:
        with PstgCursor() as db:
            db.execute(
                query,
                (
                    resend_html_body,
                    msg_id,
                ),
                autocommit=True,
            )
            return True

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_employee_info(email) -> tuple | None:
    # Возвращает информацию сотрудника
    query = """
            SELECT emp_name, emp_second_name, phone, extra_field, post
            FROM employees
            WHERE email = %s
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, (email,))
            employee_info = result.fetchone()
            if employee_info:
                return employee_info

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def insert_new_msgs(data):
    query = """
            INSERT INTO orders.msgs_order (msg_id, reply_to, reference, sender, receiver, msg_time, value_attach, files, subject, body, signature, folder)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

    try:
        with PstgCursor() as db:
            db.cursor.executemany(query, data)
            db.commit()
            logging.info("Строчки запсиан в БД")
    except Exception:
        logging.critical("Ошибка в записи строчек")
        raise


def insert_msg_metadata(*metadata: list):
    query = """
            INSERT INTO orders.order_book (manager, scan_ts, value_msg)
            VALUES (%s, %s, %s)
            """
    try:
        with PstgCursor() as db:
            _ = db.execute(query, metadata)
            db.commit()
            logging.info("Новые метаданные добавлены в БД")

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_last_scan_stamp(manager_email):
    query = """
            SELECT MAX(scan_ts) FROM orders.order_book
            WHERE manager = %s
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, (manager_email,))
            ts = result.fetchone()[0]
            logging.info("TIMESTAMP извлечен из БД")
            return ts

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_manager_tread(manager_email, subject):
    query = """
            WITH start_msg AS (
                SELECT msg_id 
                FROM orders.msgs_order
                WHERE reference = ARRAY[''] 
                AND (sender = %s OR %s = ANY(receiver)) 
                AND subject = %s
            ),
            ranked_msgs AS (
                SELECT body, sender, msg_time,
                    ROW_NUMBER() OVER (PARTITION BY body, sender ORDER BY msg_time) as rn
                FROM orders.msgs_order
                WHERE msg_id IN (SELECT msg_id FROM start_msg)
                OR EXISTS (
                    SELECT 1 FROM start_msg 
                    WHERE start_msg.msg_id = ANY(orders.msgs_order.reference)
                )
            )
            SELECT body, sender
            FROM ranked_msgs
            WHERE rn = 1
            ORDER BY msg_time;
            """
    try:
        with PstgCursor() as db:
            result = db.execute(
                query,
                (
                    manager_email,
                    manager_email,
                    subject,
                ),
            )
            tread = result.fetchall()
            return tread

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_click_deals_for_btx_mode():
    query = """
            SELECT deal_title, region, deadline, type_nmn, files
            FROM orders.threeclick
            WHERE send_btx = false
                AND gemini_see = true
                AND deal_type = 'Опубликована новая закупка'
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query)
            click_deals = result.fetchall()

            if len(click_deals) > 0:
                return click_deals
            else:
                return None

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def get_click_deals_for_btx():
    query = """
            SELECT deal_title, region, deadline, type_nmn, files
            FROM orders.threeclick
            WHERE send_btx = false
                AND ready_for_btx = true
                AND deal_type = 'Опубликована новая закупка'
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query)
            click_deals = result.fetchall()

            if len(click_deals) > 0:
                return click_deals
            else:
                return None

    except Exception as error:
        logging.error("Ошибка при работе с PostgreSQL:", error)
        raise


def update_click_deal_after_btx(deal_title):
    query = """
            UPDATE orders.threeclick SET
                send_btx = true
            WHERE deal_title = %s
                AND deal_type = 'Опубликована новая закупка'
            """
    try:
        with PstgCursor() as db:
            _ = db.execute(query, (deal_title,), autocommit=True)
            return True

    except Exception:
        logging.critical("Ошибка в записи строчек")
        raise


def check_exist_file(hash_file, data=False) -> None | str | tuple:
    query = """
            SELECT id FROM files
            WHERE hash_blake2b = %s;
            """
    if data:
        query = """
                SELECT id, hash_blake2b, content_type, size, content, file_name
                FROM files
                WHERE hash_blake2b = %s;
                """

    try:
        with PstgCursor() as db:
            result = db.execute(query, (hash_file,))
            data_db = result.fetchone()

            if data:
                return data_db
            return data_db[0] if data_db else None

    except Exception:
        logging.critical("Ошибка в записи строчек")
        raise


def insert_file_to_files(file_data):
    # "filename": filename,
    # "content_type": file_type,
    # "size": file_size,
    # "hash": file_hash,
    # "content": content

    query = """
            INSERT INTO files (file_name, content_type, size, hash_blake2b, content) VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, list(file_data.values()), autocommit=True)
            file_id = result.fetchone()

            return file_id[0] if file_id else None

    except Exception:
        logging.critical("Ошибка в записи строчек")
        raise


def get_file_content(ids: list):
    query_to_msgs = """
                SELECT content, file_name FROM files
                WHERE id = ANY(%s) AND file_name LIKE '%%.xls%%' 
                    """
    try:
        with PstgCursor() as db:
            result = db.execute(query_to_msgs, [ids])
            if result.rowcount > 0:
                excel_file = result.fetchone()
                logging.warning(f"result: {excel_file}")

                return excel_file

    except Exception as error:
        raise error


def insert_new_graph(data):
    query_to_msgs = """
                INSERT INTO graphs (object_name, task_name, data, original_size)
                VALUES (%s, %s, %s, %s)
                    """
    try:
        with PstgCursor() as db:
            _ = db.execute(query_to_msgs, tuple(data), autocommit=True)
            return True

    except Exception as error:
        raise error


def get_count_stat_msgs_check():
    """Подсчет кол-ва новых проверок писем"""
    query = """
            SELECT
                DATE(check_ts) AS date,
                COUNT(*) AS count_records
            FROM ai_msg_check
            WHERE check_ts >= CURRENT_DATE - INTERVAL '10 days'
            GROUP BY DATE(check_ts)
            ORDER BY date;
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query)
            if result.rowcount > 0:
                stat_msg = result.fetchall()
                return stat_msg
            else:
                return

    except Exception as error:
        raise error


def upload_file(file_id):
    query = """
            SELECT document 
            FROM docs
            WHERE id=%s
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query, (file_id,))
            result = result.fetchone()
            result = bytes(result[0]) if result else False
            logging.debug("Файл извлечен из БД")

            return result

    except Exception:
        logging.critical("error in upload_file")
        raise


def get_count_stat_msgs():
    """Подсчет кол-ва новых записей в БД"""
    query = """
            SELECT
                DATE(scan_ts) AS date,
                SUM(value_msg) AS sum_values
            FROM orders.order_book
            WHERE scan_ts >= CURRENT_DATE - INTERVAL '10 days'
            GROUP BY DATE(scan_ts)
            ORDER BY date;
            """
    try:
        with PstgCursor() as db:
            result = db.execute(query)
            if result.rowcount > 0:
                stat_msg = result.fetchall()
                return stat_msg
            else:
                raise ValueError("Ошибка при запросе статистики")

    except Exception as error:
        raise error
