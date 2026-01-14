"""
Модуль для оперативного исследования данных Битрикс24.

Примеры использования:
    from bitrix.explorer import Companies, Deals, Contacts, Leads

    # Найти поставщиков и вывести таблицу
    Companies.find(type="Поставщик").show()

    # Найти сделки и сохранить в Excel
    Deals.find(company=12345).to_excel("deals.xlsx")

    # Контакты компании
    Contacts.of_company(12345).show()
"""

from __future__ import annotations
from typing import Any, Iterator
import pandas as pd
from tabulate import tabulate

# Поддержка импорта изнутри папки и снаружи
try:
    from .core import query_to_bitrix, get_all_pages, BITRIX_DATA
except ImportError:
    from core import query_to_bitrix, get_all_pages, BITRIX_DATA


# Кэш для справочников
_cache: dict[str, Any] = {}


def _get_cached(key: str, loader):
    """Получить значение из кэша или загрузить."""
    if key not in _cache:
        _cache[key] = loader()
    return _cache[key]


def clear_cache():
    """Очистить кэш справочников."""
    _cache.clear()


class ResultSet:
    """
    Результат запроса с методами вывода и экспорта.

    Поддерживает цепочку методов:
        Companies.find(type="Поставщик").show()
        Deals.find(amount_gt=100000).to_excel("deals.xlsx")
    """

    def __init__(self, data: list[dict], entity_name: str = "items"):
        self.data = data
        self.entity_name = entity_name

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> Iterator[dict]:
        return iter(self.data)

    def __getitem__(self, index) -> dict | list[dict]:
        return self.data[index]

    def __repr__(self) -> str:
        return f"<ResultSet: {len(self.data)} {self.entity_name}>"

    def to_df(self, columns: list[str] | None = None) -> pd.DataFrame:
        """Преобразовать в pandas DataFrame."""
        df = pd.DataFrame(self.data)
        if columns and len(df) > 0:
            existing_cols = [c for c in columns if c in df.columns]
            df = df[existing_cols]
        return df

    def show(self, columns: list[str] | None = None, limit: int = 50) -> ResultSet:
        """
        Вывести таблицу в терминал.

        Args:
            columns: Список колонок для отображения (None = все)
            limit: Максимум строк (по умолчанию 50)

        Returns:
            self для цепочки методов
        """
        if not self.data:
            print(f"Нет данных ({self.entity_name})")
            return self

        df = self.to_df(columns)
        if limit and len(df) > limit:
            df = df.head(limit)
            print(f"Показаны первые {limit} из {len(self.data)} записей\n")

        print(tabulate(df, headers="keys", tablefmt="rounded_grid", showindex=False))
        print(f"\nВсего: {len(self.data)} {self.entity_name}")
        return self

    def to_excel(self, filename: str, columns: list[str] | None = None) -> ResultSet:
        """
        Сохранить в Excel файл.

        Args:
            filename: Имя файла (добавит .xlsx если нет)
            columns: Список колонок для сохранения

        Returns:
            self для цепочки методов
        """
        if not filename.endswith(".xlsx"):
            filename += ".xlsx"

        df = self.to_df(columns)
        df.to_excel(filename, index=False, engine="openpyxl")
        print(f"Сохранено {len(self.data)} записей в {filename}")
        return self

    def to_csv(self, filename: str, columns: list[str] | None = None) -> ResultSet:
        """
        Сохранить в CSV файл.

        Args:
            filename: Имя файла (добавит .csv если нет)
            columns: Список колонок для сохранения

        Returns:
            self для цепочки методов
        """
        if not filename.endswith(".csv"):
            filename += ".csv"

        df = self.to_df(columns)
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"Сохранено {len(self.data)} записей в {filename}")
        return self

    def first(self) -> dict | None:
        """Вернуть первый элемент или None."""
        return self.data[0] if self.data else None

    def filter(self, **kwargs) -> ResultSet:
        """
        Дополнительная фильтрация в памяти.

        Args:
            **kwargs: field=value для точного совпадения
                      field__contains=value для поиска подстроки

        Returns:
            Новый ResultSet с отфильтрованными данными
        """
        filtered = self.data

        for key, value in kwargs.items():
            if "__contains" in key:
                field = key.replace("__contains", "")
                filtered = [
                    item for item in filtered
                    if field in item and value.lower() in str(item[field]).lower()
                ]
            else:
                filtered = [
                    item for item in filtered
                    if key in item and item[key] == value
                ]

        return ResultSet(filtered, self.entity_name)

    def ids(self) -> list[str]:
        """Получить список ID."""
        return [item.get("ID") for item in self.data if item.get("ID")]


class BitrixEntity:
    """Базовый класс для сущностей Битрикс."""

    ENTITY_TYPE: str = ""
    LIST_METHOD: str = ""
    GET_METHOD: str = ""
    FIELDS_METHOD: str = ""
    ENTITY_NAME: str = "items"

    # Маппинг удобных имён на поля Битрикс
    FILTER_ALIASES: dict[str, str] = {}

    @classmethod
    def _build_filter(cls, **kwargs) -> dict:
        """Построить фильтр для API из kwargs."""
        bitrix_filter = {}

        for key, value in kwargs.items():
            if value is None:
                continue

            # Проверяем алиасы
            if key in cls.FILTER_ALIASES:
                bitrix_key = cls.FILTER_ALIASES[key]
                # Обработка операторов (>, <, >=, <=, %)
                if bitrix_key.startswith(("%", ">", "<")):
                    prefix = ""
                    while bitrix_key and bitrix_key[0] in "%><=":
                        prefix += bitrix_key[0]
                        bitrix_key = bitrix_key[1:]
                    bitrix_filter[prefix + bitrix_key] = value
                else:
                    bitrix_filter[bitrix_key] = value
            else:
                # Прямой фильтр Битрикс
                bitrix_filter[key] = value

        return bitrix_filter

    @classmethod
    def find(cls, limit: int | None = None, select: list[str] | None = None, **filters) -> ResultSet:
        """
        Найти сущности по фильтрам.

        Args:
            limit: Ограничение количества (None = все)
            select: Список полей для выборки
            **filters: Фильтры (используйте алиасы или прямые поля Битрикс)

        Returns:
            ResultSet с результатами
        """
        if not cls.LIST_METHOD:
            raise NotImplementedError(f"LIST_METHOD не определён для {cls.__name__}")

        params = {}
        bitrix_filter = cls._build_filter(**filters)
        if bitrix_filter:
            params["filter"] = bitrix_filter
        if select:
            params["select"] = select

        if limit:
            params["start"] = 0
            # Для ограниченного запроса используем обычный запрос
            result = query_to_bitrix(cls.LIST_METHOD, **params)
            data = result[:limit] if result else []
        else:
            # Получаем все страницы
            data = get_all_pages(cls.LIST_METHOD, params) or []

        return ResultSet(data, cls.ENTITY_NAME)

    @classmethod
    def get(cls, entity_id: int | str) -> dict | None:
        """
        Получить сущность по ID.

        Args:
            entity_id: ID сущности

        Returns:
            Словарь с данными или None
        """
        if not cls.GET_METHOD:
            raise NotImplementedError(f"GET_METHOD не определён для {cls.__name__}")

        return query_to_bitrix(cls.GET_METHOD, id=entity_id)

    @classmethod
    def fields(cls) -> dict:
        """Получить описание всех полей сущности."""
        if not cls.FIELDS_METHOD:
            raise NotImplementedError(f"FIELDS_METHOD не определён для {cls.__name__}")

        return query_to_bitrix(cls.FIELDS_METHOD) or {}

    @classmethod
    def count(cls, **filters) -> int:
        """Подсчитать количество сущностей по фильтру."""
        bitrix_filter = cls._build_filter(**filters)
        params = {"filter": bitrix_filter} if bitrix_filter else {}
        result = query_to_bitrix(cls.LIST_METHOD, raw_result=True, **params)
        return result.get("total", 0) if result else 0


class Companies(BitrixEntity):
    """
    Работа с компаниями.

    Примеры:
        Companies.find(type="Поставщик").show()
        Companies.find(title="ООО").to_excel("companies.xlsx")
        Companies.get(12345)
        Companies.types()
    """

    ENTITY_TYPE = "company"
    LIST_METHOD = "get_all_companies"
    GET_METHOD = "company_info"
    FIELDS_METHOD = "get_company_fields"
    ENTITY_NAME = "компаний"

    FILTER_ALIASES = {
        "type": "COMPANY_TYPE",
        "region": BITRIX_DATA.get("FIELD_COMPANY_REGION", "UF_CRM_1756212422"),
        "title": "%TITLE",
        "industry": "INDUSTRY",
    }

    @classmethod
    def types(cls) -> dict[str, str]:
        """
        Получить словарь типов компаний {название: id}.

        Returns:
            Словарь типов
        """
        def load_types():
            statuses = query_to_bitrix("get_status_list", filter={"ENTITY_ID": "COMPANY_TYPE"})
            if not statuses:
                return {}
            return {s["NAME"]: s["STATUS_ID"] for s in statuses}

        return _get_cached("company_types", load_types)

    @classmethod
    def find(cls, type: str | None = None, region: str | None = None,
             title: str | None = None, limit: int | None = None,
             select: list[str] | None = None, **raw_filters) -> ResultSet:
        """
        Найти компании.

        Args:
            type: Тип компании (название, например "Поставщик")
            region: ID региона
            title: Поиск по названию (частичное совпадение)
            limit: Ограничение количества
            select: Поля для выборки
            **raw_filters: Прямые фильтры Битрикс

        Returns:
            ResultSet с компаниями
        """
        filters = raw_filters.copy()

        # Преобразуем название типа в ID
        if type:
            types_map = cls.types()
            type_id = types_map.get(type, type)
            filters["type"] = type_id

        if region:
            filters["region"] = region
        if title:
            filters["title"] = title

        return super().find(limit=limit, select=select, **filters)

    @classmethod
    def get(cls, company_id: int | str, with_contacts: bool = False,
            with_deals: bool = False) -> dict | None:
        """
        Получить компанию по ID.

        Args:
            company_id: ID компании
            with_contacts: Загрузить контакты компании
            with_deals: Загрузить сделки компании

        Returns:
            Словарь с данными компании
        """
        company = super().get(company_id)
        if not company:
            return None

        if with_contacts:
            company["_contacts"] = Contacts.of_company(company_id).data

        if with_deals:
            company["_deals"] = Deals.find(company=company_id).data

        return company


class Deals(BitrixEntity):
    """
    Работа со сделками.

    Примеры:
        Deals.find(company=12345).show()
        Deals.find(amount_gt=100000).to_excel("big_deals.xlsx")
        Deals.stages()
    """

    ENTITY_TYPE = "deal"
    LIST_METHOD = "deal_list"
    GET_METHOD = "deal_info"
    FIELDS_METHOD = "get_deal_fields"
    ENTITY_NAME = "сделок"

    FILTER_ALIASES = {
        "status": "STAGE_ID",
        "stage": "STAGE_ID",
        "company": "COMPANY_ID",
        "company_id": "COMPANY_ID",
        "contact": "CONTACT_ID",
        "contact_id": "CONTACT_ID",
        "responsible": "ASSIGNED_BY_ID",
        "amount_gt": ">OPPORTUNITY",
        "amount_lt": "<OPPORTUNITY",
        "amount_gte": ">=OPPORTUNITY",
        "amount_lte": "<=OPPORTUNITY",
        "created_after": ">=DATE_CREATE",
        "created_before": "<=DATE_CREATE",
        "title": "%TITLE",
    }

    @classmethod
    def stages(cls, category_id: int | str = 0) -> dict[str, str]:
        """
        Получить словарь стадий сделок {название: id}.

        Args:
            category_id: ID воронки (0 = основная)

        Returns:
            Словарь стадий
        """
        def load_stages():
            statuses = query_to_bitrix(
                "get_status_list",
                filter={"ENTITY_ID": f"DEAL_STAGE_{category_id}" if category_id else "DEAL_STAGE"}
            )
            if not statuses:
                # Попробуем без суффикса
                statuses = query_to_bitrix(
                    "get_status_list",
                    filter={"ENTITY_ID": "DEAL_STAGE"}
                )
            if not statuses:
                return {}
            return {s["NAME"]: s["STATUS_ID"] for s in statuses}

        return _get_cached(f"deal_stages_{category_id}", load_stages)

    @classmethod
    def find(cls, status: str | None = None, company: int | str | None = None,
             amount_gt: float | None = None, amount_lt: float | None = None,
             limit: int | None = None, select: list[str] | None = None,
             **raw_filters) -> ResultSet:
        """
        Найти сделки.

        Args:
            status: Стадия сделки (название или ID)
            company: ID компании
            amount_gt: Сумма больше чем
            amount_lt: Сумма меньше чем
            limit: Ограничение количества
            select: Поля для выборки
            **raw_filters: Прямые фильтры Битрикс

        Returns:
            ResultSet со сделками
        """
        filters = raw_filters.copy()

        # Преобразуем название стадии в ID
        if status:
            stages_map = cls.stages()
            stage_id = stages_map.get(status, status)
            filters["status"] = stage_id

        if company:
            filters["company"] = company
        if amount_gt is not None:
            filters["amount_gt"] = amount_gt
        if amount_lt is not None:
            filters["amount_lt"] = amount_lt

        return super().find(limit=limit, select=select, **filters)


class Contacts(BitrixEntity):
    """
    Работа с контактами.

    Примеры:
        Contacts.find(name="Иван").show()
        Contacts.of_company(12345).show()
    """

    ENTITY_TYPE = "contact"
    LIST_METHOD = "contact_list"
    GET_METHOD = "get_contact_info"
    FIELDS_METHOD = "contact_fields"
    ENTITY_NAME = "контактов"

    FILTER_ALIASES = {
        "name": "%NAME",
        "company": "COMPANY_ID",
        "company_id": "COMPANY_ID",
        "email": "EMAIL",
        "phone": "PHONE",
        "responsible": "ASSIGNED_BY_ID",
    }

    @classmethod
    def of_company(cls, company_id: int | str) -> ResultSet:
        """
        Получить контакты компании.

        Args:
            company_id: ID компании

        Returns:
            ResultSet с контактами
        """
        # Используем специальный метод для контактов компании
        result = query_to_bitrix("get_contacts", id=company_id)
        if not result:
            return ResultSet([], cls.ENTITY_NAME)

        # Загружаем полную информацию о каждом контакте
        contacts = []
        for contact_item in result:
            contact_id = contact_item.get("CONTACT_ID")
            if contact_id:
                contact_info = query_to_bitrix("get_contact_info", id=contact_id)
                if contact_info:
                    contacts.append(contact_info)

        return ResultSet(contacts, cls.ENTITY_NAME)


class Leads(BitrixEntity):
    """
    Работа с лидами.

    Примеры:
        Leads.find(status="NEW").show()
        Leads.find(source="WEB").to_csv("web_leads.csv")
    """

    ENTITY_TYPE = "lead"
    LIST_METHOD = "lead_list"
    GET_METHOD = "lead_get"
    FIELDS_METHOD = "lead_fields"
    ENTITY_NAME = "лидов"

    FILTER_ALIASES = {
        "status": "STATUS_ID",
        "source": "SOURCE_ID",
        "title": "%TITLE",
        "name": "%NAME",
        "responsible": "ASSIGNED_BY_ID",
        "created_after": ">=DATE_CREATE",
        "created_before": "<=DATE_CREATE",
    }

    @classmethod
    def statuses(cls) -> dict[str, str]:
        """
        Получить словарь статусов лидов {название: id}.

        Returns:
            Словарь статусов
        """
        def load_statuses():
            statuses = query_to_bitrix("get_status_list", filter={"ENTITY_ID": "STATUS"})
            if not statuses:
                return {}
            return {s["NAME"]: s["STATUS_ID"] for s in statuses}

        return _get_cached("lead_statuses", load_statuses)


class Requisites(BitrixEntity):
    """
    Работа с реквизитами.

    Примеры:
        Requisites.of_company(12345).show()
        Requisites.find(PRESET_ID=1).show()
    """

    ENTITY_TYPE = "requisite"
    LIST_METHOD = "get_requisite_list"
    GET_METHOD = "get_requisite"
    FIELDS_METHOD = "get_requisite_fields"
    ENTITY_NAME = "реквизитов"

    @classmethod
    def of_company(cls, company_id: int | str) -> ResultSet:
        """
        Получить реквизиты компании.

        Args:
            company_id: ID компании

        Returns:
            ResultSet с реквизитами
        """
        return cls.find(ENTITY_TYPE_ID=4, ENTITY_ID=company_id)

    @classmethod
    def links(cls, entity_type: str = "company", entity_id: int | str | None = None) -> ResultSet:
        """
        Получить связи реквизитов.

        Args:
            entity_type: Тип сущности (company, contact, lead)
            entity_id: ID сущности

        Returns:
            ResultSet со связями
        """
        type_map = {"company": 4, "contact": 3, "lead": 1}
        params = {"ENTITY_TYPE_ID": type_map.get(entity_type, 4)}
        if entity_id:
            params["ENTITY_ID"] = entity_id

        result = query_to_bitrix("list_requisite_links", filter=params)
        return ResultSet(result or [], "связей")


class Users(BitrixEntity):
    """
    Работа с пользователями.

    Примеры:
        Users.find(active=True).show()
        Users.get(16)
    """

    ENTITY_TYPE = "user"
    LIST_METHOD = "get_employees"
    GET_METHOD = "user_info"
    ENTITY_NAME = "пользователей"

    FILTER_ALIASES = {
        "active": "ACTIVE",
        "name": "%NAME",
        "email": "EMAIL",
        "department": "UF_DEPARTMENT",
    }


# Экспорт всех классов
__all__ = [
    "ResultSet",
    "Companies",
    "Deals",
    "Contacts",
    "Leads",
    "Requisites",
    "Users",
    "clear_cache",
]
