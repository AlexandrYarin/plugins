# Шпаргалка по модулю Bitrix Explorer

## Быстрый старт

```python
# Активировать виртуальное окружение (если нужно)
# source .venv/bin/activate

from bitrix.explorer import Companies, Deals, Contacts, Leads, Requisites, Users
```

---

## Companies (Компании)

```python
# Найти все компании (limit=5 для теста)
Companies.find(limit=5).show()

# Найти по типу
Companies.find(type="Поставщик").show()
Companies.find(type="Клиент").show()

# Поиск по названию (частичное совпадение)
Companies.find(title="ООО").show()

# Получить компанию по ID
company = Companies.get(12345)

# Получить компанию со всеми контактами и сделками
company = Companies.get(12345, with_contacts=True, with_deals=True)

# Список всех типов компаний
Companies.types()
# {'Клиент': 'CUSTOMER', 'Поставщик': 'SUPPLIER', ...}

# Подсчитать количество
Companies.count()
Companies.count(type="Поставщик")
```

---

## Deals (Сделки)

```python
# Найти все сделки
Deals.find(limit=10).show()

# Найти сделки компании
Deals.find(company=12345).show()

# Найти по стадии
Deals.find(status="Сделка успешна").show()

# Найти по сумме
Deals.find(amount_gt=50000).show()              # больше 50000
Deals.find(amount_lt=10000).show()              # меньше 10000
Deals.find(amount_gt=10000, amount_lt=50000).show()  # от 10000 до 50000

# Найти по дате создания
Deals.find(created_after="2024-01-01").show()

# Комбинированный поиск
Deals.find(company=12345, amount_gt=10000).show()

# Список всех стадий
Deals.stages()
# {'Формирование ТЗ от клиента': 'NEW', 'Сделка успешна': 'WON', ...}

# Получить сделку по ID
deal = Deals.get(5678)
```

---

## Contacts (Контакты)

```python
# Найти все контакты
Contacts.find(limit=10).show()

# Поиск по имени
Contacts.find(name="Иван").show()

# Контакты конкретной компании
Contacts.of_company(12345).show()

# Получить контакт по ID
contact = Contacts.get(100)
```

---

## Leads (Лиды)

```python
# Найти все лиды
Leads.find(limit=10).show()

# Найти по статусу
Leads.find(status="Не обработан").show()

# Поиск по названию
Leads.find(title="заказ").show()

# Список всех статусов
Leads.statuses()
# {'Не обработан': 'NEW', 'Качественный лид': 'CONVERTED', ...}
```

---

## Requisites (Реквизиты)

```python
# Найти все реквизиты
Requisites.find(limit=10).show()

# Реквизиты конкретной компании
Requisites.of_company(12345).show()

# Связи реквизитов
Requisites.links(entity_type="company", entity_id=12345)
```

---

## Users (Пользователи)

```python
# Найти всех пользователей
Users.find(limit=10).show()

# Получить пользователя по ID
user = Users.get(16)
```

---

## Вывод и экспорт (ResultSet)

```python
# Вывести таблицу в терминал
Companies.find(limit=10).show()

# Вывести только нужные колонки
Companies.find().show(columns=["ID", "TITLE", "COMPANY_TYPE"])

# Ограничить вывод (по умолчанию 50 строк)
Companies.find().show(limit=20)

# Сохранить в Excel
Companies.find(type="Поставщик").to_excel("suppliers.xlsx")

# Сохранить в CSV
Deals.find(amount_gt=50000).to_csv("big_deals.csv")

# Сохранить только нужные колонки
Companies.find().to_excel("companies.xlsx", columns=["ID", "TITLE", "PHONE"])

# Преобразовать в pandas DataFrame
df = Companies.find().to_df()

# Получить первый элемент
first_company = Companies.find(limit=1).first()

# Получить список ID
ids = Companies.find(type="Поставщик").ids()

# Дополнительная фильтрация в памяти
Companies.find().filter(TITLE__contains="ООО").show()
```

---

## Цепочки методов

```python
# Найти → показать → сохранить
Companies.find(type="Поставщик").show().to_excel("suppliers.xlsx")

# Найти → фильтровать → показать
Companies.find().filter(TITLE__contains="Принт").show()
```

---

## Прямые фильтры Битрикс

Можно использовать любые поля Битрикс напрямую:

```python
# Фильтр по ответственному
Deals.find(ASSIGNED_BY_ID=16).show()

# Фильтр по кастомному полю
Companies.find(UF_CRM_1756212422=123).show()

# Комбинация алиасов и прямых фильтров
Deals.find(company=12345, CATEGORY_ID=0).show()
```

---

## Полезные поля для select

```python
# Компании
Companies.find(select=["ID", "TITLE", "COMPANY_TYPE", "PHONE", "EMAIL"]).show()

# Сделки
Deals.find(select=["ID", "TITLE", "OPPORTUNITY", "STAGE_ID", "COMPANY_ID"]).show()

# Контакты
Contacts.find(select=["ID", "NAME", "LAST_NAME", "PHONE", "EMAIL"]).show()
```

---

## Очистка кэша

```python
from bitrix.explorer import clear_cache

# Очистить кэш справочников (типы, стадии, статусы)
clear_cache()
```

---

## Примеры реальных задач

```python
# 1. Найти всех поставщиков и сохранить в Excel
Companies.find(type="Поставщик").to_excel("suppliers.xlsx")

# 2. Найти крупные сделки за 2024 год
Deals.find(amount_gt=100000, created_after="2024-01-01").show()

# 3. Получить контакты поставщика
company_id = Companies.find(type="Поставщик", limit=1).first()["ID"]
Contacts.of_company(company_id).show()

# 4. Статистика по стадиям сделок
for stage_name, stage_id in Deals.stages().items():
    count = Deals.count(status=stage_id)
    print(f"{stage_name}: {count}")

# 5. Экспорт всех необработанных лидов
Leads.find(status="Не обработан").to_csv("new_leads.csv")
```
