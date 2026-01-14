# Импорт работает и изнутри папки, и снаружи
try:
    from bitrix.explorer import Companies, Deals, Contacts, Leads, Requisites, Users
except ImportError:
    from explorer import Companies, Deals, Contacts, Leads, Requisites, Users


Companies.find(type="Поставщик").to_excel("suppliers.xlsx")
