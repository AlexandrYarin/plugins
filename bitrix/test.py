from core import query_to_bitrix

deal_id = 5044
comment_text = "Тестовый  комментарий"

params = {
    "fields": {"ENTITY_ID": deal_id, "ENTITY_TYPE": "deal", "COMMENT": comment_text}
}
params2 = {"ID": deal_id}

res = query_to_bitrix("deal_info", **params2)
print(res)
