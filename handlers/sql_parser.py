import re

def parse_sql(sql_content: str) -> str:
    # Регулярное выражение для поиска нужных подстрок
    pattern_from_join = r'(?<![a-zA-Z])(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z0-9_]+)'
    pattern_dictget = r"dictGet\(\'([^\']*\.[^\']*)\'"

    unique_tables = set()
    unique_dicts = set()
    template_text = ''

    # Ищем все совпадения по регулярному выражению
    matches_all = set()
    matches_from_join = re.findall(pattern_from_join, sql_content, re.IGNORECASE)
    matches_dictget = re.findall(pattern_dictget, sql_content, re.IGNORECASE)
    matches_all.update(matches_from_join)
    matches_all.update(matches_dictget)

    unique_tables.update(matches_from_join)
    unique_dicts.update(matches_dictget)

    # Добавляем найденные совпадения в текст результата
    if matches_from_join or matches_dictget:
        template_text = template_text + '\r' + f"Matches found: {matches_all}"

    # Добавляем список уникальных таблиц
    if unique_tables:
        template_text = template_text + '\r' + "\r# Список всех таблиц:"
        for u in unique_tables:
            template_text = template_text + '\n' + u

    # Добавляем список уникальных словарей
    if unique_dicts:
        template_text = template_text + '\r' + "\r# Список всех диктов:"
        for u in unique_dicts:
            template_text = template_text + '\n' + u

    return template_text