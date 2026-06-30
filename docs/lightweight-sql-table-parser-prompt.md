# Lightweight SQL Table Parser Prompt

This prompt captures the current design brief for a pragmatic SQL table/dictionary extractor. Keep it as a project reference; update this file when the expected extraction rules change.

```text
Нужно реализовать lightweight SQL Table Parser.

Цель:
Из произвольного SQL или Python/DAG-кода извлекать:
1. обычные таблицы из SQL-конструкций;
2. ClickHouse-словари из функций семейства dictGet*.

Это не должен быть полноценный SQL AST-парсер. Нужен прагматичный extractor, который устойчиво работает на рабочих ClickHouse-запросах.

Вход:
- строка с SQL или Python-кодом, внутри которого может быть SQL.

Выход:
- отсортированный уникальный список таблиц;
- отсортированный уникальный список словарей;
- желательно отдельные секции `Tables` и `Dicts`.

Базовая логика:

1. Предобработка:
   - Разбить текст на строки.
   - Убрать Python import-строки, чтобы не ловить ложные совпадения:
     - строки вида `import x`
     - строки вида `from x import y`
   - По возможности игнорировать SQL-комментарии:
     - `-- ...`
     - `/* ... */`
   - Но не ломать реальные SQL-выражения.

2. Поиск обычных таблиц:
   Ищем идентификаторы после SQL-ключевых конструкций:
   - `FROM`
   - `JOIN`
   - `INSERT INTO`
   - `TRUNCATE`
   - `TRUNCATE TABLE`

   Таблица должна быть составной, минимум с одной точкой:
   - `db.table`
   - `schema.table`
   - `cluster.db.table`
   - `datamart.v3_by_srid_d`

   Пример regex-идеи:
   ```regex
   (?i)\b(?:from|join|insert\s+into|truncate(?:\s+table)?)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z0-9_]+)+)
   ```

   Важно:
   - матчить только имя таблицы, без alias;
   - `FROM datamart.v3_by_srid_d AS src FINAL` должен вернуть `datamart.v3_by_srid_d`;
   - регистр ключевых слов не важен.

3. Поиск ClickHouse dictGet-словарей:
   Нужно поддерживать не только `dictGet(...)`, но всё семейство функций:
   - `dictGet`
   - `dictGetOrDefault`
   - `dictGetOrNull`
   - `dictGetString`
   - `dictGetInt32`
   - `dictGetUInt16`
   - `dictGetInt32OrDefault`
   - `dictGetUInt16OrDefault`
   - и другие варианты, начинающиеся с `dictGet`.

   Словарь находится в первом строковом аргументе функции:
   ```sql
   dictGetInt32OrDefault('dict.product_cards_nm_short', 'seller_id', nm_id, 0)
   ```

   Должно быть извлечено:
   ```text
   dict.product_cards_nm_short
   ```

   Пример regex-идеи:
   ```regex
   (?i)\bdictGet[A-Za-z0-9_]*\s*\(\s*['"]([^'"]*\.[^'"]*)['"]
   ```

   Важно:
   - поддержать одинарные и двойные кавычки;
   - поддержать пробелы между именем функции и `(`;
   - не ограничиваться точным именем `dictGet`;
   - брать только первый аргумент;
   - возвращать словарь, только если внутри есть точка.

4. Дедупликация и сортировка:
   - Таблицы и словари хранить в set.
   - На выходе сортировать стабильно, например по алфавиту.
   - Если один и тот же словарь встречается несколько раз, выводить один раз.

5. Минимальные тесты:
   Обязательно покрыть такие случаи:

   ```sql
   SELECT * FROM db.table1 JOIN db.table2 ON table1.id = table2.id
   ```
   Ожидаем tables:
   - `db.table1`
   - `db.table2`

   ```sql
   INSERT INTO schema1.target_table SELECT * FROM schema2.source
   ```
   Ожидаем tables:
   - `schema1.target_table`
   - `schema2.source`

   ```sql
   SELECT dictGet('db.my_dict', 'col', id) FROM db.some_table
   ```
   Ожидаем:
   - table: `db.some_table`
   - dict: `db.my_dict`

   ```sql
   SELECT dictGetInt32OrDefault('dict.product_cards_nm_short', 'seller_id', nm_id, 0),
          dictGetUInt16OrDefault('dict.product_cards_nm_short', 'subject_id', nm_id, 0)
   FROM datamart.v3_by_srid_d AS src FINAL
   ```
   Ожидаем:
   - table: `datamart.v3_by_srid_d`
   - dict: `dict.product_cards_nm_short`

   ```sql
   TRUNCATE TABLE db.old_table
   ```
   Ожидаем:
   - table: `db.old_table`

   ```python
   from module import something
   sql = "SELECT * FROM db.real_table"
   ```
   Ожидаем:
   - table: `db.real_table`
   - не возвращать `module`.

6. Не усложнять:
   - Не нужно строить полный SQL AST.
   - Не нужно поддерживать все диалекты SQL идеально.
   - Главный критерий: корректно извлекать реальные таблицы и ClickHouse dictGet-family словари из рабочих запросов.
```

Implementation note: the local extractor also supports `ALTER TABLE ... FROM ...`, because DAG lineage checks need to see ClickHouse partition moves such as `ATTACH PARTITION ... FROM buffer.table`.
