import re
from sqlfmt.api import format_string, Mode
from typing import Tuple, Optional

# Функции ClickHouse с правильным регистром (значения по умолчанию)
CLICKHOUSE_FUNCTIONS_DEFAULT = [
    # Словарные функции
    "dictGet", "dictGetOrDefault", "dictGetOrNull", "dictHas",
    "dictGetHierarchy", "dictIsIn",
    # Условные функции
    "multiIf", "if",
    # Функции даты/времени
    "toDate", "toDateTime", "toDateTime64", "toDateOrNull", "toDateTimeOrNull",
    "toStartOfDay", "toStartOfHour", "toStartOfMinute", "toStartOfMonth",
    "toStartOfQuarter", "toStartOfYear", "toStartOfWeek", "toMonday",
    "toYear", "toMonth", "toWeek", "toDayOfMonth", "toDayOfWeek", "toDayOfYear",
    "toHour", "toMinute", "toSecond", "toUnixTimestamp",
    "formatDateTime", "parseDateTimeBestEffort", "parseDateTime64BestEffort",
    "dateDiff", "dateAdd", "dateSub", "timeSlot",
    # Функции преобразования типов
    "toString", "toInt8", "toInt16", "toInt32", "toInt64", "toInt128", "toInt256",
    "toUInt8", "toUInt16", "toUInt32", "toUInt64", "toUInt128", "toUInt256",
    "toFloat32", "toFloat64", "toDecimal32", "toDecimal64", "toDecimal128",
    "toFixedString", "toUUID", "toIPv4", "toIPv6",
    "toIntervalSecond", "toIntervalMinute", "toIntervalHour", "toIntervalDay",
    "toIntervalWeek", "toIntervalMonth", "toIntervalQuarter", "toIntervalYear",
    "reinterpretAsInt8", "reinterpretAsInt16", "reinterpretAsInt32", "reinterpretAsInt64",
    "reinterpretAsUInt8", "reinterpretAsUInt16", "reinterpretAsUInt32", "reinterpretAsUInt64",
    "reinterpretAsFloat32", "reinterpretAsFloat64", "reinterpretAsString",
    "accurateCast", "accurateCastOrNull",
    # Агрегатные функции
    "countIf", "sumIf", "avgIf", "minIf", "maxIf", "anyIf",
    "uniq", "uniqExact", "uniqCombined", "uniqCombined64", "uniqHLL12",
    "groupArray", "groupArrayInsertAt", "groupUniqArray",
    "groupBitAnd", "groupBitOr", "groupBitXor",
    "argMin", "argMax",
    "quantile", "quantiles", "quantileExact", "quantileTiming",
    "simpleLinearRegression", "stochasticLinearRegression",
    # Функции для работы со строками
    "replaceAll", "replaceOne", "replaceRegexpAll", "replaceRegexpOne",
    "splitByChar", "splitByString", "splitByRegexp",
    "arrayStringConcat", "extractAll", "extractAllGroups",
    "trimLeft", "trimRight", "trimBoth",
    "leftPad", "rightPad", "leftPadUTF8", "rightPadUTF8",
    "lowerUTF8", "upperUTF8", "reverseUTF8",
    "substringUTF8", "lengthUTF8", "positionUTF8",
    "positionCaseInsensitive", "positionCaseInsensitiveUTF8",
    "multiSearchFirstIndex", "multiSearchFirstPosition", "multiSearchAny",
    "multiMatchAny", "multiMatchAnyIndex", "multiFuzzyMatchAny",
    "normalizeQuery", "normalizedQueryHash",
    "encodeXMLComponent", "decodeXMLComponent",
    "extractURLParameter", "extractURLParameters", "extractURLParameterNames",
    "cutURLParameter", "cutToFirstSignificantSubdomain",
    "URLHierarchy", "URLPathHierarchy",
    # Функции для массивов
    "arrayJoin", "arrayConcat", "arrayElement", "arrayPushBack", "arrayPushFront",
    "arrayPopBack", "arrayPopFront", "arraySlice", "arrayReverse",
    "arrayCompact", "arrayDistinct", "arrayEnumerate", "arrayEnumerateDense",
    "arrayEnumerateUniq", "arrayReduce", "arrayReduceInRanges",
    "arrayFilter", "arrayExists", "arrayAll", "arrayFirst", "arrayFirstIndex",
    "arraySum", "arrayAvg", "arrayCount", "arrayMin", "arrayMax",
    "arraySort", "arrayReverseSort", "arrayUniq", "arrayDifference",
    "hasAll", "hasAny", "hasSubstr", "indexOf", "arrayZip",
    # Функции для NULL
    "ifNull", "nullIf", "assumeNotNull", "toNullable", "coalesce", "isNull", "isNotNull",
    # Функции для JSON
    "JSONExtract", "JSONExtractString", "JSONExtractInt", "JSONExtractFloat",
    "JSONExtractBool", "JSONExtractRaw", "JSONExtractArrayRaw", "JSONExtractKeysAndValues",
    "JSONHas", "JSONLength", "JSONType", "JSONExtractKeys",
    "simpleJSONExtractString", "simpleJSONExtractInt", "simpleJSONExtractFloat",
    "simpleJSONExtractBool", "simpleJSONExtractRaw", "simpleJSONHas",
    # Битовые функции
    "bitAnd", "bitOr", "bitXor", "bitNot", "bitShiftLeft", "bitShiftRight",
    "bitRotateLeft", "bitRotateRight", "bitTest", "bitTestAll", "bitTestAny",
    "bitCount", "bitPositionsToArray",
    # Функции хеширования
    "cityHash64", "sipHash64", "sipHash128", "halfMD5", "MD5", "SHA1", "SHA224",
    "SHA256", "SHA384", "SHA512", "URLHash", "farmHash64", "javaHash",
    "murmurHash2_32", "murmurHash2_64", "murmurHash3_32", "murmurHash3_64",
    "murmurHash3_128", "xxHash32", "xxHash64",
    # Географические функции
    "geoDistance", "greatCircleDistance", "greatCircleAngle",
    "pointInEllipses", "pointInPolygon",
    "geohashEncode", "geohashDecode", "geohashesInBox",
    "h3IsValid", "h3GetResolution", "h3EdgeAngle", "h3EdgeLengthM",
    # Функции для работы с IP
    "IPv4NumToString", "IPv4StringToNum", "IPv4ToIPv6",
    "IPv6NumToString", "IPv6StringToNum",
    "toIPv4", "toIPv6", "isIPv4String", "isIPv6String",
    # Прочие функции
    "runningDifference", "runningDifferenceStartingWithFirstValue",
    "runningAccumulate", "neighborhoodRelation",
    "rowNumberInAllBlocks", "rowNumberInBlock",
    "formatRow", "formatRowNoNewline",
    "generateUUIDv4",
    "getMacro", "getSetting",
    "isFinite", "isInfinite", "isNaN",
    "toTypeName", "blockSize", "materialize", "ignore",
    "sleep", "sleepEachRow",
    "currentDatabase", "currentUser", "hostName", "uptime", "version",
    "throwIf", "identity",
]

# Текущий список функций (можно изменять через set_custom_functions)
CLICKHOUSE_FUNCTIONS = list(CLICKHOUSE_FUNCTIONS_DEFAULT)

# Словарь для быстрого поиска (lowercase -> правильный регистр)
_FUNC_CASE_MAP = {func.lower(): func for func in CLICKHOUSE_FUNCTIONS}


def set_custom_functions(functions_list: list):
    """Устанавливает пользовательский список функций ClickHouse."""
    global CLICKHOUSE_FUNCTIONS, _FUNC_CASE_MAP
    CLICKHOUSE_FUNCTIONS = list(functions_list)
    _FUNC_CASE_MAP = {func.lower(): func for func in CLICKHOUSE_FUNCTIONS}

# SQL ключевые слова для преобразования в uppercase
SQL_KEYWORDS = [
    "select", "from", "where", "and", "or", "not", "in", "is", "null",
    "as", "on", "join", "left", "right", "inner", "outer", "full", "cross",
    "group", "by", "order", "having", "limit", "offset", "union", "all",
    "distinct", "case", "when", "then", "else", "end", "cast", "between",
    "like", "ilike", "exists", "any", "with", "recursive", "insert", "into",
    "values", "update", "set", "delete", "create", "table", "view", "index",
    "drop", "alter", "add", "column", "primary", "key", "foreign", "references",
    "asc", "desc", "nulls", "first", "last", "over", "partition", "row", "rows",
    "range", "unbounded", "preceding", "following", "current", "interval",
    "true", "false", "using", "natural", "except", "intersect", "global",
    "prewhere", "sample", "array", "final", "format", "settings",
]


def _keywords_to_upper(sql: str) -> str:
    """Преобразует SQL ключевые слова в UPPERCASE."""
    def replace_keyword(match):
        word = match.group(0)
        if word.lower() in SQL_KEYWORDS:
            return word.upper()
        return word

    # Заменяем только целые слова (не внутри строк)
    # Простой подход: ищем слова на границах
    return re.sub(r'\b([a-zA-Z_]+)\b', replace_keyword, sql)


def _restore_function_case(sql: str) -> str:
    """Восстанавливает правильный регистр функций ClickHouse."""
    def replace_func(match):
        func_name = match.group(1)
        correct_case = _FUNC_CASE_MAP.get(func_name.lower())
        if correct_case:
            return correct_case + "("
        return match.group(0)

    # Ищем паттерн: слово сразу перед открывающей скобкой (имя функции)
    return re.sub(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(', replace_func, sql)


def format_sql(sql_code: str, keywords_case: str = "lower") -> Tuple[str, Optional[str]]:
    """
    Форматирует SQL код с сохранением регистра функций ClickHouse.

    Args:
        sql_code: SQL код для форматирования
        keywords_case: регистр ключевых слов - "lower" или "upper"

    Returns:
        Tuple[formatted_sql, error_message]
        error_message = None если успешно
    """
    try:
        result = format_string(sql_code, mode=Mode())
        result = _restore_function_case(result)
        if keywords_case == "upper":
            result = _keywords_to_upper(result)
        return result, None
    except Exception as e:
        return sql_code, str(e)
