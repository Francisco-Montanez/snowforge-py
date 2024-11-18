from typing import Dict, List, Union


def sql_escape_string(value: str) -> str:
    """Escapes special characters in SQL string literals.

    Args:
        value: The string to escape

    Returns:
        Escaped string safe for SQL string literals
    """
    replacements = {
        chr(92): chr(92) * 2,  # Backslash
        chr(39): chr(39) * 2,  # Single quote
        chr(34): chr(34) * 2,  # Double quote
    }

    for char, replacement in replacements.items():
        value = value.replace(char, replacement)
    return value


def sql_quote_string(value: str) -> str:
    """Quotes and escapes a string for SQL.

    Args:
        value: The string to quote and escape

    Returns:
        Quoted and escaped string safe for SQL
    """
    return f"'{sql_escape_string(value)}'"


def sql_format_boolean(value: bool) -> str:
    return str(value).upper()


def sql_format_list(values: List[str], quote_values: bool = True) -> str:
    """Formats a list of strings for SQL, with optional quoting.

    Args:
        values: List of strings to format
        quote_values: Whether to quote the values (default: True)

    Returns:
        SQL-formatted string representation of the list
    """
    if quote_values:
        formatted_values = [sql_quote_string(val) for val in values]
    else:
        formatted_values = values
    return f"({', '.join(formatted_values)})"


def sql_format_value(value: Union[bool, str, int, float, None]) -> str:
    """Formats a value for SQL with proper type handling.

    Args:
        value: The value to format

    Returns:
        SQL-formatted string representation of the value
    """
    if value is None:
        return "NULL"
    elif isinstance(value, bool):
        return str(value).upper()
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return sql_quote_string(str(value))


def sql_format_dict(d: Dict[str, str]) -> str:
    """Formats a dictionary for SQL.

    For tags in Snowflake, the format should be:
    tag (key1 = 'value1', key2 = 'value2')

    Args:
        d: Dictionary to format

    Returns:
        Formatted string for SQL
    """
    if not d:
        return ""

    pairs = [f"{k} = {sql_quote_string(v)}" for k, v in d.items()]
    return f"({', '.join(pairs)})"


def sql_escape_comment(value: str) -> str:
    """Escapes special characters in SQL comment strings.

    Handles the specific case of escaping quotes within comments for Snowflake SQL.
    Single quotes are escaped with backslash, double quotes remain unescaped.

    Args:
        value: The comment string to escape

    Returns:
        Escaped string safe for SQL comments
    """
    return value.replace("'", "\\'")


def sql_quote_comment(value: str) -> str:
    """Quotes and escapes a comment string for SQL.

    Specifically handles Snowflake SQL comment formatting.

    Args:
        value: The comment string to quote and escape

    Returns:
        Quoted and escaped comment string safe for SQL
    """
    return f"'{sql_escape_comment(value)}'"


def sql_format_tag(key: str, value: str) -> str:
    """Formats a single tag for SQL.

    For table tags in Snowflake, the format should be:
    tag (key = 'value')

    Args:
        key: Tag key
        value: Tag value

    Returns:
        Formatted string for SQL
    """
    return f"TAG ({key} = {sql_quote_string(value)})"


def sql_format_tags(tags: Dict[str, str]) -> str:
    """Formats multiple tags for SQL.

    Args:
        tags: Dictionary of tags

    Returns:
        Formatted string for SQL
    """
    if not tags:
        return ""

    return f"{' '.join(sql_format_dict(tags).split(', '))}"
