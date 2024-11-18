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


def sql_format_list(values: List[str]) -> str:
    """Formats a list of strings for SQL, with proper escaping.

    Args:
        values: List of strings to format

    Returns:
        SQL-formatted string representation of the list
    """
    quoted_values = [sql_quote_string(val) for val in values]
    return f"({', '.join(quoted_values)})"


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


def sql_format_dict(values: Dict[str, str]) -> str:
    """Formats a dictionary for SQL, with proper escaping.

    Args:
        values: Dictionary to format

    Returns:
        SQL-formatted string representation of the dictionary
    """
    parts = []
    for key, value in values.items():
        parts.append(f"{sql_quote_string(key)} = {sql_format_value(value)}")
    return f"({', '.join(parts)})"


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
