
import re


def get_template_id(value: str) -> int:
    template_id = re.sub("[^0-9]", "", value)
    return None if not template_id else int(template_id)

def get_quote_id(value: str) -> str:
    s = value
    match = re.search(r"QUOTE#\s*(.*?)\s*-", s)
    return match.group(1) if match else None