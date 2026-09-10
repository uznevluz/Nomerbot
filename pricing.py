from config import MARKUP_PERCENT


def with_markup(base_price) -> int:
    """Bazaviy (SmmUpper) narxga sozlangan foyda foizini qo'shib, yaxlit so'mga aylantiradi."""
    return round(float(base_price) * (1 + MARKUP_PERCENT / 100))
