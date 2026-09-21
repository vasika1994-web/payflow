from enum import StrEnum


def enum_values(enum_cls: type[StrEnum]) -> list[str]:
    # SQLAlchemy по умолчанию кладёт в ENUM имена членов, а не значения
    return [member.value for member in enum_cls]


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
