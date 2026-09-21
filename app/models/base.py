from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Общий базовый класс моделей. От него же Alembic берёт метаданные."""
