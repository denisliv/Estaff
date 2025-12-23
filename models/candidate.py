"""Модели данных для кандидатов."""

from typing import List, Optional

from pydantic import BaseModel, Field


class LanguageSkill(BaseModel):
    """Модель языкового навыка."""

    name: str = Field(description="Название языка")
    level: Optional[str] = Field(
        description="Уровень владения: 'A1', 'A2', 'B1', 'B2', 'C1', 'C2', 'Native' или null",
        pattern=r"^(A[12]|B[12]|C[12]|Native)?$",
    )


class CandidateMetadata(BaseModel):
    """Метаданные кандидата для векторной БД."""

    candidate_id: int = Field(description="Уникальный идентификатор кандидата")
    location_name: Optional[str] = Field(description="Локация кандидата")

    positions: List[str] = Field(
        default_factory=list,
        description="Список релевантных должностей, которые могут быть предложены кандидату",
    )
    experience_years: Optional[float] = Field(
        ge=0,
        description="Суммарный релевантный опыт в годах для указанной желаемой должности",
    )
    grade: Optional[str] = Field(
        description="Уровень специалиста: Intern, Junior, Middle, Senior, Lead, Head или null",
        pattern=r"^(Intern|Junior|Middle|Senior|Lead|Head)?$",
    )
    hard_skills: List[str] = Field(
        default_factory=list,
        description="Технические навыки и инструменты",
    )
    domain_skills: List[str] = Field(
        default_factory=list,
        description="Отраслевые и функциональные домены (предметные области)",
    )
    performed_tasks: List[str] = Field(
        default_factory=list,
        description="Фактически выполняемые профессиональные задачи",
    )
    languages: List[LanguageSkill] = Field(
        default_factory=list, description="Языки с нормализованными уровнями владения"
    )
    embedding_text: str = Field(
        description="Очищенный текст для генерации эмбеддинга: без личных данных, с ключевыми навыками и опытом"
    )
