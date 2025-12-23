"""Модели для API запросов и ответов."""

from typing import List, Optional

from pydantic import BaseModel, Field


class CandidateEvaluation(BaseModel):
    """Оценка кандидата по критериям."""

    name: str = Field(description="Имя кандидата")
    phone: str = Field(description="Телефон кандидата")
    location: str = Field(description="Локация кандидата")
    hard_skills_score: int = Field(
        ge=1, le=10, description="Оценка хард-скиллов от 1 до 10"
    )
    domain_skills_score: int = Field(
        ge=1, le=10, description="Оценка доменных навыков от 1 до 10"
    )
    relevance_score: int = Field(
        ge=1, le=10, description="Общая релевантность от 1 до 10"
    )
    relevance_explanation: str = Field(
        description="Объяснение оценки общей релевантности"
    )


class VacancySearchRequest(BaseModel):
    """Запрос на поиск кандидатов по вакансии."""

    description: str = Field(description="Описание вакансии")
    experience_years_min: Optional[float] = Field(
        default=None, ge=0, description="Минимальный опыт в годах"
    )
    grade: Optional[str] = Field(
        default=None,
        description="Требуемый уровень (Intern, Junior, Middle, Senior, Lead, Head)",
        pattern=r"^(Intern|Junior|Middle|Senior|Lead|Head)?$",
    )
    k: int = Field(
        default=5, ge=1, le=20, description="Количество возвращаемых кандидатов"
    )


class VacancySearchResponse(BaseModel):
    """Ответ с найденными кандидатами."""

    candidates: List[CandidateEvaluation] = Field(
        description="Список кандидатов с оценками"
    )
    total_found: int = Field(description="Общее количество найденных кандидатов")
