import base64
import hashlib
import re
import socket

import pandas as pd
from bs4 import BeautifulSoup, Tag
from faker import Faker


def check_port(host: str, port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            result = s.connect_ex((host, port))
            if result == 0:
                print(f"Хост {host}. Порт {port} ОТКРЫТ")
                return True
            else:
                print(f"Хост {host}. Порт {port} ЗАКРЫТ (код ошибки: {result})")
                return False
    except Exception as e:
        print(f"Ошибка {e}")
        return False


def consistent_fake(val, field):
    seed = int(hashlib.sha256(str(val).encode()).hexdigest(), 16) % (10**9)
    Faker.seed(seed)
    fake = Faker()
    if field == "name":
        return fake.name()
    elif field == "email":
        return fake.email()
    elif field == "mobile_phone":
        return fake.phone_number()


def serialize_cell(x):
    if isinstance(x, memoryview):
        x = x.tobytes()
    if isinstance(x, (bytes, bytearray)):
        return base64.b64encode(x).decode("ascii")
    return x


def deserialize_cell(x):
    if isinstance(x, str) and x:
        return base64.b64decode(x.encode("ascii"))
    return x


def extract_html_resume(x):
    if x is None:
        return None

    if isinstance(x, (bytes, bytearray, memoryview)):
        try:
            s = bytes(x).decode("utf-8", errors="ignore").strip()
        except Exception:
            return None
    else:
        s = str(x).strip()

    if "<html>" in s and '<p class="EStaffResumeTitle">' in s:
        return s
    return None


def clean_description(text: str) -> str:
    """Очистка и форматирование описания."""
    if not text:
        return "не указано"

    text = text.replace("*", "")
    text = text.replace("\n\n", "\n")
    text = re.sub(r"\n[-•·]\s*", ". ", text)
    text = text.replace("\n- ", ". ")
    text = text.replace("\n-", ". ")
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*;\s*", "; ", text)
    text = re.sub(r"\.\s*;\s*", ". ", text)
    text = re.sub(r"^\s*;\s*", "", text)
    text = re.sub(r"\.{2,}", ".", text)
    text = re.sub(r";\s*\.", ".", text)
    text = re.sub(r"\s+([,.])", r"\1", text)
    text = re.sub(r"([,.:])([^\s\d])", r"\1 \2", text)
    text = text.strip()
    text = re.sub(r"[;,]\s*$", "", text)
    return text


def safe_str(x) -> str:
    return "" if pd.isna(x) else str(x).strip()


def norm_line(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    text = text.replace("..", ".")
    text = re.sub(r"\s*;\s*", "; ", text)
    text = re.sub(r"\.\s*;\s*", ". ", text)
    return text.strip()


def norm_multiline(text: str) -> str:
    lines = text.split("\n")
    lines = [norm_line(line) for line in lines]
    while lines and lines[0] == "":
        lines.pop(0)
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines)


def parse_common_info_from_html(soup: BeautifulSoup) -> dict:
    """
    Парсинг общей информация из html.
    """
    desired_position = ""
    total_exp = "не указано"
    education_short = "не указано"
    location_name = ""

    title_tag = soup.find("p", class_="EStaffResumeTitle")
    location_tag = soup.find("span", class_="EStaffResumeLocation")
    if title_tag:
        desired_position = title_tag.get_text(strip=True)
    if location_tag:
        location_name = location_tag.get_text(strip=True)

    p_tags = soup.find_all("p")
    for p in p_tags:
        text = p.get_text(" ", strip=True)
        if "Образование:" in text and "Общий стаж:" in text:
            m_edu = re.search(r"Образование:\s*(.+?)\s+Общий стаж:", text)
            if m_edu:
                education_short = m_edu.group(1).strip()

            m_exp = re.search(r"Общий стаж:\s*(.+)$", text)
            if m_exp:
                total_exp = m_exp.group(1).strip()
            break

    return {
        "desired_position": desired_position or "не указано",
        "total_experience": total_exp or "не указано",
        "education_short": education_short or "не указано",
        "location_name": location_name or "",
    }


def parse_experience_from_html(soup: BeautifulSoup) -> str:
    """
    Парсинг раздела 'Опыт работы'.
    """
    exp_h2 = None
    for h2 in soup.find_all("h2", class_="EStaffResumeSectionTitle"):
        if "Опыт работы" in h2.get_text():
            exp_h2 = h2
            break

    if not exp_h2:
        return "Опыт работы по местам: не указано"

    exp_table = exp_h2.find_next("table")
    if not exp_table:
        return "Опыт работы по местам: не указано"

    rows = exp_table.find_all("tr", recursive=False)
    items = []
    index = 1

    for tr in rows:
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 2:
            continue

        period_td = tds[0]
        right_td = tds[1]

        length_p = period_td.find("p", class_="EStaffResumePeriodLengthDesc")
        length_text = ""
        if length_p:
            length_text = length_p.get_text(" ", strip=True)

        pos_p = right_td.find("p", class_="EStaffResumePrevJobPositionName")
        comment_p = right_td.find("p", class_="EStaffResumePrevJobPositionComment")

        pos_name = pos_p.get_text(" ", strip=True) if pos_p else ""
        comment_raw = comment_p.decode_contents() if comment_p else ""
        comment_text = BeautifulSoup(comment_raw, "html.parser").get_text("\n")
        comment_text = clean_description(comment_text)

        header = f"{index}. Должность: {pos_name or 'не указано'}"
        if length_text:
            header += f". Длительность: {length_text}"
        else:
            header += ". Длительность: не указано"

        if comment_text and comment_text != "не указано":
            descr = f"Описание: {comment_text}"
        else:
            descr = "Описание: не указано"

        items.append(f"{header}. {descr}")
        index += 1

    if not items:
        return "Опыт работы по местам: не указано"

    return "\n".join(items)


def parse_skills_from_html(soup: BeautifulSoup) -> str:
    """Парсинг раздела 'Ключевые навыки'"""
    skills_h2 = None
    for h2 in soup.find_all("h2", class_="EStaffResumeSectionTitle"):
        if "Ключевые навыки" in h2.get_text():
            skills_h2 = h2
            break

    if not skills_h2:
        return "не указано"

    p = skills_h2.find_next("p")
    if not p:
        return "не указано"

    spans = p.find_all("span")
    skills = [s.get_text(" ", strip=True) for s in spans if s.get_text(strip=True)]
    if not skills:
        return "не указано"

    return ", ".join(skills)


def parse_about_from_html(soup: BeautifulSoup) -> str:
    """
    Парсинг раздела 'Обо мне'.
    """
    about_h2 = None
    for h2 in soup.find_all("h2", class_="EStaffResumeSectionTitle"):
        if "Обо мне" in h2.get_text():
            about_h2 = h2
            break

    if not about_h2:
        return "не указано"

    texts = []

    for sibling in about_h2.find_all_next():
        if sibling.name == "h2" and sibling is not about_h2:
            break

        if sibling.name == "p":
            only_link = (
                len(list(sibling.children)) == 1
                and isinstance(next(iter(sibling.children), None), Tag)
                and next(iter(sibling.children)).name == "a"
            )
            if only_link:
                continue
            t = sibling.get_text("\n", strip=True)
            if t:
                texts.append(t)

    if not texts:
        return "не указано"

    about_raw = "\n".join(texts)
    return clean_description(about_raw)


def parse_education_core_from_html(soup: BeautifulSoup) -> list[str]:
    """
    Парсинг основного образования из HTML.
    Возвращает список строк вида: "Специальность - квалификация".
    """
    edu_h2 = None
    for h2 in soup.find_all("h2", class_="EStaffResumeSectionTitle"):
        if h2.get_text(strip=True).startswith("Образование"):
            edu_h2 = h2
            break

    if not edu_h2:
        return []

    table = edu_h2.find_next("table")
    if not table:
        return []

    items = []
    rows = table.find_all("tr", recursive=False)
    for tr in rows:
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 2:
            continue

        right_td = tds[1]
        p_list = right_td.find_all("p")
        if len(p_list) >= 3:
            specialty = p_list[1].get_text(strip=True)
            qualification = p_list[2].get_text(strip=True)
            if specialty or qualification:
                items.append(f"{specialty} - {qualification}")
        elif len(p_list) == 2:
            specialty = p_list[1].get_text(strip=True)
            items.append(f"{specialty} - ")

    return items


def parse_additional_education_specialties_from_html(soup: BeautifulSoup) -> list:
    """
    Парсинг Дополнительного образования.
    """
    add_h2 = None
    for h2 in soup.find_all("h2", class_="EStaffResumeSectionTitle"):
        if "Дополнительное образование" in h2.get_text():
            add_h2 = h2
            break

    if not add_h2:
        return []

    table = add_h2.find_next("table")
    if not table:
        return []

    rows = table.find_all("tr", recursive=False)
    items = []
    for tr in rows:
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 2:
            continue

        right_td = tds[1]
        p_list = right_td.find_all("p")
        if len(p_list) > 1:
            spec = p_list[1].get_text(" ", strip=True)
            if spec:
                items.append(spec)

    return items


def parse_languages_from_html(soup: BeautifulSoup) -> str:
    """
    Парсинг Знания языков.
    """
    lang_h2 = None
    for h2 in soup.find_all("h2", class_="EStaffResumeSectionTitle"):
        if "Знание языков" in h2.get_text():
            lang_h2 = h2
            break

    if not lang_h2:
        return "не указано"

    langs = []
    p = lang_h2.find_next("p")
    while p:
        text = p.get_text(" ", strip=True)
        if not text:
            nxt = p.find_next_sibling()
            if not nxt or nxt.name == "h2":
                break
            p = nxt if nxt.name == "p" else p.find_next_sibling("p")
            continue

        lang_only = "".join(t for t in p.find_all(string=True, recursive=False)).strip()
        span_level = p.find("span", class_="EStaffResumeLanguageLevel")
        level = span_level.get_text(" ", strip=True) if span_level else ""

        if lang_only:
            if level:
                langs.append(f"{lang_only} {level}")
            else:
                langs.append(lang_only)

        nxt = p.find_next_sibling()
        if not nxt or (
            nxt.name == "h2" and "SectionTitle" in " ".join(nxt.get("class", []))
        ):
            break
        p = nxt if nxt.name == "p" else p.find_next_sibling("p")

    if not langs:
        return "не указано"

    return ", ".join(langs)


def build_resume_row(row: pd.Series) -> str:
    """
    Формирование структурированного текста резюме для LLM.
    """
    id_ = safe_str(row.get("id"))
    html = row.get("html")

    profession_name_1 = safe_str(row.get("profession_name_1"))
    profession_name_2 = safe_str(row.get("profession_name_2"))
    last_job_position_name = safe_str(row.get("last_job_position_name"))
    location_name_estaff = safe_str(row.get("location_name"))
    if profession_name_1:
        if profession_name_2:
            profession_full = f"{profession_name_1}, {profession_name_2}"
        else:
            profession_full = profession_name_1
    else:
        profession_full = "не указано"

    if not html or pd.isna(html):
        return (
            "=== ОБЩАЯ ИНФОРМАЦИЯ ===\n"
            f"ID кандидата: {id_}\n"
            "Образование: не указано\n"
            "Желаемая должность: не указано\n"
            f"Общее название профессии: {profession_full}\n"
            f"Должность на последнем месте: {last_job_position_name or 'не указано'}"
            "Общий опыт работы: не указано\n"
            f"Локация: {location_name_estaff or 'не указано'}"
        )

    soup = BeautifulSoup(str(html), "html.parser")

    common = parse_common_info_from_html(soup)
    desired_position = common["desired_position"]
    total_experience = common["total_experience"]
    education_short = common["education_short"]
    location_name_html = common["location_name"]

    about_text = parse_about_from_html(soup)
    experience_text = parse_experience_from_html(soup)
    skills_text = parse_skills_from_html(soup)
    languages_text = parse_languages_from_html(soup)
    edu_core_list = parse_education_core_from_html(soup)
    add_specs_list = parse_additional_education_specialties_from_html(soup)

    main_edu_parts = []
    add_edu_parts = []

    main_edu_parts.extend(edu_core_list)
    add_edu_parts.extend(add_specs_list)

    seen_main = set()
    main_edu_unique = []
    for item in main_edu_parts:
        item_norm = item.strip()
        if item_norm and item_norm not in seen_main:
            seen_main.add(item_norm)
            main_edu_unique.append(item_norm)

    if main_edu_unique:
        main_education_final = "; ".join(main_edu_unique)
    else:
        main_education_final = "не указано"

    seen_add = set()
    add_edu_unique = []
    for item in add_edu_parts:
        item_norm = item.strip()
        if item_norm and item_norm not in seen_add:
            seen_add.add(item_norm)
            add_edu_unique.append(item_norm)

    if add_edu_unique:
        add_education_final = "; ".join(add_edu_unique)
    else:
        add_education_final = "не указано"

    parts = []

    parts.append("=== ОБЩАЯ ИНФОРМАЦИЯ ===")
    parts.append(f"ID кандидата: {id_}")
    parts.append(f"Образование: {education_short}")
    parts.append(f"Желаемая должность: {desired_position or 'не указано'}")
    parts.append(f"Общее название профессии: {profession_full}")
    parts.append(
        f"Должность на последнем месте: {last_job_position_name or 'не указано'}"
    )
    parts.append(f"Общий опыт работы: {total_experience or 'не указано'}")
    parts.append(
        f"Локация: {location_name_estaff or location_name_html or 'не указано'}"
    )

    parts.append("\n=== О СЕБЕ ===")
    parts.append(f"Описание кандидата: {about_text or 'не указано'}")

    parts.append("\n=== ОБУЧЕНИЕ ===")
    parts.append(f"Основное образование: {main_education_final}")
    parts.append(f"Дополнительное образование: {add_education_final}")

    parts.append("\n=== ОПЫТ РАБОТЫ ===")
    parts.append(experience_text)

    parts.append("\n=== КЛЮЧЕВЫЕ НАВЫКИ ===")
    parts.append(f"Список ключевых навыков: {skills_text or 'не указано'}")

    parts.append("\n=== ЯЗЫКИ ===")
    parts.append(f"Список языков: {languages_text or 'не указано'}")

    text = "\n".join(parts)
    text = norm_multiline(text)
    return text
