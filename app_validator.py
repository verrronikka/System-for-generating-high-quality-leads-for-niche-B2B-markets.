import streamlit as st
import pandas as pd
import os
import sqlite3
from datetime import datetime


ENRICHED_PATH = 'data/processed/enriched_companies.db'
RAW_PATH = 'data/raw/companies_demo.db'
VALIDATED_PATH = 'data/processed/validated.csv'


def load_data():
    """Загружает и объединяет обогащённые и сырые данные о компаниях.

    Читает:
    - обогащённые признаки из `enriched_companies.db` (колонка `company_id`);
    - сырые данные из `companies_demo.db` (колонки `inn`, `name_short`, `news`).

    Выполняет LEFT JOIN по `company_id = inn`.

    Returns:
        pd.DataFrame or None: Объединённый DataFrame с полными данными,
                              или None в случае ошибки.
    """
    try:
        conn1 = sqlite3.connect(ENRICHED_PATH)
        conn2 = sqlite3.connect(RAW_PATH)
        enriched = pd.read_sql("SELECT * FROM companies", conn1)
        raw = pd.read_sql("SELECT * FROM companies", conn2)

        merged = enriched.merge(raw[['inn', 'name_short', 'news']],
                                left_on='company_id', right_on='inn',
                                how='left')
        return merged
    except FileNotFoundError as e:
        st.error(f"Файл не найден: {e}")
        st.info("Запустите pipeline.py для генерации enriched_companies.csv")
        return None


def load_validated():
    """Загружает результаты ручной валидации из CSV-файла.

    Если файл не существует, возвращает пустой DataFrame с правильной структурой.

    Returns:
        pd.DataFrame: Таблица с колонками:
            - company_id
            - validator_name
            - is_active_importer
            - confidence
            - comment
            - validated_at
    """
    if os.path.exists(VALIDATED_PATH):
        return pd.read_csv(VALIDATED_PATH)
    else:
        return pd.DataFrame(columns=[
            'company_id', 'validator_name', 'is_active_importer',
            'confidence', 'comment', 'validated_at'
        ])


def save_validation(company_id, validator_name, is_active_importer,
                    confidence, comment):
    """Сохраняет или обновляет запись ручной валидации в CSV-файл.

    Если компания уже валидирована — обновляет существующую запись.
    Иначе — добавляет новую.

    Args:
        company_id (str or int): Идентификатор компании (ИНН).
        validator_name (str): Имя валидатора.
        is_active_importer (str): Значение из ["Да", "Нет", "Неизвестно"].
        confidence (str): Уровень уверенности ("Низкая", "Средняя", "Высокая").
        comment (str): Опциональный комментарий.

    Returns:
        bool: True — если сохранение прошло успешно.
    """
    validated = load_validated()

    if company_id in validated['company_id'].values:
        idx = validated[validated['company_id'] == company_id].index[0]
        validated.loc[idx] = [
            company_id, validator_name, is_active_importer,
            confidence, comment, datetime.now().isoformat()
        ]
    else:
        new_row = pd.DataFrame([{
            'company_id': company_id,
            'validator_name': validator_name,
            'is_active_importer': is_active_importer,
            'confidence': confidence,
            'comment': comment,
            'validated_at': datetime.now().isoformat()
        }])
        validated = pd.concat([validated, new_row], ignore_index=True)

    validated.to_csv(VALIDATED_PATH, index=False)
    return True


# Основной интерфейс Streamlit-приложения
if __name__ == "__main__":
    """Интерактивный валидатор B2B-лидов для ручной проверки NLP-результатов.

    Приложение позволяет:
    - Просматривать компании, обогащённые NLP-пайплайном.
    - Видеть автоматически извлечённые признаки (импорт, продукция, страны и др.).
    - Выполнять ручную валидацию: подтверждать, что компания — активный импортёр.
    - Сохранять результаты в CSV для последующей генерации персонализированных писем.

    Использует данные из:
    - data/processed/enriched_companies.db
    - data/raw/companies_demo.db
    - data/processed/validated.csv (создаётся при первом сохранении)
    """

    st.set_page_config(
        page_title="B2B Lead Validator",
        page_icon="🦈",
        layout="wide"
    )

    st.title("B2B Lead Validator ;з")
    st.markdown("Проверка и валидация извлечённых признаков компаний")

    data = load_data()

    if data is None:
        st.stop()

    validated = load_validated()

    st.sidebar.header("☀️ Статистика ☀️")
    total_companies = len(data)
    validated_count = len(validated)
    remaining = total_companies - validated_count

    st.sidebar.metric("Всего компаний", total_companies)
    st.sidebar.metric("Прошли валидацию", validated_count)
    st.sidebar.metric("Осталось", remaining)

    if remaining > 0:
        progress = validated_count / total_companies
        st.sidebar.progress(progress)
    else:
        st.sidebar.success("Все компании прошли валидацию!")

    st.sidebar.header("Навигация")

    not_validated = data[~data['company_id'].isin(validated['company_id'])]

    if len(not_validated) == 0:
        st.info("Все компании прошли валидацию!")
        selected_company_id = st.sidebar.selectbox(
            "Выберите компанию:",
            data['company_id'].tolist()
        )
    else:
        selected_company_id = st.sidebar.selectbox(
            "Выберите компанию (не прошли валидацию):",
            not_validated['company_id'].tolist()
        )

    company = data[data['company_id'] == selected_company_id].iloc[0]

    st.header(f"Компания: {company['name_short']}")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Исходный текст")
        st.text_area("Текст с сайта:", value=company['news'], height=200,
                     disabled=True)

    with col2:
        st.subheader("Автоматически извлечённые признаки")

        st.metric("Импортёр?", "Да" if company['is_importer'] else "Нет")

        st.write("**Продукция:**")
        if company['product_mentions'] and company['product_mentions'] != '[]':
            st.write(company['product_mentions'])
        else:
            st.write("_Не обнаружено_")

        st.write("**Страны:**")
        if company['mentioned_countries'] and \
           company['mentioned_countries'] != '[]':

            st.write(company['mentioned_countries'])
        else:
            st.write("_Не обнаружено_")

        st.metric("Показатели активности", company['activity_indicators'])
        st.metric("Финансовые показатели",
                  "Да" if company['has_financial_indicators'] else "Нет")
        st.metric("Недавняя активность",
                  "Да" if company['recent_activity'] else "Нет")

    st.divider()
    st.subheader("Ручная валидация")

    existing_validation = validated[validated['company_id'] == selected_company_id]

    if len(existing_validation) > 0:
        st.info(f"{existing_validation.iloc[0]['validator_name']} "
                f"выполнил валидацию "
                f"{existing_validation.iloc[0]['validated_at']}")

        st.write(f"Импорт: {existing_validation.iloc[0]['is_active_importer']}")
        st.write(f"Уверенность: {existing_validation.iloc[0]['confidence']}")
        st.write(f"Комментарий: {existing_validation.iloc[0]['comment']}")

        if st.checkbox("Изменить валидацию"):
            show_form = True
        else:
            show_form = False
    else:
        show_form = True

    if show_form:
        with st.form("validation_form"):
            col1, col2 = st.columns(2)

            with col1:
                validator_name = st.text_input("Ваше имя:", value="Validator")

                is_active_importer = st.radio(
                    "**Является ли компания активным импортёром?**",
                    options=["Да", "Нет", "Неизвестно"],
                    help="Основной вопрос для валидации"
                )

                confidence = st.select_slider(
                    "Уверенность в оценке:",
                    options=["Низкая", "Средняя", "Высокая"],
                    value="Высокая"
                )

            with col2:
                comment = st.text_area(
                    "Комментарий (опционально):",
                    placeholder="Почему..?",
                    height=150
                )

            submitted = st.form_submit_button("Сохранить валидацию",
                                              type="primary")

            if submitted:
                success = save_validation(
                    company_id=selected_company_id,
                    validator_name=validator_name,
                    is_active_importer=is_active_importer,
                    confidence=confidence,
                    comment=comment
                )

                if success:
                    st.success("Валидация сохранена!")
                    st.balloons()
                    st.rerun()

    if len(validated) > 0:
        st.divider()
        st.subheader("Все валидации")
        st.dataframe(validated, width="stretch")

        csv = validated.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Скачать валидации CSV",
            data=csv,
            file_name=f"validations_"
                      f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime='text/csv'
        )