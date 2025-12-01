import pandas as pd
import json
import os
import sqlite3
from enricher import CompanyEnricher


def run_pipeline(enricher_cls=CompanyEnricher):
    """Запускает полный пайплайн обогащения данных о компаниях.

    1. Создаёт директорию для результатов.
    2. Загружает сырые данные из SQLite-файла `companies_demo.db`.
    3. Применяет NLP-анализ к текстовому полю каждой компании.
    4. Сохраняет обогащённые признаки в новую SQLite-базу.

    Args:
        enricher_cls (type): Класс обогатителя, реализующий метод `analyze(text, id)`.
                             По умолчанию — `CompanyEnricher`.

    Returns:
        pd.DataFrame: DataFrame с обогащёнными признаками компаний.

    Raises:
        FileNotFoundError: Если исходная БД не найдена.
        ValueError: Если ни одна компания не была успешно обработана.
        Exception: При ошибках чтения/записи БД или инициализации обогатителя.
    """
    try:
        os.makedirs('data/processed', exist_ok=True)
    except Exception as e:
        print(f"Ошибка создания директории data/processed: {e}")
        raise

    try:
        conn1 = sqlite3.connect('data/raw/companies_demo.db')
        df = pd.read_sql("SELECT * FROM companies", conn1)
    except FileNotFoundError:
        raise FileNotFoundError(
            "Файл data/raw/companies_demo.db не найден. "
            "Убедитесь, что он существует."
        )
    except Exception as e:
        print(f"Ошибка чтения SQL: {e}")
        raise

    try:
        enricher = enricher_cls()
    except Exception as e:
        print(f"Ошибка инициализации enricher: {e}")
        raise

    results = []
    for company in df.itertuples(index=False):
        try:
            text = company.news
            company_id = company.inn
            features = enricher.analyze(text, company_id)
            results.append(features)
        except AttributeError as e:
            print(f"Ошибка структуры данных: {e}. Пропускаем запись.")
            continue
        except Exception as e:
            print(f"Ошибка обработки компании {company_id}: {e}")
            continue

    if not results:
        raise ValueError("Не удалось обработать ни одной компании")

    try:
        conn2 = sqlite3.connect('data/processed/enriched_companies.db')
        results_df = pd.DataFrame(results)

        # Преобразуем списки в строки для совместимости с SQLite
        for col in results_df.columns:
            results_df[col] = results_df[col].apply(
                lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x
            )
        
        results_df.to_sql("companies", conn2, if_exists="replace", index=False)
        conn2.close()
    except Exception as e:
        print(f"Ошибка записи результатов: {e}")
        raise

    return results_df


def report(df):
    """Генерирует и сохраняет краткий отчёт по результатам обогащения.

    Отчёт включает:
    - общее число компаний,
    - число выявленных импортёров,
    - средний уровень бизнес-активности.

    Args:
        df (pd.DataFrame): DataFrame с обогащёнными данными (должен содержать
                           колонки 'is_importer' и 'activity_indicators').

    Side Effects:
        - Выводит отчёт в консоль.
        - Сохраняет JSON-отчёт в `data/processed/enrichment_report.json`.
    """
    report = {
        'total_companies': int(len(df)),
        'importers': int(df['is_importer'].sum()),
        'average_activity_score': float(df['activity_indicators'].mean())
    }

    print("Enrichment Report:")
    for key, value in report.items():
        print(f"{key}: {value}")

    try:
        with open('data/processed/enrichment_report.json',
                  'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Ошибка записи отчёта: {e}")


if __name__ == '__main__':
    """Точка входа для запуска пайплайна обогащения данных.

    Выполняет:
    1. Обогащение сырых данных из `data/raw/companies_demo.db`.
    2. Сохранение результата в `data/processed/enriched_companies.db`.
    3. Генерацию итогового отчёта.

    При возникновении критической ошибки завершает процесс с кодом 1.
    """
    try:
        res = run_pipeline(CompanyEnricher)
        report(res)
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        exit(1)