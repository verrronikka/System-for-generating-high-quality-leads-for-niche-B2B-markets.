import pandas as pd
import os
import sqlite3
from datetime import datetime


class EmailComposer:
    """Генератор персонализированных коммерческих писем для B2B-лидов.

    Класс анализирует данные о компании (импорт, продукты, страны, активность)
    и создаёт адаптированное письмо с учётом уровня персонализации.
    """

    def __init__(self, sender_company="Ваша Компания",
                 sender_name="Менеджер по развитию",
                 product_offering="логистические решения для импортёров"):
        """Инициализирует настройки отправителя и предложения.

        Args:
            sender_company (str): Название компании-отправителя.
            sender_name (str): Имя менеджера.
            product_offering (str): Описание основного продукта/услуги.
        """
        self.sender_company = sender_company
        self.sender_name = sender_name
        self.product_offering = product_offering

    def generate_email(self, company_data):
        """Генерирует тему и тело письма на основе данных о компании.

        Args:
            company_data (dict): Словарь с полями:
                - 'name' (str, optional): Название компании.
                - 'is_importer' (bool): Является ли импортёром.
                - 'product_mentions' (str): Список продуктов в виде строки "[...]".
                - 'mentioned_countries' (str): Список стран в виде строки "[...]".
                - 'has_financial_indicators' (bool): Признак финансовой активности.
                - 'recent_activity' (bool): Признак недавней активности.

        Returns:
            dict: Словарь с ключами:
                - 'subject' (str): Тема письма.
                - 'body' (str): Тело письма.
                - 'personalization_score' (int): Уровень персонализации (0–9).
                - 'generated_at' (str): Время генерации в ISO-формате.
        """
        name = company_data.get('name', 'Уважаемые коллеги')
        if not name or name == 'N/A' or name.strip() == '':
            name = 'Уважаемые коллеги'

        is_importer = company_data.get('is_importer', False)
        products = self._parse_list(company_data.get('product_mentions', '[]'))
        countries = self._parse_list(company_data.get('mentioned_countries',
                                                      '[]'))
        has_financial = company_data.get('has_financial_indicators', False)
        recent_activity = company_data.get('recent_activity', False)

        personalization_score = 0
        facts = []

        if is_importer:
            facts.append("занимаетесь импортом")
            personalization_score += 3

        if products:
            products_str = ", ".join(products[:3])
            facts.append(f"работаете с {products_str}")
            personalization_score += 2

        if countries:
            countries_str = ", ".join(countries[:2])
            facts.append(f"сотрудничаете с партнёрами из {countries_str}")
            personalization_score += 2

        if has_financial:
            facts.append("ведёте активную коммерческую деятельность")
            personalization_score += 1

        if recent_activity:
            facts.append("развиваете бизнес в текущем году")
            personalization_score += 1

        subject = self._generate_subject(is_importer, products,
                                         personalization_score)

        body = self._generate_body(name, facts, personalization_score)

        return {
            'subject': subject,
            'body': body,
            'personalization_score': personalization_score,
            'generated_at': datetime.now().isoformat()
        }

    def _parse_list(self, list_str):
        """Преобразует строку вида \"[элемент1, элемент2]\" в список.

        Args:
            list_str (str): Строка, содержащая список в квадратных скобках.

        Returns:
            list[str]: Список очищенных строк без кавычек и лишних пробелов.
                       Возвращает пустой список, если вход пуст или '[]'.
        """
        if not list_str or list_str == '[]':
            return []

        clean = list_str.strip("[]").replace("'", "").replace('"', '')
        items = [item.strip() for item in clean.split(',') if item.strip()]
        return items

    def _generate_subject(self, is_importer, products, score):
        """Формирует тему письма в зависимости от уровня персонализации.

        Args:
            is_importer (bool): Является ли компания импортёром.
            products (list[str]): Список упомянутых продуктов.
            score (int): Уровень персонализации (0–9).

        Returns:
            str: Тема письма.
        """
        if score >= 5:
            if products:
                return (f"Оптимизация поставок {products[0]}. "
                        f"Персональное предложение")
            return "Специальное предложение для вашей компании"
        elif is_importer:
            return "Решения для импортёров. Снижение затрат до 20%"
        else:
            return f"Предложение от {self.sender_company}"

    def _generate_body(self, company_name, facts, score):
        """Формирует тело письма с учётом персонализированных фактов.

        Args:
            company_name (str): Название компании или обращение по умолчанию.
            facts (list[str]): Список персонализированных фактов о компании.
            score (int): Уровень персонализации.

        Returns:
            str: Полный текст письма (включая приветствие, ценность, CTA и подпись).
        """
        greeting = f"Здравствуйте, {company_name}!\n\n"

        if facts:
            facts_text = ", ".join(facts)
            intro = (f"Мы заметили, что вы {facts_text}. "
                     f"Это делает наше предложение "
                     f"особенно актуальным для вас.\n\n")
        else:
            intro = "Мы внимательно изучили профиль вашей компании.\n\n"

        if score >= 5:
            value_prop = (
                f"{self.sender_company} "
                f"специализируется на {self.product_offering}. "
                f"Учитывая специфику вашего бизнеса, мы можем предложить:\n\n"
                f"+ Снижение затрат на логистику до 20%\n"
                f"+ Оптимизацию таможенного оформления\n"
                f"+ Надёжных партнёров для комплексных поставок\n\n"
            )
        elif score >= 3:
            value_prop = (
                f"{self.sender_company} помогает "
                f"компаниям оптимизировать {self.product_offering}.\n\n"
                f"Наши клиенты в среднем:\n"
                f"+ Экономят до 15% на логистике\n"
                f"+ Сокращают время доставки на 30%\n"
                f"+ Минимизируют риски при импорте\n\n"
            )
        else:
            value_prop = (
                f"{self.sender_company} "
                f"предлагает {self.product_offering}.\n\n"
                f"Мы будем рады обсудить возможности сотрудничества.\n\n"
            )

        cta = (
            "Готовы рассказать о конкретных решениях "
            "для вашей компании.\n\n"
            "Удобно ли вам созвониться на этой неделе?\n\n"
        )

        signature = (
            f"С уважением,\n"
            f"{self.sender_name}\n"
            f"{self.sender_company}\n"
            f"Email: manager@example.com\n"
            f"Тел: +7 (XXX) XXX-XX-XX"
        )

        return greeting + intro + value_prop + cta + signature


def generate_emails_batch(
        enriched_sql_path='data/processed/enriched_companies.db',
        validated_csv_path='data/processed/validated.csv',
        raw_sql_path='data/raw/companies_demo.db',
        output_path='data/processed/generated_emails.csv',
        only_validated=True):
    """Генерирует персонализированные письма для компаний на основе обогащённых данных.

    Объединяет данные из enriched-БД, raw-БД и (опционально) валидированного CSV.
    Генерирует письма только для компаний, помеченных как активные импортёры,
    если only_validated=True.

    Args:
        enriched_sql_path (str): Путь к SQLite-файлу с обогащёнными данными.
        validated_csv_path (str): Путь к CSV с результатами валидации.
        raw_sql_path (str): Путь к SQLite-файлу с сырыми данными (для названий).
        output_path (str): Путь для сохранения результата в CSV.
        only_validated (bool): Если True — генерировать только для валидированных.

    Returns:
        pd.DataFrame or None: DataFrame с письмами или None, если нет данных.
    """
    try:
        conn1 = sqlite3.connect(enriched_sql_path)
        conn2 = sqlite3.connect(raw_sql_path)
        enriched = pd.read_sql("SELECT * FROM companies", conn1)

        raw = pd.read_sql("SELECT * FROM companies", conn2)

        enriched = enriched.merge(raw[['inn', 'name_short']],
                                  left_on='company_id', right_on='inn',
                                  how='left').drop(columns=['inn'])
    except FileNotFoundError as e:
        print(f"Файл не найден: {e}")
        return None

    if only_validated and os.path.exists(validated_csv_path):
        validated = pd.read_csv(validated_csv_path)

        active_importers = validated[
            validated['is_active_importer'] == 'Да'
        ]['company_id'].astype(str).tolist()

        enriched = enriched[enriched['company_id'].astype(str).isin(active_importers)]

        print(f"Генерируем письма для {len(enriched)} валидированных компаний")
    else:
        print(f"Генерируем письма для всех {len(enriched)} компаний")

    if len(enriched) == 0:
        print("Нет компаний для генерации писем")
        return None

    composer = EmailComposer()
    results = []

    for idx, row in enriched.iterrows():
        company_data = row.to_dict()
        email = composer.generate_email(company_data)

        results.append({
            'company_id': company_data['company_id'],
            'company_name': company_data.get('name_short', 'N/A'),
            'subject': email['subject'],
            'body': email['body'],
            'personalization_score': email['personalization_score'],
            'generated_at': email['generated_at'],
            'sent_status': 'pending'
        })

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False, encoding='utf-8')

    print(f"Сгенерировано {len(results)} писем")
    print(f"Сохранено в {output_path}")

    avg_score = results_df['personalization_score'].mean()
    print(f"Средний уровень персонализации: {avg_score:.1f}/9")

    return results_df


if __name__ == '__main__':
    print("="*60)
    print("ГЕНЕРАТОР ПЕРСОНАЛИЗИРОВАННЫХ ПИСЕМ")
    print("="*60)

    df = generate_emails_batch(only_validated=True)

    if df is not None and len(df) > 0:
        print("\nПример письма:")
        print("-"*60)
        print(f"Кому: {df.iloc[0]['company_name']}")
        print(f"Тема: {df.iloc[0]['subject']}")
        print(f"\n{df.iloc[0]['body']}")
        print("-"*60)