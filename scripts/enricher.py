import spacy
import pandas as pd


class CompanyEnricher:
    """Анализирует текстовые данные о компаниях для извлечения признаков.

    Использует spaCy с русской моделью для NLP-обработки текстов
    и определяет:
    - является ли компания импортёром,
    - какие продукты упоминаются (электроника),
    - какие страны упоминаются,
    - наличие финансовых и активностных индикаторов.

    Attributes:
        nlp (spacy.Language): Загруженная русская NLP-модель.
        min_text_length (int): Минимальная длина текста для обработки.
    """

    def __init__(self, min_text_length=20):
        """Настраивает обогатитель, загружая spaCy-модель и настраивая ключи.

        Args:
            min_text_length (int): Минимальная длина текста для анализа.
                                   Короткие тексты пропускаются.
                                   По умолчанию - 20.

        Raises:
            OSError: Если модель 'ru_core_news_sm' не установлена.
        """
        print('Загружаем модель...')
        try:
            self.nlp = spacy.load('ru_core_news_sm')
        except OSError:
            raise OSError(
                "Модель ru_core_news_sm не найдена. "
                "Установите: python -m spacy download ru_core_news_sm"
            )
        self.min_text_length = min_text_length
        self.setup_keys()

    def setup_keys(self):
        """Инициализирует списки ключевых слов и лемм для анализа."""
        self.imp_kws = ['импорт', 'импортирование', 'ввоз', 'ввозить',
                        'импортировать', 'импортёр', 'закупка',
                        'закупать', 'поставляем из', 'поставка',
                        'поставлять', 'дистрибуция',
                        'дистрибьютор', 'поставщик',
                        'оптовый', 'оптом', 'опт', 'реэкспорт',
                        'экспорт/импорт']
        self.negations = {'не', 'нет', 'без', 'ни', 'никогда', 'никак'}
        self.electronics = ['микросхема', 'интегральная схема', 'ics', 'чип',
                            'плата', 'контроллер',
                            'процессор',
                            'память', 'модуль',
                            'радиодеталь', 'конденсатор', 'резистор',
                            'транзистор', 'адаптер', 'разъём', 'дисплей',
                            'экран',
                            'смартфон', 'телевизор', 'ноутбук', 'компьютер',
                            'компонент', 'сборка', 'электроника']
        self.countries = ['китай', 'кндр', 'китайская народная республика',
                          'тайвань', 'корея', 'южная корея', 'юж. корея',
                          'япония', 'германия', 'польша', 'сша',
                          'соединенные штаты', 'великобритания', 'европа']
        self.financial_lemmas = {'миллион', 'миллиард', 'тысяча',
                                 'рубль', 'доллар', 'евро',
                                 'млн', 'млрд', 'тыс'}

    def _detect_importer(self, doc):
        """Определяет, является ли компания импортёром, на основе анализа.

        Проверяет наличие ключевых слов импорта в каждом предложении,
        учитывая ближайшие слова на предмет отрицаний.

        Args:
            doc (spacy.tokens.Doc): Обработанный spaCy документ.

        Returns:
            bool: True, если найдены признаки импорта без отрицания.
        """
        for sent in doc.sents:
            sent_lemmas = [token.lemma_.lower() for token in sent]

            for keyword in self.imp_kws:
                if ' ' in keyword:
                    if keyword in sent.text:
                        return True
                else:
                    for i, lemma in enumerate(sent_lemmas):
                        if lemma == keyword.lower():
                            has_negation = False

                            if i >= 1 and sent_lemmas[i-1] in self.negations:
                                has_negation = True

                            if i >= 2 and sent_lemmas[i-2] in self.negations:
                                has_negation = True

                            if not has_negation:
                                return True
        return False

    def _extract(self, doc, keys):
        """Извлекает уникальные леммы из текста, совпадающие со списком.

        Args:
            doc (spacy.tokens.Doc): Обработанный spaCy документ.
            keys (list[str]): Список целевых лемм (строки в нижнем регистре).

        Returns:
            list[str]: Список уникальных найденных лемм.
        """
        found = []
        for token in doc:
            if token.lemma_.lower() in keys:
                found.append(token.lemma_.lower())
        return list(set(found))

    def _find_indicators(self, doc):
        """Подсчитывает количество бизнес-активностных индикаторов в тексте.

        Args:
            doc (spacy.tokens.Doc): Обработанный spaCy документ.

        Returns:
            int: Количество найденных индикаторов.
        """
        indicators = ['поставка', 'поставок', 'оборот', 'объём', 'объем',
                      'контракт', 'контракты', 'договор', 'договоры',
                      'реализовано', 'продано', 'продажи', 'продажа',
                      'клиент', 'клиенты', 'партнёр', 'партнёры', 'поставщ',
                      'закупк', 'закупки', 'заказ', 'заказы', 'проект',
                      'проекты', 'экспорт', 'импор']
        return sum(1 for token in doc if token.lemma_.lower() in indicators)

    def _has_financial(self, doc):
        """Проверяет наличие признаков финансовой активности.

        Args:
            doc (spacy.tokens.Doc): Обработанный spaCy документ.

        Returns:
            bool: True, если найдены финансовые леммы (млн, доллар и т.п.).
        """
        lemmas = {token.lemma_ for token in doc}
        return bool(lemmas & self.financial_lemmas)

    def _detect_recent_activity(self, doc):
        """Определяет, упоминается ли недавняя бизнес-активность.

        Args:
            doc (spacy.tokens.Doc): Обработанный spaCy документ.

        Returns:
            bool: True, если в тексте есть активность в 2024–2025 гг.
        """
        time_indicators = ['последний год', 'в этом году', 'за последний год',
                           'недавно', 'в прошлом году', '2025']
        return any(indicator in doc.text for indicator in time_indicators)

    def _empty(self, company_id):
        """Создаёт заглушку для компаний с недостаточным/пустым текстом.

        Args:
            company_id (any): Идентификатор компании.

        Returns:
            dict: Словарь признаков со значениями по умолчанию False/0/[].
        """
        return {
            "company_id": company_id,
            "is_importer": False,
            "product_mentions": [],
            "mentioned_countries": [],
            "activity_indicators": 0,
            "processed": False,
            "has_financial_indicators": False,
            "recent_activity": False
        }

    def analyze(self, text, company_id):
        """Анализирует текст компании и возвращает структурированные признаки.

        Args:
            text (str or None): Текст для анализа (например, контент сайта).
            company_id (any): Уникальный идентификатор компании.

        Returns:
            dict: Словарь с ключами:
                - 'company_id'
                - 'is_importer' (bool)
                - 'product_mentions' (list[str])
                - 'mentioned_countries' (list[str])
                - 'activity_indicators' (int)
                - 'processed' (bool)
                - 'has_financial_indicators' (bool)
                - 'recent_activity' (bool)
        """
        if not text or len(text) < self.min_text_length:
            return self._empty(company_id)

        try:
            doc = self.nlp(text.lower())
        except Exception as e:
            print(f"Ошибка обработки текста для company_id={company_id}: {e}")
            return self._empty(company_id)

        features = {
            "company_id": company_id,
            "is_importer": self._detect_importer(doc),
            "product_mentions": self._extract(doc, self.electronics),
            "mentioned_countries": self._extract(doc, self.countries),
            "activity_indicators": self._find_indicators(doc),
            "processed": True,
            "has_financial_indicators": self._has_financial(doc),
            "recent_activity": self._detect_recent_activity(doc)
        }

        return features


if __name__ == '__main__':
    """Запускает обогащение тестового CSV-файла и сохраняет результат.

    Читает 'data/raw/test_companies.csv' с колонками 'id' и 'website_text',
    обогащает каждую запись и сохраняет результат в
    'data/processed/enriched_companies.csv'.
    """
    enricher = CompanyEnricher()
    df = pd.read_csv('data/raw/test_companies.csv', encoding='utf-8')
    res = []
    for id, row in df.iterrows():
        text = row['website_text']
        company_id = row['id']
        features = enricher.analyze(text, company_id)
        res.append(features)

    res_df = pd.DataFrame(res)
    res_df.to_csv('data/processed/enriched_companies.csv', index=False,
                  encoding='utf-8')
