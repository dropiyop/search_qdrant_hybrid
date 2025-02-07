import logging
import os
import re
import nltk
import nltk.corpus
import yake
import json


class FileParser:


    def __init__(self, max_length: int, directory_path: str = None, file_path: str = None):
        if directory_path is None and file_path is None:
            raise ValueError("Вы должны указать либо путь к файлу, либо путь к папке с файлами")

        self.directory_path = directory_path
        self.file_path = file_path
        self.max_length = max_length
        self.points_batch = []

        language = "ru"
        max_ngram_size = 3
        deduplication_threshold = 0.5
        deduplication_algo = 'seqm'
        windowSize = 1
        numOfKeywords = 20
        self.custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size,
                                                         dedupLim=deduplication_threshold,
                                                         dedupFunc=deduplication_algo, windowsSize=windowSize,
                                                         top=numOfKeywords, features=None)

        if self.directory_path:
            self.__upload_documents_from_directory()



    def tokenize_text(self, text: str) -> list:
        tokens = self.custom_kw_extractor.extract_keywords(text)
        return [token[0] for token in tokens]

    def __upload_documents_from_directory(self):
        for filename in os.listdir(self.directory_path):
            file_path = os.path.join(self.directory_path, filename)
            if not os.path.isfile(file_path):
                continue

            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Проверяем, начинается ли файл с `[` и заканчивается `]`
            if not (content.startswith("[") and content.endswith("]")):
                logging.error(f"Ошибка формата JSON в файле: {filename}")
                continue

            content = re.sub(r"(?<!\\)'", '"', content)

            content = re.sub(r",\s*]", "]", content)  # Убираем лишнюю запятую перед закрывающей скобкой
            content = re.sub(r"}\s*{", "}, {", content)  # Добавляем запятые между объектами

            try:
                json_data = json.loads(content)  # Парсим JSON-массив
                if not isinstance(json_data, list):
                    logging.error(f"Ошибка: Ожидался массив JSON в файле: {filename}")
                    continue
            except json.JSONDecodeError as e:
                logging.error(f"Ошибка парсинга JSON в файле {filename}: {e}")
                continue


            # Определяем source: если в JSON-данных есть, берем его, иначе - filename
            source = filename
            if json_data and "source" in json_data[0]:
                source = json_data[0]["source"]

            for idx, json_object in enumerate(json_data):
                if not isinstance(json_object, dict):
                    logging.warning(f"Пропущен некорректный JSON-объект в файле {filename}")
                    continue

                try:
                    project_name, wiki_name = filename[:-4].split("()")
                    wiki_name = wiki_name.rsplit('.')[0]
                except ValueError:
                    logging.error(f"Ошибка формата имени файла: {filename}")
                    continue

                self.points_batch.append({
                    "source": source,
                    "type_source": 'текстовый файл',
                    "content": json_object.get("content", "").lower(),
                    "title": f"{project_name} {wiki_name} - объект {idx+1}",
                    "tokens": self.tokenize_text(json_object.get("content", "")),
                    "project": project_name,
                    "name": wiki_name,
                })

                logging.debug(f"Загружен {filename} объект {idx+1}.")

    def __smart_chunk_text(self, text: str):
        paragraphs = text.split("\n\n")
        final_chunks = []

        for paragraph in paragraphs:
            if len(paragraph) <= self.max_length:
                final_chunks.append(paragraph)
            else:
                lines = paragraph.split("\n")
                for line in lines:
                    if len(line) <= self.max_length:
                        final_chunks.append(line)
                    else:
                        words = line.split()
                        tmp = []
                        count = 0
                        for w in words:
                            if count + len(w) < self.max_length:
                                tmp.append(w)
                                count += len(w) + 1
                            else:
                                final_chunks.append(" ".join(tmp))
                                tmp = [w]
                                count = len(w)
                        if tmp:
                            final_chunks.append(" ".join(tmp))

        return final_chunks

    def __merge_chunks(self, chunks):
        merged_chunks = []
        current_chunk = ""

        for chunk in chunks:
            if len(current_chunk) + len(chunk) + (1 if current_chunk else 0) <= self.max_length:
                current_chunk += (" " + chunk if current_chunk else chunk)
            else:
                merged_chunks.append(current_chunk)
                current_chunk = chunk

        if current_chunk:
            merged_chunks.append(current_chunk)

        return merged_chunks


if __name__ == "__main__":
    file_parser = FileParser(max_length=3000, directory_path='../data')
    print(file_parser.points_batch)
