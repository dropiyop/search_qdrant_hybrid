import logging
import os
import re
import yake


class FileParser:
    def __init__(self, max_length: int = None, directory_path: str = None, file_path: str = None):
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

            blocks = []
            if len(content) > 512:
                header_matches = list(re.finditer(r'^h[2-3]\..*', content, flags=re.MULTILINE))

                if header_matches:
                    if len(header_matches) > 1:
                        end_pos = header_matches[1].start()
                    else:
                        end_pos = len(content)
                    first_block = content[0:end_pos].strip()
                    blocks.append(first_block)

                    for i in range(1, len(header_matches)):
                        start = header_matches[i].start()
                        if i + 1 < len(header_matches):
                            end = header_matches[i + 1].start()
                        else:
                            end = len(content)
                        block = content[start:end].strip()
                        blocks.append(block)
                else:
                    blocks.append(content.strip())
            else:
                blocks.append(content.strip())

            # blocks = self.__merge_chunks(blocks)
            for idx_block, block in enumerate(blocks):
                raw_chunks = self.__smart_chunk_text(block.strip())
                merged_chunks = self.__merge_chunks(raw_chunks)

                for idx_chunk, chunk in enumerate(merged_chunks):
                    if len(chunk) < 100 and 'http' not in chunk:
                        continue

                    project_name, wiki_name = filename[:-4].split("()")
                    wiki_name = wiki_name.rsplit('.')[0]

                    self.points_batch.append({
                        "source": filename,
                        "type_source": 'текстовый файл',
                        "content": chunk.lower(),
                        "title": f"{project_name} {wiki_name} - часть {idx_chunk+1}",
                        "tokens": self.tokenize_text(chunk),
                        "project": project_name,
                        "name": wiki_name,
                    })

                    logging.debug(f"Загружен {filename} часть {idx_chunk + 1}.")

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
