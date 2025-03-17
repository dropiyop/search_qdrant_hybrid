import logging
import os
import re
import yake
import bs4


class FileParser:
    def __init__(self, max_length: int = None, directory_path: str = None, file_path: str = None):
        self.directory_path = directory_path
        self.file_path = file_path
        self.max_length = max_length
        self.points_batch = []

        self.custom_kw_extractor = yake.KeywordExtractor(
            lan="ru", n=3, dedupLim=0.5, dedupFunc='seqm', windowsSize=1, top=20, features=None)

        if self.directory_path:
            self.__upload_documents_from_directory()

    def tokenize_text(self, text: str) -> list:
        tokens = self.custom_kw_extractor.extract_keywords(text)
        return [token[0] for token in tokens]

    @classmethod
    def clean_html(cls, html, base_url="https://www.1c-uc3.ru", url_transformer=None):
        if html is None:
            return None

        soup = bs4.BeautifulSoup(html, 'html.parser')
        for a_tag in soup.find_all('a'):
            link = a_tag.get('href')
            if link:
                if link.startswith('/') or not (link.startswith('http://') or link.startswith('https://')):
                    if base_url.endswith('/') and link.startswith('/'):
                        full_link = base_url + link[1:]
                    else:
                        full_link = base_url + ('' if base_url.endswith('/') or link.startswith('/') else '/') + link

                    if url_transformer is not None:
                        full_link = url_transformer(full_link)
                    a_tag.replace_with(soup.new_string(full_link))
                else:
                    if url_transformer is not None:
                        link = url_transformer(link)
                    a_tag.replace_with(soup.new_string(link))
            else:
                a_tag.decompose()

        for tag in soup.find_all(['p', 'div', 'span', 'img', 'br']):
            tag.unwrap()

        for tag in soup.find_all(True):
            tag.attrs = {}

        result = str(soup)
        result = result.replace('\xa0', ' ').replace('&nbsp;', ' ')
        result = result.replace('\r\n', '\n')
        result = result.replace(' "', " «").replace('"', "»")

        return result

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
