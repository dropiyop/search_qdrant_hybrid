import re

import nltk
import qdrant_client
import qdrant_client.models
import enum
import logging
import typing
import uuid




class TypeOfSource(enum.Enum):
    SITE = "сайт"
    TXT_FILE = "текстовый файл"
    PDF_FILE = "pdf файл"
    EXCEL_FILE = "excel таблица"


class DataObject:
    content: str
    type_source: TypeOfSource
    source: str
    # tokens: list

    def __init__(self, content: str, type_source: TypeOfSource, source: str):
        self.content = content
        self.type_source = type_source
        self.source = source
        # self.tokens = tokens

    @classmethod
    def from_dict(cls, item: dict):
        if not all(key in item.keys() for key in cls.get_fields()):
            raise ValueError(f"В объекте обязательно должны присутствовать поля {", ".join(cls.get_fields())}")
        return cls(
            content=str(item['content']),
            type_source=TypeOfSource(item['type_source']),
            source=str(item['source']))

    @classmethod
    def get_fields(cls):
        return list(cls.__annotations__.keys())

    def __iter__(self) -> typing.Iterator[tuple[str, any]]:
        yield 'content', self.content
        yield 'type_source', self.type_source.value
        yield 'source', self.source
        # yield 'tokens', self.tokens

    def __getitem__(self, key: str) -> typing.Any:
        if key == 'content':
            return self.content
        elif key == 'type_source':
            return self.type_source.value
        elif key == 'source':
            return self.source
        # elif key == 'tokens':
        #     return self.tokens
        else:
            raise KeyError(f"Ключ '{key}' не найден в DataObject.")


class RedmineWikiObject(DataObject):
    content: str
    type_source: TypeOfSource
    source: str
    title: str
    project: str
    name: str

    def __init__(self, content: str, type_source: TypeOfSource, source: str,
                 title: str, project: str, name: str, tokens: list):
        super().__init__(content, type_source, source, tokens)
        self.title = title
        self.project = project
        self.name = name

    @classmethod
    def from_dict(cls, item: dict):
        if not all(key in item.keys() for key in cls.get_fields()):
            raise ValueError(f"В объекте обязательно должны присутствовать поля {", ".join(cls.get_fields())}")
        return cls(
            content=str(item['content']),
            type_source=TypeOfSource(item['type_source']),
            source=str(item['source']),
            title=str(item['title']),
            project=str(item['project']),
            name=str(item['name']),
            tokens=list(item['tokens']))

    def __iter__(self) -> typing.Iterator[tuple[str, any]]:
        yield 'content', self.content
        yield 'type_source', self.type_source.value
        yield 'source', self.source
        yield 'tokens', self.tokens
        yield 'title', self.title
        yield 'project', self.project
        yield 'name', self.name

    def __getitem__(self, key: str) -> typing.Any:
        if key == 'content':
            return self.content
        elif key == 'type_source':
            return self.type_source.value
        elif key == 'source':
            return self.source
        elif key == 'title':
            return self.title
        elif key == 'project':
            return self.project
        elif key == 'name':
            return self.name
        elif key == 'tokens':
            return self.tokens
        else:
            raise KeyError(f"Ключ '{key}' не найден в DataObject.")


class VectorInfo:
    name: str
    size: int
    distance: qdrant_client.models.Distance

    def __init__(self, name: str, size: int, name_for_embed: str, client_embed, type_of_object: typing.Type[DataObject],
                 distance: qdrant_client.models.Distance = qdrant_client.models.Distance.COSINE):
        if not any(key == name_for_embed for key in type_of_object.get_fields()):
            raise ValueError(f"Вы должны указать для какого поля будет происходить векторизация: "
                             f"{', '.join(type_of_object.get_fields())}")

        self.name = name
        self.name_for_embed = name_for_embed
        self.size = size
        self.distance = distance
        self.client = client_embed

    async def get_embedding(self, text: str, model: str = "text-embedding-3-small") -> list[float]:
        response = await self.client.embeddings.create(
            input=text,
            model=model
        )
        return response.data[0].embedding


class VectorInfoOllama(VectorInfo):
    def __init__(self, name: str, size: int, name_for_embed: str, client_embed,
                 type_of_object: typing.Type[DataObject],
                 distance: qdrant_client.models.Distance = qdrant_client.models.Distance.COSINE):
        super().__init__(name, size, name_for_embed, client_embed, type_of_object, distance)

    async def get_embedding(self, text: str, model: str = None) -> list[float]:
        response = self.client.get_text_embedding(text)
        return response

    async def get_size(self) -> int:
        vector = self.client.get_text_embedding('test')
        return len(vector)


class QdClient:
    def __init__(self, gd_host: str, qd_port: int, qd_key: str,
                 vector_config: list[VectorInfo], type_of_object: typing.Type[DataObject]):
        self.qdrant = qdrant_client.AsyncQdrantClient(
            url=f"http://{gd_host}:{qd_port}",
            api_key=qd_key)
        self.vector_config = vector_config
        self.type_of_object = type_of_object

    async def create_collection(self, collection_name: str):
        response = await self.qdrant.get_collections()
        existing_collections = [collection.name for collection in response.collections]

        if collection_name in existing_collections:
            logging.warning(f"Коллекция '{collection_name}' уже существует.")
        else:
            vector_config = dict()
            for vector in self.vector_config:
                vector_config[vector.name] = qdrant_client.models.VectorParams(
                    size=vector.size, distance=vector.distance)

            await self.qdrant.create_collection(
                collection_name=collection_name,
                vectors_config=vector_config)

            # await self.qdrant.create_payload_index(
            #     collection_name=collection_name,
            #     field_name="tokens",
            #     field_schema="keyword")

            logging.info(f"Коллекция '{collection_name}' успешно создана.")

    async def delete_by_filter(self, filter_data: dict, collection_name: str):
        filtering = []
        for key, value in filter_data.items():
            filtering.append(qdrant_client.models.FieldCondition(
                key=key,
                match=qdrant_client.models.MatchValue(value=value)))

        await self.qdrant.delete(
            collection_name=collection_name,
            points_selector=qdrant_client.models.FilterSelector(filter=qdrant_client.models.Filter(must=filtering)))

    async def delete_by_ids(self, ids: list[str], collection_name: str):
        await self.qdrant.delete(
            collection_name=collection_name,
            points_selector=qdrant_client.models.PointIdsList(points=ids))

    async def delete_collection(self, collection_name: str):
        await self.qdrant.delete_collection(collection_name)

    async def search(self, text: str, collection_name: str, using: str, limit: int = 3):
        for vector in self.vector_config:
            if vector.name_for_embed == using:
                text_vector = await vector.get_embedding(text.lower())
                res = await self.qdrant.query_points(
                    collection_name=collection_name,
                    query=text_vector,
                    using=vector.name,
                    limit=limit)
                break
        else:
            raise ValueError(f"Некорректное название для векторизуемого поля - {using}")

        return res.points

    async def hybrid_search(self, text: str, collection_name: str,
                            content_field: str, limit: int = 3):
        content_vector = None

        content_vector_name = None

        for vector in self.vector_config:
            if vector.name_for_embed == content_field:
                content_vector = await vector.get_embedding(text.lower())
                content_vector_name = vector.name

        if content_vector is None:
            raise ValueError(f"Некорректное название для векторизуемого поля (content): {content_field}")

        stop_words = set(nltk.corpus.stopwords.words('russian'))
        tokens = re.findall(r'\w+', text.lower())
        query_tokens = [t for t in tokens if t not in stop_words]
        filtering_conditions = []
        for token in query_tokens:
            filtering_conditions.append(
                qdrant_client.models.FieldCondition(
                    key="tokens",
                    match=qdrant_client.models.MatchValue(value=token)
                )
            )
        token_filter = qdrant_client.models.Filter(should=filtering_conditions)

        content_prefetch = qdrant_client.models.Prefetch(
            query=content_vector,
            using=content_vector_name,
            limit=limit
        )

        # Выполняем итоговый запрос, передавая prefetch и фильтр по токенам
        res = await self.qdrant.query_points(
            collection_name=collection_name,
            prefetch=[content_prefetch],
            query=content_vector,  # основной вектор – можно выбрать один из них
            using=content_vector_name,  # используем для итогового сравнения один из полей
            limit=limit,
            query_filter=token_filter
        )

        return res.points

    async def must_search(self, filter_data: dict, collection_name: str):
        filtering = []
        for key, value in filter_data.items():
            filtering.append(qdrant_client.models.FieldCondition(
                key=key,
                match=qdrant_client.models.MatchValue(value=value)))

        filter_search = qdrant_client.models.Filter(must=filtering)

        response = await self.qdrant.scroll(
            collection_name=collection_name,
            scroll_filter=filter_search)

        return response[0]

    async def add_points(self, points_batch: list[dict], collection_name: str):
        correct_data_object = []
        for point in points_batch:
            correct_data_object.append(self.type_of_object.from_dict(point))

        correct_points = []
        for index, point in enumerate(correct_data_object, 1):
            correct_points.append(self.create_point(
                doc_id=str(uuid.uuid4()),
                vector={
                    vector.name: await vector.get_embedding(point[vector.name_for_embed])
                    for vector in self.vector_config},
                data_object=point))

        await self.qdrant.upsert(
            collection_name=collection_name,
            points=correct_points)

        logging.info(f"Успешно записано {len(correct_points)} чанков в коллекцию '{collection_name}'.")

    @staticmethod
    def create_point(doc_id: str | int, vector: list | dict[str, list], data_object: DataObject):
        return qdrant_client.models.PointStruct(id=doc_id, vector=vector, payload=dict(data_object))

