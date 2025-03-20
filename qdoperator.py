import asyncio
import datetime

import qdrant_client
import qdrant_client.models
import qdrant_client.conversions
import enum
import logging
import typing
import uuid


import qdparser


class TypeOfSource(enum.Enum):
    SITE = "сайт"
    TXT_FILE = "текстовый файл"
    PDF_FILE = "pdf файл"
    EXCEL_FILE = "excel таблица"
    KASKAD = "каскад"
    WORD_FILE = "Word файл"


class DataObject:
    type_source: TypeOfSource
    source: str
    tokens: list

    def __init__(self, type_source: TypeOfSource, source: str, tokens: list = None):
        self.type_source = type_source
        self.source = source
        self.tokens = tokens

    @classmethod
    def from_dict(cls, item: dict):
        if not all(key in item.keys() for key in cls.get_fields()):
            raise ValueError(f"В объекте обязательно должны присутствовать поля {", ".join(cls.get_fields())}")
        return cls(
            type_source=TypeOfSource(item['type_source']),
            source=str(item['source']),
            tokens=item.get('tokens', None))

    @classmethod
    def get_fields(cls):
        return list(cls.__annotations__.keys())

    def __iter__(self) -> typing.Iterator[tuple[str, any]]:
        yield 'type_source', self.type_source.value
        yield 'source', self.source
        yield 'tokens', self.tokens

    def __getitem__(self, key: str) -> typing.Any:
        if key == 'type_source':
            return self.type_source.value
        elif key == 'source':
            return self.source
        elif key == 'tokens':
            return self.tokens
        else:
            raise KeyError(f"Ключ '{key}' не найден в DataObject.")


class NomenclatureObjet(DataObject):
    content: str
    type_source: TypeOfSource
    source: str
    tokens: list
    n_id: str
    modified_at: float | None

    def __init__(self, content: str, type_source: TypeOfSource, source: str, n_id: str, modified_at: str | None):
        super().__init__(type_source, source)
        self.content = content
        self.n_id = n_id
        self.modified_at = datetime.datetime.strptime(modified_at, '%Y-%m-%dT%H:%M:%S').timestamp()

    @classmethod
    def from_dict(cls, item: dict):
        if not all(key in item.keys() for key in cls.get_fields()):
            raise ValueError(f"В объекте обязательно должны присутствовать поля {", ".join(cls.get_fields())}")
        return cls(
            content=str(item['content']),
            type_source=TypeOfSource(item['type_source']),
            source=str(item['source']),
            n_id=str(item['n_id']),
            modified_at=str(item['modified_at']))

    def __iter__(self) -> typing.Iterator[tuple[str, any]]:
        yield 'content', self.content
        yield 'type_source', self.type_source.value
        yield 'source', self.source
        yield 'tokens', self.tokens
        yield 'n_id', self.n_id
        yield 'modified_at', self.modified_at

    def __getitem__(self, key: str) -> typing.Any:
        if key == 'content':
            return self.content
        elif key == 'type_source':
            return self.type_source.value
        elif key == 'source':
            return self.source
        elif key == 'n_id':
            return self.n_id
        elif key == 'modified_at':
            return self.modified_at
        elif key == 'tokens':
            return self.tokens
        else:
            raise KeyError(f"Ключ '{key}' не найден в DataObject.")


class RedmineWikiObject(DataObject):
    content: str
    type_source: TypeOfSource
    source: str
    tokens: list
    title: str
    project: str
    name: str

    def __init__(self, content: str, type_source: TypeOfSource, source: str,
                 title: str, project: str, name: str, tokens: list = None):
        super().__init__(type_source, source, tokens)
        self.content = content
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
            tokens=item.get('tokens', None))

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


class UcCourseScheduleObject(DataObject):
    type_source: TypeOfSource
    source: str
    tokens: list
    course_id: int | None
    course_name: str
    schedule_id: int | None
    schedule_date_start: datetime.date
    schedule_date_finish: datetime.date
    schedule_time_start: datetime.time | None
    schedule_time_finish: datetime.time | None
    schedule_format_name: str
    schedule_format_description: str
    schedule_duration: str
    schedule_content_description: str
    schedule_price: list
    schedule_order_url: str | bool

    def __init__(
            self,
            type_source: TypeOfSource,
            source: str,
            course_id: int | None,
            course_name: str,
            schedule_id: int | None,
            schedule_date_start: datetime.date,
            schedule_date_finish: datetime.date,
            schedule_time_start: datetime.time,
            schedule_time_finish: datetime.time,
            schedule_format_name: str,
            schedule_format_description: str,
            schedule_duration: str,
            schedule_content_description: str,
            schedule_price: list,
            schedule_order_url: str | bool,
            tokens: list = None):
        super().__init__(type_source, source, tokens)
        self.course_id = course_id
        self.course_name = course_name
        self.schedule_id = schedule_id
        self.schedule_date_start = schedule_date_start
        self.schedule_date_finish = schedule_date_finish
        self.schedule_time_start = schedule_time_start
        self.schedule_time_finish = schedule_time_finish
        self.schedule_format_name = schedule_format_name
        self.schedule_format_description = schedule_format_description
        self.schedule_duration = schedule_duration
        self.schedule_content_description = schedule_content_description
        self.schedule_price = schedule_price
        self.schedule_order_url = schedule_order_url

    @classmethod
    def from_dict(cls, item: dict):
        if not all(key in item.keys() for key in cls.get_fields()):
            raise ValueError(f"В объекте обязательно должны присутствовать поля {", ".join(cls.get_fields())}")
        return cls(
            type_source=TypeOfSource(item['type_source']),
            source=str(item['source']),
            course_id=int(item['course_id']) if item.get('course_id') else None,
            course_name=item['course_name'],
            schedule_id=int(item['schedule_id']) if item.get('schedule_id') else None,
            schedule_date_start=datetime.datetime.strptime(item['schedule_date_start'], "%Y-%m-%d").date(),
            schedule_date_finish=datetime.datetime.strptime(item['schedule_date_finish'], "%Y-%m-%d").date(),
            schedule_time_start=datetime.datetime.strptime(
                item['schedule_time_start'], "%H:%M").time() if item['schedule_time_start'] else None,
            schedule_time_finish=datetime.datetime.strptime(
                item['schedule_time_finish'], "%H:%M").time() if item['schedule_time_finish'] else None,
            schedule_format_name=item['schedule_format_name'],
            schedule_format_description=item['schedule_format_description'],
            schedule_duration=item['schedule_duration'],
            schedule_content_description=item['schedule_content_description'],
            schedule_price=item['schedule_price'],
            schedule_order_url=item['schedule_order_url'],
            tokens=item.get('tokens', None))

    def __iter__(self) -> typing.Iterator[tuple[str, any]]:
        yield 'type_source', self.type_source.value
        yield 'source', self.source
        yield 'tokens', self.tokens
        yield 'course_id', self.course_id
        yield 'course_name', self.course_name
        yield 'schedule_id', self.schedule_id
        yield 'schedule_date_start', self.schedule_date_start
        yield 'schedule_date_finish', self.schedule_date_finish
        yield 'schedule_time_start', self.schedule_time_start
        yield 'schedule_time_finish', self.schedule_time_finish
        yield 'schedule_format_name', self.schedule_format_name
        yield 'schedule_format_description', self.schedule_format_description
        yield 'schedule_duration', self.schedule_duration
        yield 'schedule_content_description', self.schedule_content_description
        yield 'schedule_price', self.schedule_price
        yield 'schedule_order_url', self.schedule_order_url

    def __getitem__(self, key: str) -> typing.Any:
        if key == 'type_source':
            return self.type_source.value
        elif key == 'source':
            return self.source
        elif key == 'tokens':
            return self.tokens
        elif key == 'course_id':
            return self.course_id
        elif key == 'course_name':
            return self.course_name
        elif key == 'schedule_id':
            return self.schedule_id
        elif key == 'schedule_date_start':
            return self.schedule_date_start
        elif key == 'schedule_date_finish':
            return self.schedule_date_finish
        elif key == 'schedule_time_start':
            return self.schedule_time_start
        elif key == 'schedule_time_finish':
            return self.schedule_time_finish
        elif key == 'schedule_format_name':
            return self.schedule_format_name
        elif key == 'schedule_format_description':
            return self.schedule_format_description
        elif key == 'schedule_duration':
            return self.schedule_duration
        elif key == 'schedule_content_description':
            return self.schedule_content_description
        elif key == 'schedule_price':
            return self.schedule_price
        elif key == 'schedule_order_url':
            return self.schedule_order_url
        else:
            raise KeyError(f"Ключ '{key}' не найден в DataObject.")


class UcQuestion(DataObject):
    type_source: TypeOfSource
    source: str
    tokens: list
    question: str
    answer: str

    def __init__(
            self,
            type_source: TypeOfSource,
            source: str,
            question: str,
            answer: str,
            tokens: list = None):
        super().__init__(type_source, source, tokens)
        self.question = question
        self.answer = answer

    @classmethod
    def from_dict(cls, item: dict):
        if not all(key in item.keys() for key in cls.get_fields()):
            raise ValueError(f"В объекте обязательно должны присутствовать поля {", ".join(cls.get_fields())}")
        return cls(
            type_source=TypeOfSource(item['type_source']),
            source=str(item['source']),
            question=item['question'],
            answer=item['answer'],
            tokens=item.get('tokens', None))

    def __iter__(self) -> typing.Iterator[tuple[str, any]]:
        yield 'type_source', self.type_source.value
        yield 'source', self.source
        yield 'tokens', self.tokens
        yield 'question', self.question
        yield 'answer', self.answer

    def __getitem__(self, key: str) -> typing.Any:
        if key == 'type_source':
            return self.type_source.value
        elif key == 'source':
            return self.source
        elif key == 'tokens':
            return self.tokens
        elif key == 'question':
            return self.question
        elif key == 'answer':
            return self.answer
        else:
            raise KeyError(f"Ключ '{key}' не найден в DataObject.")

class AlexQuestion(DataObject):
    type_source: TypeOfSource
    source: str
    tokens: list
    category_name:str
    thread_name:str
    question: str
    answer: str

    def __init__(
            self,
            type_source: TypeOfSource,
            source: str,
            question: str,
            category_name: str,
            thread_name: str,
            answer: str,
            tokens: list = None
            ):

        super().__init__(type_source, source, tokens)
        self.question = question
        self.answer = answer
        self.category_name = category_name
        self.thread_name = thread_name


    @classmethod
    def from_dict(cls, item: dict):
        if not all(key in item.keys() for key in cls.get_fields()):
            raise ValueError(f"В объекте обязательно должны присутствовать поля {", ".join(cls.get_fields())}")
        return cls(
            type_source=TypeOfSource(item['type_source']),
            source=str(item['source']),
            question=item['question'],
            category_name = item['category_name'],
            thread_name = item['thread_name'],
            answer=item['answer'],
            tokens=item.get('tokens', None)
           )

    def __iter__(self) -> typing.Iterator[tuple[str, any]]:
        yield 'type_source', self.type_source.value
        yield 'source', self.source
        yield 'tokens', self.tokens
        yield 'category_name', self.category_name
        yield 'thread_name', self.thread_name
        yield 'question', self.question
        yield 'answer', self.answer


    def __getitem__(self, key: str) -> typing.Any:
        if key == 'type_source':
            return self.type_source.value
        elif key == 'source':
            return self.source
        elif key == 'tokens':
            return self.tokens
        elif key == 'category_name':
            return self.category_name
        elif key == 'thread_name':
            return self.thread_name
        elif key == 'question':
            return self.question
        elif key == 'answer':
            return self.answer
        else:
            raise KeyError(f"Ключ '{key}' не найден в DataObject.")


# class AlexQuestion(DataObject):
#     type_source: TypeOfSource
#     source: str
#     created_at: int
#     username: str
#     tokens: list
#     category_name:str
#     thread_name:str
#     question: str
#     answer: str
#
#     def __init__(
#             self,
#             type_source: TypeOfSource,
#             source: str,
#             question: str,
#             category_name: str,
#             thread_name: str,
#             answer: str,
#             tokens: list = None,
#             created_at: int = None,
#             username: str = None):
#
#         super().__init__(type_source, source, tokens)
#         self.question = question
#         self.answer = answer
#         self.username = username
#         self.category_name = category_name
#         self.thread_name = thread_name
#         self.created_at = created_at
#
#     @classmethod
#     def from_dict(cls, item: dict):
#         if not all(key in item.keys() for key in cls.get_fields()):
#             raise ValueError(f"В объекте обязательно должны присутствовать поля {", ".join(cls.get_fields())}")
#         return cls(
#             type_source=TypeOfSource(item['type_source']),
#             source=str(item['source']),
#             question=item['question'],
#             category_name = item['category_name'],
#             thread_name = item['thread_name'],
#             answer=item['answer'],
#             tokens=item.get('tokens', None),
#             created_at=item['created_at', None],
#             username= item.get('username', None))
#
#     def __iter__(self) -> typing.Iterator[tuple[str, any]]:
#         yield 'type_source', self.type_source.value
#         yield 'source', self.source
#         yield 'created_at', self.created_at
#         yield 'tokens', self.tokens
#         yield 'category_name', self.category_name
#         yield 'thread_name', self.thread_name
#         yield 'question', self.question
#         yield 'answer', self.answer
#         yield 'username', self.username
#
#     def __getitem__(self, key: str) -> typing.Any:
#         if key == 'type_source':
#             return self.type_source.value
#         elif key == 'source':
#             return self.source
#         elif key == 'created_at':
#             return self.created_at
#         elif key == 'username':
#             return self.username
#         elif key == 'tokens':
#             return self.tokens
#         elif key == 'category_name':
#             return self.category_name
#         elif key == 'thread_name':
#             return self.thread_name
#         elif key == 'question':
#             return self.question
#         elif key == 'answer':
#             return self.answer
#         else:
#             raise KeyError(f"Ключ '{key}' не найден в DataObject.")


class UcCourseObject(DataObject):
    type_source: TypeOfSource
    source: str
    tokens: list | None
    course_id: int | None
    course_name: str
    course_level: str | None
    course_teacher: list
    course_novelty: bool | None
    course_announcement: str | None
    course_accent: str | None
    course_announce: str | None
    course_for_who: str | None
    course_after: list
    course_notes: str | None
    course_moretext: str | None
    course_program_block: list
    course_formats: list | None
    course_url: str | None

    def __init__(
            self,
            type_source: TypeOfSource,
            source: str,
            course_id: int | None,
            course_name: str,
            course_level: str | None,
            course_teacher: list,
            course_novelty: bool | None,
            course_announcement: str | None,
            course_accent: str | None,
            course_announce: str | None,
            course_for_who: str | None,
            course_after: list,
            course_notes: str | None,
            course_moretext: str | None,
            course_program_block: list,
            course_url: str | None,
            course_formats: list | None,
            tokens: list = None):
        super().__init__(type_source, source, tokens)
        self.course_id: int | None = course_id
        self.course_name: str = course_name
        self.course_level: str | None = course_level
        self.course_teacher: list = course_teacher
        self.course_novelty: bool | None = course_novelty
        self.course_announcement: str | None = course_announcement
        self.course_accent: str | None = course_accent
        self.course_announce: str | None = course_announce
        self.course_for_who: str | None = course_for_who
        self.course_after: list = course_after
        self.course_notes: str | None = course_notes
        self.course_moretext: str | None = course_moretext
        self.course_program_block: list = course_program_block
        self.course_formats: list | None = course_formats
        self.course_url: str | None = course_url

    @classmethod
    def from_dict(cls, item: dict):
        if not all(key in item.keys() for key in cls.get_fields()):
            raise ValueError(f"В объекте обязательно должны присутствовать поля {", ".join(cls.get_fields())}")
        return cls(
            type_source=TypeOfSource(item['type_source']),
            source=str(item['source']),
            tokens=item.get('tokens', None),
            course_id=item.get('course_id', None),
            course_name=str(item['course_name']),
            course_level=item.get('course_level', None),
            course_teacher=item.get('course_teacher', []),
            course_novelty=item.get('course_novelty', None),
            course_announcement=item.get('course_announcement', None),
            course_accent=item.get('course_accent', None),
            course_announce=item.get('course_announce', None),
            course_for_who=item.get('course_for_who', None),
            course_after=item.get('course_after', []),
            course_notes=item.get('course_notes', None),
            course_moretext=item.get('course_moretext', None),
            course_program_block=item.get('course_program_block', []),
            course_url=item.get('course_url', None),
            course_formats=item.get('course_formats', None))

    def __iter__(self) -> typing.Iterator[tuple[str, any]]:
        yield 'type_source', self.type_source.value
        yield 'source', self.source
        yield 'tokens', self.tokens
        yield 'course_id', self.course_id
        yield 'course_name', self.course_name
        yield 'course_level', self.course_level
        yield 'course_teacher', self.course_teacher
        yield 'course_novelty', self.course_novelty
        yield 'course_announcement', self.course_announcement
        yield 'course_accent', self.course_accent
        yield 'course_announce', self.course_announce
        yield 'course_for_who', self.course_for_who
        yield 'course_after', self.course_after
        yield 'course_notes', self.course_notes
        yield 'course_moretext', self.course_moretext
        yield 'course_program_block', self.course_program_block
        yield 'course_url', self.course_url
        yield 'course_formats', self.course_formats

    def __getitem__(self, key: str) -> typing.Any:
        if key == 'type_source':
            return self.type_source.value
        elif key == 'source':
            return self.source
        elif key == 'course_id':
            return self.course_id
        elif key == 'course_name':
            return self.course_name
        elif key == 'course_level':
            return self.course_level
        elif key == 'tokens':
            return self.tokens
        elif key == 'course_teacher':
            return self.course_teacher
        elif key == 'course_novelty':
            return self.course_novelty
        elif key == 'course_announcement':
            return self.course_announcement
        elif key == 'course_accent':
            return self.course_accent
        elif key == 'course_announce':
            return self.course_announce
        elif key == 'course_for_who':
            return self.course_for_who
        elif key == 'course_after':
            return self.course_after
        elif key == 'course_notes':
            return self.course_notes
        elif key == 'course_moretext':
            return self.course_moretext
        elif key == 'course_program_block':
            return self.course_program_block
        elif key == 'course_url':
            return self.course_url
        elif key == 'course_formats':
            return self.course_formats
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
        if text is None:
            return [0.] * self.size

        response = await self.client.embeddings.create(
            input=text,
            model=model
        )
        return response.data[0].embedding

    async def get_size(self) -> int:
        vector = await self.get_embedding('test')
        return len(vector)


class VectorInfoOllama(VectorInfo):
    def __init__(self, name: str, size: int, name_for_embed: str, client_embed,
                 type_of_object: typing.Type[DataObject],
                 distance: qdrant_client.models.Distance = qdrant_client.models.Distance.COSINE):
        super().__init__(name, size, name_for_embed, client_embed, type_of_object, distance)

    async def get_embedding(self, text: str, model: str = None) -> list[float]:
        response = self.client.get_text_embedding(text)
        return response


class VectorInfoSelf(VectorInfo):
    def __init__(self, name: str, size: int, name_for_embed: str, client_embed,
                 type_of_object: typing.Type[DataObject]):
        super().__init__(name, size, name_for_embed, client_embed, type_of_object)

    async def get_embedding(self, text: str | list[str], model: str = None) -> list[float]:
        single_input = False
        if isinstance(text, str):
            text = [text]
            single_input = True

        embeddings = await asyncio.to_thread(self.client.encode, text)
        embeddings = embeddings.tolist()

        if single_input:
            return embeddings[0]
        else:
            return embeddings


class QdClient:
    def __init__(self, gd_host: str, qd_port: int, qd_key: str):
        self.qdrant = qdrant_client.AsyncQdrantClient(
            url=f"http://{gd_host}:{qd_port}",
            api_key=qd_key)
        self.collection_configs = {}

    async def create_collection(self, collection_name: str, vector_config: list,
                                type_of_object: typing.Type[DataObject],
                                payload_index: list | None = None):

        self.collection_configs[collection_name] = {
            "vector_config": vector_config,
            "type_of_object": type_of_object,
            "payload_index": payload_index
        }

        response = await self.qdrant.get_collections()
        existing_collections = [collection.name for collection in response.collections]

        if collection_name in existing_collections:
            logging.warning(f"Коллекция '{collection_name}' уже существует.")
        else:
            vectors_config = {
                vector.name: qdrant_client.models.VectorParams(
                    size=vector.size, distance=vector.distance
                )
                for vector in vector_config
            }

            await self.qdrant.create_collection(
                collection_name=collection_name,
                vectors_config=vectors_config)

            if payload_index:
                for index in payload_index:
                    await self.qdrant.create_payload_index(
                        collection_name=collection_name,
                        field_name=index['name'],
                        field_schema=index['schema'])

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
        config = self.collection_configs.get(collection_name)
        if config is None:
            raise ValueError(f"Конфигурация для коллекции '{collection_name}' не найдена.")
        vector_config = config["vector_config"]

        for vector in vector_config:
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
                            field_vectors: list, limit: int = 3):
        config = self.collection_configs.get(collection_name)
        if config is None:
            raise ValueError(f"Конфигурация для коллекции '{collection_name}' не найдена.")
        vector_config = config["vector_config"]
        payload_index = config["payload_index"]

        if not field_vectors:
            raise ValueError("Список field_vectors не должен быть пустым")

        text_lower = text.lower()
        main_vector = None
        main_vector_name = None
        prefetch_objects = []
        same_vectors = len(set(type(v) for v in vector_config)) == 1
        embedding = await vector_config[0].get_embedding(text_lower)

        for field in field_vectors:
            vector_found = None
            for vector in vector_config:
                if vector.name_for_embed == field:
                    vector_found = vector
                    break
            if vector_found is None:
                raise ValueError(f"Некорректное название для векторизуемого поля: {field}")

            if not same_vectors:
                embedding = await vector_found.get_embedding(text_lower)

            if main_vector is None:
                main_vector = embedding
                main_vector_name = vector_found.name

            prefetch_objects.append(
                qdrant_client.models.Prefetch(
                    query=embedding,
                    using=vector_found.name,
                    limit=limit))

        if payload_index:
            query_tokens = qdparser.FileParser().tokenize_text(text)
            filtering_conditions = []
            for token in query_tokens:
                filtering_conditions.append(
                    qdrant_client.models.FieldCondition(
                        key="tokens",
                        match=qdrant_client.models.MatchValue(value=token)))
            token_filter = qdrant_client.models.Filter(should=filtering_conditions)

            res = await self.qdrant.query_points(
                collection_name=collection_name,
                prefetch=prefetch_objects,
                query=main_vector,
                using=main_vector_name,
                limit=limit,
                query_filter=token_filter)
        else:
            res = await self.qdrant.query_points(
                collection_name=collection_name,
                prefetch=prefetch_objects,
                query=main_vector,
                using=main_vector_name,
                limit=limit)

        return res.points

    async def must_search(self, filter_data: dict, collection_name: str):
        filtering = []
        for key, value in filter_data.items():
            if isinstance(value, dict) and '$in' in value:
                # Если значение является словарем с ключом '$in', используем фильтрацию по списку значений
                filtering.append(qdrant_client.models.FieldCondition(
                    key=key,
                    match=qdrant_client.models.MatchAny(any=value['$in'])
                ))
            else:
                # Стандартная фильтрация для одиночного значения
                filtering.append(qdrant_client.models.FieldCondition(
                    key=key,
                    match=qdrant_client.models.MatchValue(value=value)
                ))

        filter_search = qdrant_client.models.Filter(must=filtering)

        response = await self.qdrant.scroll(
            collection_name=collection_name,
            scroll_filter=filter_search
        )

        return response[0]

    async def get_all(self, collection_name: str):
        filter_all = qdrant_client.models.Filter(must=[])

        response = await self.qdrant.scroll(
            collection_name=collection_name,
            scroll_filter=filter_all,
            limit=1000
        )

        return response[0]

    async def add_points(self, points_batch: list[dict], collection_name: str):
        config = self.collection_configs.get(collection_name)
        if config is None:
            raise ValueError(f"Конфигурация для коллекции '{collection_name}' не найдена.")
        vector_config = config["vector_config"]
        type_of_object = config["type_of_object"]

        correct_data_object = []
        for point in points_batch:
            correct_data_object.append(type_of_object.from_dict(point))

        correct_points = []
        embedding_cache = {}
        for index, point in enumerate(correct_data_object, 1):
            vector_data = {}
            for vector in vector_config:
                text_to_embed = point[vector.name_for_embed]
                cache_key = f"{vector.name}:{text_to_embed}"

                if cache_key in embedding_cache:
                    embed_result = embedding_cache[cache_key]
                else:
                    embed_result = await vector.get_embedding(text_to_embed)
                    embedding_cache[cache_key] = embed_result

                vector_data[vector.name] = embed_result

            correct_points.append(self.create_point(
                doc_id=str(uuid.uuid4()),
                vector=vector_data,
                data_object=point))

        for i in range(0, len(correct_points), 100):
            chunk = correct_points[i: i + 100]
            await self.qdrant.upsert(
                collection_name=collection_name,
                points=chunk)

        logging.info(f"Успешно записано {len(correct_points)} чанков в коллекцию '{collection_name}'.")

    async def update_points(self, points_batch: list[dict], collection_name: str,
                            compare_fields: list[str], fields_to_check: list[str]):
        config = self.collection_configs.get(collection_name)
        if config is None:
            raise ValueError(f"Конфигурация для коллекции '{collection_name}' не найдена.")
        vector_config = config["vector_config"]
        type_of_object = config["type_of_object"]

        updated = 0
        adding = 0
        for record in points_batch:
            point_data = type_of_object.from_dict(record)

            filter_data = {}
            for field in compare_fields:
                if field in point_data.get_fields():
                    filter_data[field] = point_data[field]

            existing_point = None
            if filter_data:
                filtering = []
                for key, value in filter_data.items():
                    filtering.append(
                        qdrant_client.models.FieldCondition(
                            key=key,
                            match=qdrant_client.models.MatchValue(value=value)
                        )
                    )

                filter_search = qdrant_client.models.Filter(must=filtering)

                # Выполняем поиск через scroll с ограничением до одного найденного элемента
                response = await self.qdrant.scroll(
                    collection_name=collection_name,
                    scroll_filter=filter_search,
                    limit=1
                )
                if response[0] and len(response[0]) > 0:
                    existing_point = response[0][0]

            if existing_point:
                all_match = True
                for field in fields_to_check:
                    if field in point_data.get_fields():
                        value = point_data[field]
                        if isinstance(value, datetime.date) and not isinstance(value, datetime.time):
                            value = value.strftime("%Y-%m-%d")
                        elif isinstance(value, datetime.time):
                            value = value.strftime("%H:%M:%S")
                    else:
                        value = None

                    left_value = getattr(existing_point.payload, field, None)
                    if left_value is None and hasattr(existing_point.payload, 'get'):
                        left_value = existing_point.payload.get(field)

                    if left_value != value:
                        all_match = False
                        break

                if all_match:
                    continue

            vectors = {}
            vector_cache = {}
            for vector in vector_config:
                text = point_data[vector.name_for_embed]
                cache_key = (vector.name, text)
                if cache_key in vector_cache:
                    embedding = vector_cache[cache_key]
                else:
                    embedding = await vector.get_embedding(text)
                    vector_cache[cache_key] = embedding
                vectors[vector.name] = embedding

            if existing_point:
                point_id = getattr(existing_point, "id", None) or existing_point.get("id")
                if point_id is None:
                    point_id = str(uuid.uuid4())
                    adding += 1
                else:
                    updated += 1
            else:
                point_id = str(uuid.uuid4())
                adding += 1

            upsert_point = self.create_point(
                doc_id=point_id,
                vector=vectors,
                data_object=point_data
            )

            await self.qdrant.upsert(
                collection_name=collection_name,
                points=[upsert_point]
            )

        return {'updating': updated, 'adding': adding}

    async def update_points_for_date(self, points_batch: list[dict], collection_name: str, compare_field: str):
        config = self.collection_configs.get(collection_name)
        if config is None:
            raise ValueError(f"Конфигурация для коллекции '{collection_name}' не найдена.")
        vector_config = config["vector_config"]
        type_of_object = config["type_of_object"]

        records, next_offset = await self.qdrant.scroll(
            collection_name=collection_name,
            limit=1,
            order_by=qdrant_client.models.OrderBy(key=compare_field, direction=qdrant_client.models.Direction.DESC),
            with_payload=True,
            with_vectors=False)

        if records:
            timestamp_in_db = records[0].payload.get(compare_field)
            if timestamp_in_db is not None:
                max_db_date = datetime.datetime.fromtimestamp(float(timestamp_in_db))
            else:
                raise ValueError(f"Поле {compare_field} не является UNIX представлением даты")
        else:
            raise ValueError("В коллекции отсутствуют points")

        max_batch_date = None
        for record in points_batch:
            if dt_str := record.get(compare_field):
                dt_val = datetime.datetime.fromisoformat(dt_str)
                if (max_batch_date is None) or (dt_val > max_batch_date):
                    max_batch_date = dt_val

        if max_db_date and max_batch_date <= max_db_date:
            return

        points_for_upsert = []
        for record in points_batch:
            if dt_str := record.get(compare_field):
                dt_val = datetime.datetime.fromisoformat(dt_str)
                if (not max_db_date) or (dt_val > max_db_date):
                    if n_id := record.get("n_id"):
                        filter_ = qdrant_client.models.Filter(
                            must=[
                                qdrant_client.models.FieldCondition(
                                    key="n_id",
                                    match=qdrant_client.models.MatchValue(value=n_id))])

                        found_records, _offset = await self.qdrant.scroll(
                            collection_name=collection_name,
                            scroll_filter=filter_,
                            limit=1,
                            with_payload=True,
                            with_vectors=False)

                        if found_records:
                            existing_record = found_records[0]
                            old_timestamp = existing_record.payload.get(compare_field)
                            if old_timestamp is not None:
                                old_dt = datetime.datetime.fromtimestamp(float(old_timestamp))
                                if dt_val <= old_dt:
                                    continue
                            qdrant_id = existing_record.id
                        else:
                            qdrant_id = str(uuid.uuid4())
                    else:
                        qdrant_id = str(uuid.uuid4())

                    point_data = type_of_object.from_dict(record)
                    vectors = {
                        vector.name: await vector.get_embedding(point_data[vector.name_for_embed])
                        for vector in vector_config
                    }
                    upsert_point = self.create_point(
                        doc_id=qdrant_id,
                        vector=vectors,
                        data_object=point_data
                    )
                    points_for_upsert.append(upsert_point)

        if points_for_upsert:
            await self.qdrant.upsert(
                collection_name=collection_name,
                points=points_for_upsert)
            logging.info(f"Обновлено/добавлено {len(points_for_upsert)} записей в коллекции '{collection_name}'.")
        else:
            logging.info("Не нашлось записей для обновления/добавления.")

    @staticmethod
    def create_point(doc_id: str | int, vector: list | dict[str, list], data_object: DataObject):
        return qdrant_client.models.PointStruct(id=doc_id, vector=vector, payload=dict(data_object))
