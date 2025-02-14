import asyncio

import qdoperator, qdparser

import config
import init_clients
import json



async def main():
    client = qdoperator.QdClient(
        gd_host=config.QDRANT_HOST,
        qd_port=config.QDRANT_PORT,
        qd_key=config.QDRANT_KEY,
        type_of_object=qdoperator.DataObject,
        vector_config=[
            qdoperator.VectorInfo(
                name="content-openai",
                name_for_embed='content',
                size=1536,
                client_embed=init_clients.client_openai,
                type_of_object=qdoperator.DataObject),
            qdoperator.VectorInfo(
                name="source-openai",
                name_for_embed='source',
                size=1536,
                client_embed=init_clients.client_openai,
                type_of_object=qdoperator.DataObject)
            ])

    file_parser = qdparser.FileParser()

    with open('data/parseuc().json', 'r', encoding='utf-8') as f:
        content =  json.load(f)



    for el in content:
        el['tokens'] = file_parser.tokenize_text(el['source'])
        el['source'] = el['source'].lower()
        el['content'] = el['content'].lower()


    # await client.delete_collection("UC")
    # await client.create_collection("UC")
    # await client.add_points(points_batch=content, collection_name="UC")
    res = await client.hybrid_search(
        text="1С Предприятие",
        collection_name="UC",
        source_field="source",
        content_field="content")
    print(res)

    # res = await client.must_search(
    #     filter_data={"type_source": "текстовый файл"},
    #     collection_name="KIS")
    # print(res)

    # await client.delete_by_ids(ids=[res[0].id], collection_name="KIS")
    # await client.delete_by_filter(filter_data={'source': '1c_zabbix_template()wiki.txt'}, collection_name="KIS2")


if __name__ == "__main__":
    asyncio.run(main())
