import asyncio
import config
import qdoperator, qdparser
import init_clients
import json
from tqdm import tqdm

async def main():
    client = qdoperator.QdClient(
        gd_host=config.QDRANT_HOST,
        qd_port=config.QDRANT_PORT,
        qd_key=config.QDRANT_KEY)


    # await client.delete_collection(collection_name="Alex")

    await client.create_collection(
        collection_name="Alex",
        type_of_object=qdoperator.AlexQuestion,
        vector_config=[
            qdoperator.VectorInfo(
                name="question-openai",
                name_for_embed='question',
                size=1536,
                client_embed=init_clients.client_openai,
                type_of_object=qdoperator.AlexQuestion),
            qdoperator.VectorInfo(
                name="answer-openai",
                name_for_embed='answer',
                size=1536,
                client_embed=init_clients.client_openai,
                type_of_object=qdoperator.AlexQuestion)
            ])

    file_parser = qdparser.FileParser()

    with open('data/transformed_data.json', 'r', encoding='utf-8') as f:
        content =  json.load(f)

    await client.add_points(points_batch=content,collection_name='Alex')

    # Process in batches of 100
    batch_size = 100
    total_records = len(content)
    total_batches = (total_records + batch_size - 1) // batch_size

    for i in tqdm(range(0, total_records, batch_size), total=total_batches, desc="Processing batches"):
        # Get current batch
        batch = content[i:i + batch_size]

        # Upload batch
        await client.add_points(points_batch=batch, collection_name='Alex')

if __name__ == "__main__":
    asyncio.run(main())
