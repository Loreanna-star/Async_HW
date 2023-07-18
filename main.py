import asyncio

from aiohttp import ClientSession
from more_itertools import chunked

from db import Session, SwapiPeople, engine, Base

CHUNK_SIZE = 10

async def chunked_async(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_url(url, key, session):
    async with session.get(f'{url}') as response:
        data = await response.json()
        return data[key]


async def get_urls(urls, key, session):
    tasks = (asyncio.create_task(get_url(url, key, session)) for url in urls)
    for task in tasks:
        yield await task


async def get_data(urls, key, session):
    result_list = []
    async for item in get_urls(urls, key, session):
        result_list.append(item)
    return ', '.join(result_list)


async def insert_people(people_chunk):
    async with Session() as session_:
        async with ClientSession() as session:
            for person_json in people_chunk:
                if person_json.get('status') == 404:
                    break
                homeworld_str = await get_data([person_json['homeworld']], 'name', session)
                films_str = await get_data(person_json['films'], 'title', session)
                species_str = await get_data(person_json['species'], 'name', session)
                starships_str = await get_data(person_json['starships'], 'name', session)
                vehicles_str = await get_data(person_json['vehicles'], 'name', session)
                newperson = SwapiPeople(
                    birth_year=person_json['birth_year'],
                    eye_color=person_json['eye_color'],
                    films=films_str,
                    gender=person_json['gender'],
                    hair_color=person_json['hair_color'],
                    height=person_json['height'],
                    mass=person_json['mass'],
                    name=person_json['name'],
                    skin_color=person_json['skin_color'],
                    homeworld=homeworld_str,
                    species=species_str,
                    starships=starships_str,
                    vehicles=vehicles_str,
                )
                session_.add(newperson)
                await session_.commit()


async def get_person(person_id: int, session: ClientSession):
    print(f'begin {person_id}')
    async with session.get(f'https://swapi.dev/api/people/{person_id}') as response:
        if response.status == 404:
            return {'status': 404}
        person = await response.json()
        print(f'end {person_id}')
        return person


async def get_people():
    async with ClientSession() as session:
        for id_chunk in chunked(range(1, 201), CHUNK_SIZE):
            coroutines = [get_person(i, session=session) for i in id_chunk]
            people = await asyncio.gather(*coroutines)
            for item in people:
                yield item

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))
    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task

if __name__ == '__main__':
    asyncio.run(main())
