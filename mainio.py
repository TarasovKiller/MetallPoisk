import asyncio
import aiohttp
import csv
from aiohttp_socks import ProxyConnector
import itertools
from bs4 import BeautifulSoup
import re
import time
import aiofiles
import random
from aiologger.loggers.json import JsonLogger
from aiologger.handlers.streams import AsyncStreamHandler
import httpx
import logging

logging.basicConfig(level=logging.INFO, filename="logs/log.txt",filemode="w")

PROXIES = [
    'http://73729TG:proxysoxybot@45.86.0.135:5500',
    'http://DvdQTb:h22L4X@213.166.73.109:9116',
    'http://73729TG:proxysoxybot@45.134.182.112:5500'
]

async def get_data(session, page, name):
    logging.info(f"Session = {session}, Page = {page}, Name = {name}")
    url = 'https://poiskmetalla.ru/load/filter'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Content-Length': "15",
        "Host": "poiskmetalla.ru"
    }

    data = {
        'filter': 1,
        "page": page,
        "size": " ",
        "model": " ",
        "name": name
    }
    # print(f"url={url},data={data}")


    async with session.post(url, headers=headers, data=data, timeout=10) as response:
        text = await response.text()
        # print(text)
        pattern = r'data-page=(\d+)'
        match = re.search(pattern, text)
        data_page = None
        if match:
            data_page = match.group(1)
        else:
            logging.warning(f"No match data-page found ({name}, {page})")
            print(f"No match data-page found ({name}, {page})")
            match = re.search("Results: (\d+)/50", text)
            if match:
                result_load = int(match.group(1))
                if result_load >= 50:
                    logging.error("Result error!")
                    raise ValueError("Result error!")

        pattern = r'\.append\((.*)'
        match = re.search(pattern, text)
        result = ""
        if match:
            result = match.group(1)
        else:
            logging.warning("No match html found")
            print("No match html found")

        soup = BeautifulSoup(result, "html.parser")
        rows = soup.find_all('ul', {'itemtype': lambda value: value and 'http://schema' in value})
        # print(rows[0])
        for row in rows:
            li_tags = row.find_all('li')[:-1]
            name, size, mark, offer, description, suplier, city, phone, email = list(map(lambda x: x.text, li_tags))
            size = re.sub(r'Размер:\s*', "", size)
            mark = re.sub(r'Марка:\s*', "", mark)
            offer = re.sub(r'Цена:\s*', "", offer)
            data = [name, size, mark, offer, description, suplier, city, phone, email]

            await write_row_to_csv(data)

        return data_page

async def write_row_to_csv(row):
    async with aiofiles.open("data.csv", 'a+', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter="$")
        await writer.writerow(row)

async def get_categories(session):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
    }

    async with session.get("https://poiskmetalla.ru/search", headers=headers) as resp:
        content = await resp.content.read()
        soup = BeautifulSoup(content, "html.parser")
        li_tags = soup.find('ul', class_='tabs').find_all('li', class_=False)
        links = []
        for li in li_tags:
            hrefs = li.find_all('a', href=lambda href: href and 'javascript' in href)
            links+=hrefs


        categories = []
        for link in links:
            category = link.get('data-url')
            if category:
                categories.append(category)

        return categories
async def process_category(category,proxy):
    connector = ProxyConnector.from_url(proxy)
    async with aiohttp.ClientSession(connector=connector) as session:
        previous_data_page = 0
        page = 0
        while True:
            page += 1
            data_page = await get_data(session, page, name=category)

            if previous_data_page == data_page:
                break
            previous_data_page = data_page

            sleep_value = random.uniform(0.8, 3.1)
            await asyncio.sleep(sleep_value)
        return 1

async def main():

    async with aiohttp.ClientSession() as session:


        categories = await get_categories(session)
        logging.info(f"Got categories ({len(categories)} categories): ")
        random.shuffle(categories)

        tasks = []
        sem = asyncio.Semaphore(3)  # Ограничение на количество параллельных запросов
        proxies = itertools.cycle(PROXIES)  # Циклический итератор по списку прокси

        async def worker(category,proxy):
            async with sem:
                logging.info("Process with category = {category}, proxy = {proxy} is launched".format(category=category,proxy=proxy))
                await process_category(category, proxy)

        for category in categories:
            proxy = next(proxies)
            task = asyncio.create_task(worker(category,proxy))
            tasks.append(task)

        await asyncio.gather(*tasks)
        logging.info("Tasks gathered")









if __name__ == '__main__':
    start = time.time()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    end = time.time()
    print(f"time = {end - start}")