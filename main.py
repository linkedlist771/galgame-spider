from utils import (load_all_character_name_and_id,
                   get_current_time,
                   print_table_entries,
                   parse_character_info,
                   remove_null_values_from_jsonl
                   )
from config import (MAX_TIME_OUT,
                    HEADERS,
                    COROUTINE_LIMIT,
                    REQUEST_INTERVAL,
                    RATE_LIMIT_WAIT_TIME,
                    EXCEPTION_WAIT_TIME
                    )
import asyncio
import aiofiles
import logging
import json
import httpx
from tqdm.asyncio import tqdm
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GalGameCharacterCrawler:
    def __init__(self, base_url, timeout):
        self.base_url = base_url
        self.timeout = timeout

    async def get_character_info(self, character_id):
        retries = 3  # 设置重试次数
        while retries > 0:
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    headers = HEADERS
                    headers['Date'] = get_current_time()
                    url = f"{self.base_url}/{character_id}"
                    response = await client.get(url, headers=headers)
                    if response.status_code == 200:
                        return response.text
                    else:
                        logger.error(f"Failed to get character info for {character_id}, status code: {response.status_code}")
                        logger.error(f"Caused by: {response.text}")
                        if "rate-limited!" in response.text:
                            logger.warning("Rate limit reached, waiting for 10 seconds...")
                            await asyncio.sleep(RATE_LIMIT_WAIT_TIME)
                            return await self.get_character_info(character_id)  # Retry the request
                        return None
            except (httpx.ConnectError, httpx.ReadError) as e:
                logger.error(f"Error while getting character info for {character_id}: {str(e)}")
                retries -= 1
                if retries > 0:
                    logger.info(f"Retrying... ({3 - retries} attempts left)")
                    await asyncio.sleep(EXCEPTION_WAIT_TIME)  # 等待5秒后重试
                else:
                    logger.error(f"Failed to get character info for {character_id} after 3 attempts")
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                return None

async def save_to_jsonl(data, filename="output.jsonl"):
    async with aiofiles.open(filename, mode='a') as f:
        await f.write(json.dumps(data) + '\n')


async def fetch_and_save_character_info(crawler, character_id, semaphore):
    async with semaphore:
        character_info = await crawler.get_character_info(character_id)

        if character_info:
            # print_table_entries(character_info)

            parsed_character_info = await parse_character_info(character_info)
            parsed_character_info['id'] = character_id
            # print_table_entries(parsed_character_info)
            logger.info(f"Character info: {parsed_character_info}")
            logger.info(f"Saved character info for {character_id}")
            await save_to_jsonl(parsed_character_info)

async def main():
    parser = argparse.ArgumentParser(description='Crawl character info from vndb.org')
    parser.add_argument('--base_url', type=str, default="https://vnstat.org", help='Base url of the website')
    parser.add_argument('--coroutine_limit', type=int, default=COROUTINE_LIMIT, help='Number of coroutines')
    parser.add_argument('--request_interval', type=float, default=REQUEST_INTERVAL, help='Request interval')
    parser.add_argument('--max_time_out', type=int, default=MAX_TIME_OUT, help='Max time out')
    parser.add_argument('--rate_limit_wait_time', type=int, default=RATE_LIMIT_WAIT_TIME, help='Rate limit wait time')
    parser.add_argument('--exception_wait_time', type=int, default=EXCEPTION_WAIT_TIME, help='Exception wait time')
    parser.add_argument('--output_file', type=str, default="output.jsonl", help='Output file name')
    parser.add_argument('--drop_null_values', type=bool, default=True, help='Drop null values')
    parser.add_argument('--run_crawler', type=bool, default=True, help='Whether to run the crawler')
    args = parser.parse_args()
    if args.run_crawler:

        BASE_URL = args.base_url

        crawler = GalGameCharacterCrawler(BASE_URL, MAX_TIME_OUT)
        character_name_and_id = load_all_character_name_and_id()
        character_ids = list(character_name_and_id.values())
        # 定义协程数目
        semaphore = asyncio.Semaphore(COROUTINE_LIMIT)
        logger.info(f"Start to crawl {len(character_ids)} characters")
        # tasks = []
        async for character_id in tqdm(character_ids, total=len(character_ids)):
            await fetch_and_save_character_info(crawler, character_id, semaphore)
            await asyncio.sleep(REQUEST_INTERVAL)  # 请求间隔
    if args.drop_null_values:
        logger.info("Removing null values from jsonl file...")
        await remove_null_values_from_jsonl(args.output_file, f"null_removed_{args.output_file}")

if __name__ == "__main__":
    asyncio.run(main())
