import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import yaml
from typing import List, Dict, Union, Optional
import logging

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Parser:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.session = aiohttp.ClientSession()

    async def fetch_content(self, url: str) -> str:
        """Загрузка HTML-контента страницы"""
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                logging.info(f"Загружена страница {url}")
                return await response.text()
        except Exception as e:
            logging.error(f"Ошибка при загрузке {url}: {e}")
            return ""

    async def parse_page_quotes(self, html: str, parsing_rules: Dict[str, str]) -> List[
        Dict[str, Union[str, List[str]]]]:
        """Парсинг всех цитат с одной страницы"""
        soup = BeautifulSoup(html, 'html.parser')
        page_data = []
        quotes = soup.select(parsing_rules['quote_block'])

        for quote in quotes:
            quote_data = {
                'quote': quote.select_one(parsing_rules['quote']).get_text(strip=True) if quote.select_one(
                    parsing_rules['quote']) else None,
                'author': quote.select_one(parsing_rules['author']).get_text(strip=True) if quote.select_one(
                    parsing_rules['author']) else None,
                'tags': [tag.get_text(strip=True) for tag in quote.select(parsing_rules['tags'])],
            }
            page_data.append(quote_data)
        return page_data

    async def get_next_page(self, soup: BeautifulSoup, next_page_selector: Optional[str], base_url: str) -> Optional[
        str]:
        """Определяет ссылку на следующую страницу, если она указана"""
        if next_page_selector:
            next_page = soup.select_one(next_page_selector)
            if next_page and 'href' in next_page.attrs:
                return base_url + next_page['href']
        return None

    async def scrape_site(self) -> List[Dict[str, Union[str, List[str]]]]:
        """Сбор данных со всех страниц, указанных в конфигурации"""
        collected_data = []
        for site in self.config['sites']:
            url = site['url']
            rules = site['parsing_rules']
            next_page_selector = site.get('next_page_selector')
            base_url = site.get('base_url', '')

            while url:
                html = await self.fetch_content(url)
                if html:
                    soup = BeautifulSoup(html, 'html.parser')
                    page_data = await self.parse_page_quotes(html, rules)
                    collected_data.extend(page_data)

                    # Переход на следующую страницу, если она доступна
                    url = await self.get_next_page(soup, next_page_selector, base_url)
                else:
                    url = None  # Прекращаем обработку, если не удалось загрузить страницу
        return collected_data

    async def save_data(self, data: List[Dict[str, Union[str, List[str]]]], output_file: str):
        """Сохранение собранных данных в JSON-файл"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Сохранено {len(data)} записей в файл {output_file}")

    async def run(self):
        """Запуск парсера с инициализацией и закрытием сессии"""
        collected_data = await self.scrape_site()
        await self.save_data(collected_data, self.config['output_file'])
        await self.session.close()


async def main():
    parser = Parser(config_path="config.yaml")
    await parser.run()


if __name__ == '__main__':
    asyncio.run(main())
