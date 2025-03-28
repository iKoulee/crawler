from functools import cached_property
import re
import requests
from abc import abstractmethod
from time import sleep, time
from protego import Protego

from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

from advert import Advertisement


class Harvester:
    AGENT = "Crawler"
    _url = None
    _headers = {
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/116.0.0.0",
        "User-Agent": AGENT,
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "accept": "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    }
    _cookies = None
    _referer = None
    _last_request = time()
    _robot_parser = None

    def __init__(self, config):
        self.url = config["url"]
        self.requests_per_minute = config.get("requests_per_minute", 1)

    @staticmethod
    def create_schema(connection):
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS advertisements (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                title TEXT,
                description TEXT,
                company TEXT,
                location TEXT,
                harvest_date DATE,
                url TEXT NOT NULL UNIQUE,
                html_body TEXT NOT NULL,
                html_status INTEGER NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                keyword TEXT NOT NULL UNIQUE
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS keyword_advertisement (
                keyword_id INTEGER,
                advertisement_id INTEGER,
                FOREIGN KEY (keyword_id) REFERENCES keywords(id),
                FOREIGN KEY (advertisement_id) REFERENCES advertisements(id)
                PRIMARY KEY (keyword_id, advertisement_id)
            )
            """
        )
        connection.commit()

    @staticmethod
    def insert_keyword(connection, keyword):
        cursor = connection.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO keywords (keyword) VALUES (?)", (keyword,)
        )
        connection.commit()

    def fetch_keywords(self, connection):
        cursor = connection.cursor()
        cursor.execute("SELECT id, keyword FROM keywords")
        result = cursor.fetchall()
        if result:
            return dict([(row[0], re.compile(row[1])) for row in result])
        return {}

    @abstractmethod
    def get_next_link(self):
        raise NotImplementedError(
            f"{self.__class__.__name__}.get_next_link() is not implemented yet."
        )

    def get_next_advert(self):
        """
        Retrieves and yields the next advertisement from the sitemap.
        """
        for link in self.get_next_link():
            response = self._get(link, headers=self._headers, cookies=self.cookies)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            yield Advertisement(
                status=response.status_code,
                link=link,
                source=response.text,
            )

    def harvest(self, connection):
        self.create_schema(connection)
        regexes = self.fetch_keywords(connection)
        cursor = connection.cursor()
        for advert in self.get_next_advert():
            cursor.execute(
                "INSERT INTO advertisements (html_body, html_status, url) VALUES (?, ?, ?)",
                (
                    advert.source,
                    advert.status,
                    advert.link,
                ),
            )
            advert_id = cursor.lastrowid
            for id, regex in regexes.items():
                if regex.search(advert.source):
                    cursor.execute(
                        "INSERT INTO keyword_advertisement (keyword_id, advertisement_id) VALUES (?, ?)",
                        (id, advert_id),
                    )

    def _get_robot_parser(self, refresh=False):
        if not self._robot_parser or refresh:
            self._robot_parser = Protego.parse(
                requests.get(f"{self.url}/robots.txt", headers=self._headers).text
            )
        return self._robot_parser

    @property
    def cookies(self):
        if not self._cookies:
            response = self._get(self.url, headers=self._headers)
            self._cookies = response.cookies
            self._referer = response.url
        return self._cookies

    @cached_property
    def crawl_delay(self):
        robots_value: int = self._get_robot_parser().crawl_delay(self.AGENT) or 0
        return max((60 / self.requests_per_minute), robots_value)

    def can_fetch(self, uri):
        return self._get_robot_parser().can_fetch(uri, self.AGENT)

    def _get(self, *args, **kwargs):
        now = time()
        if self._last_request + self.crawl_delay > now:
            sleep(self.crawl_delay)
        self._last_request = time()
        response = requests.get(*args, **kwargs)
        if response.cookies:
            self._cookies = response.cookies
        return response


class StepStoneHarvester(Harvester):

    def get_next_link(self):
        """
        Retrieves and yields links from the sitemap and nested sitemaps.
        """
        # Fetch the main sitemap
        response = self._get(f"{self.url}/sitemap.xml", headers=self._headers)
        response.raise_for_status()
        main_sitemap = ET.fromstring(response.text)

        # Iterate through all <loc> elements in the main sitemap
        for link in main_sitemap.findall(
            ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
        ):
            if re.match(r".*listings-[0-9]+.*", link.text):
                # Fetch the nested sitemap
                nested_sitemap_link = link.text
                nested_response = self._get(
                    nested_sitemap_link, headers=self._headers, cookies=self.cookies
                )
                nested_response.raise_for_status()
                nested_response.encoding = nested_response.apparent_encoding
                nested_sitemap = ET.fromstring(nested_response.text)

                # Yield all <loc> elements from the nested sitemap
                for nested_link in nested_sitemap.findall(
                    ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                ):
                    yield nested_link.text


class KarriereHarvester(Harvester):

    def get_next_link(self):
        for sitemap_link in self._get_robot_parser().sitemaps:
            if re.match(r".*sitemap-jobs.*", sitemap_link):
                response = self._get(sitemap_link, headers=self._headers)
                response.raise_for_status()
                response.encoding = response.apparent_encoding
                sitemap = ET.fromstring(response.text)
                for link in sitemap.findall(
                    ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                ):
                    print(link.text)
                    yield link.text


class MonsterHarvester(Harvester):

    pass


class IndeedHarvester(Harvester):

    pass
