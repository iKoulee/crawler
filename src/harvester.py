from functools import cached_property
import re
import requests
from abc import abstractmethod
from time import sleep, time
from protego import Protego

from bs4 import BeautifulSoup

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
        self.config = config
        self.url = config["url"]
        self.requests_per_minute = config.get("requests_per_minute", 1)

    def harvest(self):
        # code to harvest data
        pass

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
        print("Cookies", self._cookies)
        print("Referer", self._referer)
        return self._cookies

    @cached_property
    def crawl_delay(self):
        robots_value: int = self._get_robot_parser().crawl_delay(self.AGENT) or 0
        return max((60 / self.requests_per_minute), robots_value)

    def can_fetch(self, uri):
        return self._get_robot_parser().can_fetch(uri, self.AGENT)

    @abstractmethod
    def search_keyword(self, keyword):
        # code to search keyword
        pass

    def _get(self, *args, **kwargs):
        now = time()
        if self._last_request + self.crawl_delay > now:
            sleep(self.crawl_delay)
        self._last_request = time()
        return requests.get(*args, **kwargs)


class StepStoneHarvester(Harvester):

    def __init__(self, config):
        super().__init__(config)

    def harvest(self):
        keywords = ["manager"]
        for keyword in keywords:
            search_result = self.search_keyword(keyword)
            for article in search_result.find_all(
                "article", attrs={"data-testid": "job-item"}
            ):
                link = article.find("a", attrs={"data-testid": "job-item-title"})
                if not link:
                    continue
                response = self._get(
                    self.url + link["href"], headers=self._headers, cookies=self.cookies
                )
                response.raise_for_status()
                ad = Advertisement(response.text)
                ad.parse()

    def search_keyword(self, keyword) -> BeautifulSoup:
        uri = f"/jobs/{keyword}?q={keyword}"
        if not self.can_fetch(uri):
            raise Exception(f"URI {uri} is not allowed by robots.txt")
        response = self._get(
            self.url + uri, headers=self._headers, cookies=self.cookies
        )
        response.raise_for_status()
        return BeautifulSoup(response.text, features="html.parser")


class KarriereAtHarvester(Harvester):
    def __init__(self, config):
        super().__init__(config)

    def harvest(self):
        keywords = ["manager"]
        for keyword in keywords:
            search_result = self.search_keyword(keyword)
            for link in search_result.find_all(
                "a", attrs={"class": "m-jobsListItem__titleLink"}
            ):
                response = self._get(
                    self.url + link["href"], headers=self._headers, cookies=self.cookies
                )
                response.raise_for_status()
                response.encoding = response.apparent_encoding
                ad = Advertisement(response.text)

    def search_keyword(self, keyword) -> BeautifulSoup:
        uri = f"/jobs?keywords={keyword}"
        if not self.can_fetch(uri):
            raise Exception(f"URI {uri} is not allowed by robots.txt")
        response = self._get(
            self.url + uri, headers=self._headers, cookies=self.cookies
        )
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        return BeautifulSoup(response.text, features="html.parser")


class MonsterHarvester(Harvester):
    def __init__(self, config):
        super().__init__(config)

    def harvest(self):
        keywords = ["manager"]
        for keyword in keywords:
            search_result = self.search_keyword(keyword)
            for link in search_result.find_all(
                "a", attrs={"class": "m-jobsListItem__titleLink"}
            ):
                response = self._get(
                    self.url + link["href"], headers=self._headers, cookies=self.cookies
                )
                response.raise_for_status()
                response.encoding = response.apparent_encoding
                ad = Advertisement(response.text)

    def search_keyword(self, keyword) -> BeautifulSoup:
        uri = f"/jobs/suche?q={keyword}&page=1"
        if not self.can_fetch(uri):
            raise Exception(f"URI {uri} is not allowed by robots.txt")
        print("Getting", self.url + uri)
        response = self._get(
            self.url + uri, headers=self._headers, cookies=self.cookies
        )
        response.raise_for_status()
        print("Status", response.status_code)
        response.encoding = response.apparent_encoding
        with open("monster.html", "w") as f:
            f.write(response.text)
        return BeautifulSoup(response.text, features="html.parser")


class IndeedHarvester(Harvester):
    _headers = {
        "User-Agent": "Googlebot",
        # "User-Agent": AGENT,
        "Accept-Encoding": "br",
        "Connection": "keep-alive",
        "accept": "accept: text/html,application/xhtml+xml,application/xml",
    }

    def search_keyword(self, keyword):
        uri = f"/jobs?q={keyword}"
        if not self.can_fetch(uri):
            raise Exception(f"URI {uri} is not allowed by robots.txt")
        print("Getting", self.url + uri)
        response = self._get(
            self.url + uri, headers=self._headers, cookies=self.cookies
        )
        print("Status", response.status_code)
        # print(response.text)
        with open("anticrawler_data", "bw") as f:
            f.write(response.content)
        response.raise_for_status()
        print("Status", response.status_code)
        response.encoding = response.apparent_encoding
        with open("monster.html", "w") as f:
            f.write(response.text)
        return BeautifulSoup(response.text, features="html.parser")
