from dataclasses import dataclass
from bs4 import BeautifulSoup


@dataclass(init=True, repr=True, frozen=True)
class Advertisement:
    status: int = None
    link: str = None
    source: str = None
