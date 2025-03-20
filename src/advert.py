from bs4 import BeautifulSoup


class Advertisement:
    source = None
    soup = None

    def __init__(self, source):
        self.source = source

    def parse(self):
        self.soup = BeautifulSoup(self.source, features="html.parser")

    def get_advertiser(self):
        print(self.soup.find("ul"))
        print(self.soup.find(["ul", "li"]))
        return self.soup.find("li", attrs={"data-at": "metadata-company-name"})