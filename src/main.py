# This is a sample Python script.
import os


from harvester import (
    IndeedHarvester,
    KarriereAtHarvester,
    MonsterHarvester,
    StepStoneHarvester,
)


class HarvesterFactory:
    @staticmethod
    def create_harvester(config):
        if "karriere.at" in config["url"]:
            return KarriereAtHarvester(config)
        elif "monster.de" in config["url"]:
            return MonsterHarvester(config)
        elif "stepstone.at" in config["url"]:
            return StepStoneHarvester(config)
        else:
            raise Exception("Unknown harvester")


def main():
    # harverster = StepStoneHarvester({'url': 'https://www.stepstone.at'})
    # harverster.harvest()
    harvester = IndeedHarvester(
        {"url": "https://at.indeed.com", "requests_per_minute": 10}
    )
    harvester._referer = "https://www.google.com"
    harvester.search_keyword("manager")


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    os.getcwd()
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
