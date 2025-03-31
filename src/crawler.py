# This is a sample Python script.
import yaml
import argparse
import os


from harvester import (
    IndeedHarvester,
    KarriereHarvester,
    MonsterHarvester,
    StepStoneHarvester,
)


class HarvesterFactory:
    engines = {
        StepStoneHarvester.__class__.__name__: StepStoneHarvester,
        KarriereHarvester.__class__.__name__: KarriereHarvester,
    }

    def __init__(self, config):
        self.config = config
        print(self.config)


def main(*args):
    argument_parser = argparse.ArgumentParser(
        description= "WU Advertisment crawler"
    )

    argument_parser.add_argument("-c", "--config")
    options, args = argument_parser.parse_args()

    with open(options.config) as config_handle:
        config = yaml.safe_load(config_handle)
    
    HarvesterFactory(config)


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
