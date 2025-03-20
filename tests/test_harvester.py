import unittest
from unittest.mock import call, patch
from time import time, sleep
import sys
import os
import re


sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)
from harvester import (
    Harvester,
    IndeedHarvester,
    KarriereAtHarvester,
    MonsterHarvester,
    StepStoneHarvester,
)


def mocked_request_get(*args, **kwargs):
    class MockResponse:
        cookies = {"session": "12345"}
        status_code = 200
        apparent_encoding = "utf-8"

        def __init__(self, file, status_code):
            self.file = file
            self.status_code = status_code

        @property
        def text(self):
            if not self.file:
                raise ValueError(f"Test data for '{args[0]}' does not found!")
            with open(self.file, "r", encoding="utf-8") as file:
                return file.read()

        @property
        def url(self):
            return args[0]

        def raise_for_status(self):
            pass

        def read(self):
            return self.file.read()

    if re.search(r"/jobs/manager", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "stepstone.html",
                )
            ),
            200,
        )
    if re.search(r"/jobs\?keywords=manager", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_at.html",
                )
            ),
            200,
        )
    if re.search(r"stepstone.at/robots.txt$", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "stepstone_robots.txt",
                ),
            ),
            200,
        )
    if re.search(r"karriere.at/robots.txt$", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_robots.txt",
                ),
            ),
            200,
        )
    if re.search(r"monster.de/robots.txt$", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_robots.txt",
                ),
            ),
            200,
        )
    if re.search(r"indeed.com/robots.txt$", args[0]):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_robots.txt",
                ),
            ),
            200,
        )
    if re.search(
        r"/stellenangebote--.*\.html$",
        args[0],
    ):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "stepstone_jobs_OTR_Manager.txt.iconv_cleaned_utf8",
                )
            ),
            200,
        )
    if re.search(
        r"/jobs/[0-9]+$",
        args[0],
    ):
        return MockResponse(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "test_data",
                    "karriere_job.html",
                )
            ),
            200,
        )
    return MockResponse(None, 404)


class TestHarvester(unittest.TestCase):

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    @patch("harvester.time")
    @patch("harvester.sleep")
    def test_get(self, mock_sleep, mock_time, mock_requests_get):
        config = {"url": "http://example.com", "requests_per_minute": 30}
        harvester = Harvester(config)
        harvester._last_request = 0

        mock_time.return_value = 10

        response = harvester._get("http://example.com")

        self.assertEqual(response.status_code, 404)
        mock_sleep.assert_called_once()
        self.assertEqual(harvester._last_request, 10)
        mock_requests_get.assert_has_calls(
            [
                call(
                    "http://example.com/robots.txt",
                    headers={
                        "User-Agent": "Crawler",
                        "Accept-Language": "en-US,en;q=0.5",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Connection": "keep-alive",
                        "accept": "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    },
                ),
                call("http://example.com"),
            ]
        )

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_cookies(self, mock_requests_get):
        config = {"url": "https://www.stepstone.at", "requests_per_minute": 30}
        harvester = Harvester(config)

        cookies = harvester.cookies

        self.assertEqual(cookies, {"session": "12345"})
        self.assertEqual(harvester._referer, config["url"])
        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.stepstone.at/robots.txt",
                    headers={
                        "User-Agent": "Crawler",
                        "Accept-Language": "en-US,en;q=0.5",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Connection": "keep-alive",
                        "accept": "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    },
                ),
                call(
                    "https://www.stepstone.at",
                    headers={
                        "User-Agent": "Crawler",
                        "Accept-Language": "en-US,en;q=0.5",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Connection": "keep-alive",
                        "accept": "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    },
                ),
            ]
        )

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_robot_uri_check(self, mock_requests_get):
        config = {"url": "https://www.stepstone.at", "requests_per_minute": 60}
        harvester = Harvester(config)

        self.assertTrue(harvester.can_fetch("/jobs/manager"))
        mock_requests_get.assert_called_once_with(
            "https://www.stepstone.at/robots.txt", headers=harvester._headers
        )


class TestStepstoneHarvester(unittest.TestCase):

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_search_keyword(self, mock_requests_get):
        config = {"url": "https://www.stepstone.at", "requests_per_minute": 30}
        harvester = StepStoneHarvester(config)
        harvester._headers = {"User-Agent": "Mozilla/5.0"}

        response = harvester.search_keyword("manager")

        self.assertIsNotNone(response.text)
        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.stepstone.at/robots.txt",
                    headers={"User-Agent": "Mozilla/5.0"},
                ),
                call("https://www.stepstone.at", headers={"User-Agent": "Mozilla/5.0"}),
                call(
                    "https://www.stepstone.at/jobs/manager?q=manager",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
            ]
        )

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_harwest(self, mock_requests_get):
        config = {"url": "https://www.stepstone.at", "requests_per_minute": 60}
        harvester = StepStoneHarvester(config)
        harvester._headers = {"User-Agent": "Mozilla/5.0"}

        start_time = time()
        harvester.harvest()
        finish_time = time()

        self.assertAlmostEqual(
            finish_time - start_time,
            30.0,
            delta=3.0,
            msg="Time difference should be around 30 seconds",
        )

        # self.assertIsNotNone(response.text)
        # mock_sleep.assert_called_once()
        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.stepstone.at/robots.txt",
                    headers={"User-Agent": "Mozilla/5.0"},
                ),
                call("https://www.stepstone.at", headers={"User-Agent": "Mozilla/5.0"}),
                call(
                    "https://www.stepstone.at/jobs/manager?q=manager",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--OTR-Manager-Wien-Amazon-Europe-Core--876597-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--ISMS-Manager-in-m-w-x-Wien-Linz-Stuttgart-Koeln-STRABAG-BRVZ-GMBH--887526-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Fuhrpark-Mangerin-Innsbruck-Wuerth-Hochenburger-GmbH--886402-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--ManagerIN-Indoor-Freizeitpark-4-Tage-Woche-Wien-Monki-Park-e-U--878410-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--HR-Management-w-m-d-Wels-ISG-Personalmanagement-GmbH--881175-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Senior-Solution-Advisor-for-MES-or-PLM-f-m-d-Supply-Chain-Management-Wien-SAP-AG--881360-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Regulatory-Management-m-w-d-Bezirk-Voecklabruck-Adhara-GmbH--876983-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Produktmanager-m-w-d-Ladeinfrastruktur-und-Energiespeicher-Kufstein-VAHLE-AUTOMATION-GmbH--885180-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--IT-Manager-Geschaeftsprozess-Manager-m-w-d-Liezen-WLL-Personalservice-GmbH--883801-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Produktmanager-Elektrotechnik-m-w-x-Linz-epunkt-GmbH--880919-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--HR-Manager-m-w-d-Laakirchen-Iventa-The-Human-Management-Group--881192-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Area-Sales-Manager-Osteuropa-m-w-d-Wels-AVADOM-Personalmanagement--881149-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Site-Manager-Bauleiter-Elektrotechnik-w-m-x-Linz-epunkt-GmbH--829364-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Risk-Manager-m-w-d-Rankweil-Hirschmann-Automotive-GmbH--884823-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Manager-Sales-Marketing-Pamhagen-ISG-Personalmanagement-GmbH--880804-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Sales-Manager-Vienna-safeREACH-GmbH--852860-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Junior-Finance-Manager-m-w-d-Zentralraum-Oberoesterreich-Schulmeister-Management-Consulting--876625-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Leiter-Securities-Settlement-m-w-d-Bregenz-Hypo-Vorarlberg-Bank-AG--881184-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Marktmanager-in-Stellvertretung-Leoben-BILLA-AG--884539-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Manager-m-w-d-im-Financial-Accounting-Internal-Control-Lannach-Trenkwalder-Personaldienste-GmbH--884591-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--IT-Security-Manager-all-genders-Wien-Frequentis-AG--884511-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--IT-Manager-m-w-d-4300-St-Valentin-Leading-Brokers-United-Austria-GmbH--886831-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Sales-Manager-m-w-d-fuer-Elektromotoren-Generatoren-24111-Graz-ACTIEF-JOBMADE-GmbH--885183-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Medical-Manager-f-m-d-Gastroenterologie-Hepatologie-Wien-EBLINGER-PARTNER--876770-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.stepstone.at/stellenangebote--Marketing-Manager-m-w-d-Wien-Kardex-Austria-GmbH--881092-inline.html",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
            ]
        )


class TestKarriereAtHarvester(unittest.TestCase):

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_search_keyword(self, mock_requests_get):
        config = {"url": "https://www.karriere.at", "requests_per_minute": 30}
        harvester = KarriereAtHarvester(config)
        harvester._headers = {"User-Agent": "Mozilla/5.0"}

        response = harvester.search_keyword("manager")

        self.assertIsNotNone(response.text)
        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.karriere.at/robots.txt",
                    headers={"User-Agent": "Mozilla/5.0"},
                ),
                call("https://www.karriere.at", headers={"User-Agent": "Mozilla/5.0"}),
                call(
                    "https://www.karriere.at/jobs?keywords=manager",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
            ]
        )

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_harwest(self, mock_requests_get):
        config = {"url": "https://www.karriere.at", "requests_per_minute": 60}
        harvester = KarriereAtHarvester(config)
        harvester._headers = {"User-Agent": "Mozilla/5.0"}

        start_time = time()
        harvester.harvest()
        finish_time = time()

        self.assertAlmostEqual(
            finish_time - start_time,
            20.0,
            delta=3.0,
            msg="Time difference should be around 30 seconds",
        )
        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.karriere.at/robots.txt",
                    headers={"User-Agent": "Mozilla/5.0"},
                ),
                call("https://www.karriere.at", headers={"User-Agent": "Mozilla/5.0"}),
                call(
                    "https://www.karriere.at/jobs?keywords=manager",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7473235",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7482247",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7440898",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7467400",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7477366",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7466941",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7461445",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7441276",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7418758",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7291924",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7467235",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7459498",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7457008",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7478263",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7453957",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7428325",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7227538",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
                call(
                    "https://www.karriere.athttps://www.karriere.at/jobs/7478590",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
            ]
        )


class TestMonsterHarvester(unittest.TestCase):

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_search_keyword(self, mock_requests_get):
        config = {"url": "https://www.monster.de", "requests_per_minute": 30}
        harvester = MonsterHarvester(config)
        harvester._headers = {"User-Agent": "Mozilla/5.0"}

        response = harvester.search_keyword("manager")

        self.assertIsNotNone(response.text)
        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.monster.at/robots.txt",
                    headers={"User-Agent": "Mozilla/5.0"},
                ),
                call("https://www.monster.at", headers={"User-Agent": "Mozilla/5.0"}),
                call(
                    "https://www.monster.at/jobs?keywords=manager",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
            ]
        )

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_harwest(self, mock_requests_get):
        config = {"url": "https://www.monster.de", "requests_per_minute": 60}
        harvester = MonsterHarvester(config)
        harvester._headers = {"User-Agent": "Mozilla/5.0"}

        start_time = time()
        harvester.harvest()
        finish_time = time()

        self.assertAlmostEqual(
            finish_time - start_time,
            20.0,
            delta=3.0,
            msg="Time difference should be around 30 seconds",
        )


class TestIneedHarvester(unittest.TestCase):

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_search_keyword(self, mock_requests_get):
        config = {"url": "https://at.indeed.com", "requests_per_minute": 30}
        harvester = IndeedHarvester(config)
        harvester._headers = {"User-Agent": "Crawler"}

        response = harvester.search_keyword("manager")

        self.assertIsNotNone(response.text)
        mock_requests_get.assert_has_calls(
            [
                call(
                    "https://www.indeed.com/robots.txt",
                    headers={"User-Agent": "Mozilla/5.0"},
                ),
                call("https://www.indeed.com", headers={"User-Agent": "Mozilla/5.0"}),
                call(
                    "https://www.indeed.com/jobs?keywords=manager",
                    headers={"User-Agent": "Mozilla/5.0"},
                    cookies={"session": "12345"},
                ),
            ]
        )

    @patch("harvester.requests.get", side_effect=mocked_request_get)
    def test_harwest(self, mock_requests_get):
        config = {"url": "https://at.indeed.com", "requests_per_minute": 60}
        harvester = IndeedHarvester(config)
        harvester._headers = {"User-Agent": "Crawler"}

        start_time = time()
        harvester.harvest()
        finish_time = time()

        self.assertAlmostEqual(
            finish_time - start_time,
            20.0,
            delta=3.0,
            msg="Time difference should be around 30 seconds",
        )


if __name__ == "__main__":
    unittest.main()
