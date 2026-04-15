from datetime import date
import unittest

from ingestion.scrapper.sources.nse import NSEScraper


class NSEScraperTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = NSEScraper()

    def test_parse_response_uses_download_url_for_url_and_pdf_url(self) -> None:
        payload = {
            "data": [
                {
                    "fileExt": "pdf",
                    "fileDept": "faop",
                    "circNumber": "/73629",
                    "cirDate": "20260415",
                    "circDisplayNo": "FAOP/73629",
                    "sub": "Margin collection update",
                    "circFilelink": "https://nsearchives.nseindia.com/content/circulars/FAOP73629.pdf",
                }
            ]
        }

        circulars = self.scraper.parse_response(payload)

        self.assertEqual(len(circulars), 1)
        circular = circulars[0]
        self.assertEqual(circular.circular_id, "FAOP73629")
        self.assertEqual(circular.issue_date, date(2026, 4, 15))
        self.assertEqual(
            circular.url,
            "https://nsearchives.nseindia.com/content/circulars/FAOP73629.pdf",
        )
        self.assertEqual(circular.pdf_url, circular.url)
        self.assertEqual(circular.source_item_key, "FAOP73629")

    def test_parse_response_falls_back_to_derived_download_url(self) -> None:
        payload = {
            "data": [
                {
                    "fileExt": "pdf",
                    "fileDept": "faop",
                    "circNumber": "/73629",
                    "cirDate": "20260415",
                    "circDisplayNo": "FAOP/73629",
                    "sub": "Margin collection update",
                    "circFilelink": "",
                }
            ]
        }

        circulars = self.scraper.parse_response(payload)

        self.assertEqual(len(circulars), 1)
        circular = circulars[0]
        self.assertEqual(
            circular.url,
            "https://nsearchives.nseindia.com/content/circulars/FAOP73629.pdf",
        )
        self.assertEqual(circular.pdf_url, circular.url)
        self.assertEqual(circular.source_item_key, "FAOP73629")


if __name__ == "__main__":
    unittest.main()
