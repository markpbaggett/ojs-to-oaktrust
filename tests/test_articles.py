from src.ojsnake import Article
import unittest
from unittest.mock import patch, Mock
from lxml import etree
import requests
import json


class TestArticle(unittest.TestCase):

    def test_fectch_metadata_valid_record(self):
        with open("fixtures/paj-46.json", 'r') as f:
            article_data = json.load(f)
        oai_endpoint = "https://paj-ojs-tamu.tdl.org/paj/oai"
        identification = "Issue Vol. 3"
        article = Article(
            article_data,
            oai_endpoint=oai_endpoint,
            identification=identification
        )
        metadata = article.fetch_metadata()
        self.assertIsNotNone(metadata)
        

if __name__ == "__main__":
    unittest.main()

