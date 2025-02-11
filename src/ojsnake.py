import requests
import json
import yaml
from csv import DictWriter


class Article:
    def __init__(self, article_data, oai_endpoint):
        self.id = article_data.get("id")
        self.title = article_data["publications"][0]["fullTitle"]
        self.authors = article_data["publications"][0].get("authorsString")
        self.oai_endpoint = oai_endpoint
        self.metadata_as_xml = self.fetch_metadata()

    def fetch_metadata(self):
        response = requests.get(
            f"{self.oai_endpoint}?verb=GetRecord&metadataPrefix=oai_dc&identifier={self.oai_endpoint.split('/')[2]}:article/{self.id}"
        )
        return response.content


class Issue:
    def __init__(self, issue_data, journal_title):
        self.all_data = issue_data
        self.for_csv = {
            "bundle:THUMBNAIL": issue_data["coverImageUrl"]["en"],
            "dcterms.available": issue_data["datePublished"],
            "dc.description": issue_data["description"]["en"],
            "dc.title": f"{journal_title}: {issue_data.get('identification')}",
            "dc.identifier": issue_data.get("number"),
            "dc.created": issue_data.get("year"),
            "dcterms.type": "Issue",
        }


class OJSnake:
    def __init__(self, journal_config):
        self.journal_config = journal_config
        self.headers = {"Authorization": f"Bearer {self.journal_config.get('token')}"}
        self.oai_endpoint = journal_config.get("oai_endpoint")
        self.output = journal_config.get("output_directory")
        self.url = self.journal_config.get("url")
        self.journal_title = journal_config.get("journal_title")

    def get_issues(self):
        r = requests.get(f"{self.url}/api/v1/issues", headers=self.headers)
        return r.json()

    def get_articles(self, issue_id):
        all_articles = self.get_articles_in_issue(issue_id)
        return [Article(article, self.oai_endpoint) for article in all_articles.get("articles", [])]

    def get_all_issues(self):
        all_issues = self.get_issues()
        return [Issue(issue, self.journal_title) for issue in all_issues.get("items", [])]

    def write_issues(self, output_file):
        all_issues = self.get_all_issues()
        with open(output_file, "w", encoding="utf-8") as out:
            writer = DictWriter(out, fieldnames=all_issues[0].for_csv.keys())
            writer.writeheader()
            for issue in all_issues:
                writer.writerow(issue.for_csv)


    def get_articles_in_issue(self, issue_id):
        r = requests.get(f"{self.url}/api/v1/issues/{issue_id}", headers=self.headers)
        return r.json()


if __name__ == "__main__":
    with open("config/config.yml", 'r') as stream:
        yml = yaml.safe_load(stream)
    x = OJSnake(yml.get('ciney'))
    x.write_issues('issues_test.csv')
