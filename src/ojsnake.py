import requests
import json
import yaml
from csv import DictWriter
from lxml import etree


class Article:
    def __init__(self, article_data, oai_endpoint):
        self.namespaces = {
            "oai": "http://www.openarchives.org/OAI/2.0/",
            "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
            "dc": "http://purl.org/dc/elements/1.1/"
        }
        self.id = article_data.get("id")
        self.all_data = article_data
        self.title = article_data["publications"][0]["fullTitle"]
        self.authors = article_data["publications"][0].get("authorsString")
        self.oai_endpoint = oai_endpoint
        self.tree = self.fetch_metadata()
        self.for_csv = self.get_metadata()

    def fetch_metadata(self):
        response = requests.get(
            f"{self.oai_endpoint}?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:{self.oai_endpoint.split('/')[2]}:article/{self.id}"
        )
        return etree.fromstring(response.content)

    def get_metadata(self):
        title = self.tree.find(".//dc:title", namespaces=self.namespaces)
        creator = self.tree.find(".//dc:creator", namespaces=self.namespaces)
        date = self.tree.find(".//dc:date", namespaces=self.namespaces)
        source = self.tree.find(".//dc:source", namespaces=self.namespaces)
        return {
            "bundle:ORIGINAL": self.all_data['urlPublished'],
            'dc.title': title.text if title is not None else "",
            'dc.creator': creator.text if creator is not None else "",
            'dc.date': date.text if date is not None else "",
            'dc.source': source.text if source is not None else "",
        }


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
        self.journal_title = journal_config.get("title")

    def get_issues(self):
        r = requests.get(f"{self.url}/api/v1/issues", headers=self.headers)
        return r.json()

    def get_articles(self, issue_id):
        all_articles = self.get_articles_in_issue(issue_id)
        return [Article(article, self.oai_endpoint) for article in all_articles.get("articles", [])]

    def get_all_articles(self):
        all_issues = self.get_issues()
        all_articles = []
        for issue in all_issues.get('items', []):
            issue_articles = self.get_articles(issue.get("id"))
            for article in issue_articles:
                all_articles.append(article.for_csv)
        return all_articles

    def get_all_issues(self):
        all_issues = self.get_issues()
        return [Issue(issue, self.journal_title) for issue in all_issues.get("items", [])]

    def get_all_volumes(self):
        all_issues = self.get_issues()
        volumes = {}
        for issue in all_issues.get("items", []):
            if issue.get('volume'):
                if issue['volume'] not in volumes:
                    volumes[issue['volume']] = {
                        'dc.title': f"{self.journal_title}: Volume {issue['volume']}",
                        "dc.created": issue['year'],
                        "dcterms.type": "Volume",
                    }
        return [v for k, v in volumes.items()]

    def write_volumes(self, output_file):
        all_volumes = self.get_all_volumes()
        with open(output_file, "w", encoding="utf-8") as out:
            writer = DictWriter(out, fieldnames=all_volumes[0].keys())
            writer.writeheader()
            for volume in all_volumes:
                writer.writerow(volume)

    def write_issues(self, output_file):
        all_issues = self.get_all_issues()
        with open(output_file, "w", encoding="utf-8") as out:
            writer = DictWriter(out, fieldnames=all_issues[0].for_csv.keys())
            writer.writeheader()
            for issue in all_issues:
                writer.writerow(issue.for_csv)

    def write_articles(self, output_file):
        all_articles = self.get_all_articles()
        with open(output_file, "w", encoding="utf-8") as out:
            writer = DictWriter(out, fieldnames=all_articles[0].keys())
            writer.writeheader()
            for article in all_articles:
                writer.writerow(article)


    def get_articles_in_issue(self, issue_id):
        r = requests.get(f"{self.url}/api/v1/issues/{issue_id}", headers=self.headers)
        return r.json()


if __name__ == "__main__":
    with open("config/config.yml", 'r') as stream:
        yml = yaml.safe_load(stream)
    x = OJSnake(yml.get('ciney'))
    # x.write_issues('issues_test.csv')
    # x.write_volumes('volumes_test.csv')
    x.write_articles(f"article_test.csv")
