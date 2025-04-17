import requests
import json
import yaml
from csv import DictWriter
from lxml import etree
from tqdm import tqdm
import base64


class Article:
    def __init__(self, article_data, oai_endpoint, identification):
        self.namespaces = {
            "oai": "http://www.openarchives.org/OAI/2.0/",
            "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
            "dc": "http://purl.org/dc/elements/1.1/"
        }
        self.id = article_data.get("id")
        self.parent = identification
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
        try:
            published_galley = self.all_data['publications'][0]['galleys'][0]['urlPublished']
        except:
            published_galley = None
        if published_galley:
            original = published_galley
        else:
            try:
                original = self.all_data['publications'][0]['urlPublished']
            except KeyError:
                original = ""
        return {
            "bundle:ORIGINAL": original,
            "bundle:THUMBNAIL": self.get_thumbnail(original),
            'dc.title': title.text if title is not None else "",
            'dc.creator': creator.text if creator is not None else "",
            'dc.date': date.text if date is not None else "",
            'dc.source': source.text if source is not None else "",
            "dspace.entity.type": "Publication",
            'published': self.all_data.get('statusLabel', ''),
            "relation.isJournalIssueOfPublication": ""
        }

    @staticmethod
    def get_thumbnail(url):
        encoded = base64.urlsafe_b64encode(url.encode()).decode()
        return f"https://api.library.tamu.edu/iiif/2/{encoded};1/full/159,/0/default.jpg"


class Issue:
    def __init__(self, issue_data, journal_title):
        self.all_data = issue_data
        self.for_csv = {
            "bundle:THUMBNAIL": issue_data["coverImageUrl"]["en"],
            "dcterms.available": issue_data["datePublished"],
            "dc.description": issue_data["description"]["en"],
            "dc.title": f"{journal_title}: {issue_data.get('identification')}",
            "dc.identifier": issue_data.get("number"),
            "dc.date": issue_data.get("year"),
            "dcterms.type": "Issue",
            "dspace.entity.type": "JournalIssue",
            "relation.isJournalVolumeOfIssue": ""
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

    def get_articles(self, issue_id, identification):
        all_articles = self.get_articles_in_issue(issue_id)
        # Is status and statusLabel consistent across all OJS instances?
        return [Article(article, self.oai_endpoint, identification) for article in tqdm(all_articles.get("articles", []))]

    def get_all_articles(self):
        all_issues = self.get_issues()
        all_articles = []
        for issue in all_issues.get('items', []):
            issue_articles = self.get_articles(issue.get("id"), issue.get("identification"))
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
                        'bundle:THUMBNAIL': self.journal_config.get('default_thumbnail', ''),
                        'dc.title': f"{self.journal_title}: Volume {issue['volume']}",
                        "dc.date": issue['year'],
                        "dcterms.type": "Volume",
                        "dspace.entity.type": "JournalVolume",
                        "relation.isJournalOfVolume": ""
                    }
        return [v for k, v in volumes.items()]

    def write_volumes(self):
        all_volumes = self.get_all_volumes()
        with open(f"{self.output}/volumes.csv", "w", encoding="utf-8") as out:
            writer = DictWriter(out, fieldnames=all_volumes[0].keys())
            writer.writeheader()
            for volume in all_volumes:
                writer.writerow(volume)

    def write_issues(self):
        all_issues = self.get_all_issues()
        with open(f"{self.output}/issues.csv", "w", encoding="utf-8") as out:
            writer = DictWriter(out, fieldnames=all_issues[0].for_csv.keys())
            writer.writeheader()
            for issue in all_issues:
                writer.writerow(issue.for_csv)

    def write_articles(self):
        all_articles = self.get_all_articles()
        with open(f"{self.output}/articles.csv", "w", encoding="utf-8") as out:
            writer = DictWriter(out, fieldnames=all_articles[0].keys())
            writer.writeheader()
            for article in all_articles:
                if article.get('published') == "Published":
                    writer.writerow(article)


    def get_articles_in_issue(self, issue_id):
        r = requests.get(f"{self.url}/api/v1/issues/{issue_id}", headers=self.headers)
        return r.json()

    def get_title_data(self):
        return {
            "bundle:THUMBNAIL": self.journal_config.get('default_thumbnail', ''),
            "dc.title": self.journal_title,
            "dc.date": self.journal_config.get('date', ''),
            "dc.subject": ",".join(self.journal_config.get('subjects', [])),
            "dc.description": self.journal_config.get('description', ''),
            "dcterms.alternative": self.journal_config.get('alternative', ''),
            "dspace.entity.type": "Journal",
        }

    def write_title_data(self):
        title = self.get_title_data()
        with open(f"{self.output}/title.csv", "w", encoding="utf-8") as out:
            writer = DictWriter(out, fieldnames=title.keys())
            writer.writeheader()
            writer.writerow(title)


if __name__ == "__main__":
    with open("config/config.yml", 'r') as stream:
        yml = yaml.safe_load(stream)
    x = OJSnake(yml.get('ciney'))
    x.write_issues()
    x.write_volumes()
    x.write_articles()
    x.write_title_data()
