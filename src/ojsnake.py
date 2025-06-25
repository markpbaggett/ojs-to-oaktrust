import requests
import json
import yaml
from csv import DictWriter
from lxml import etree
import lxml
from tqdm import tqdm
import base64
import os
from bs4 import BeautifulSoup as bs
from pdf2image import convert_from_path
import argparse


class Article:
    def __init__(self, article_data, oai_endpoint, identification, base_thumb="", output_dir=""):
        self.namespaces = {
            "oai": "http://www.openarchives.org/OAI/2.0/",
            "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
            "dc": "http://purl.org/dc/elements/1.1/"
        }
        self.id = article_data.get("id")
        self.output_dir = output_dir
        self.base_thumb = base_thumb
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
        try:
            return etree.fromstring(response.content)
        except lxml.etree.XMLSyntaxError:
            return None

    def get_metadata(self):
        title = None
        creator = None
        date = None
        source = None
        if self.tree is not None:
            title = self.tree.find(
                ".//dc:title", namespaces=self.namespaces
            )
            creator = self.tree.find(
                ".//dc:creator", namespaces=self.namespaces
            )
            date = self.tree.find(
                ".//dc:date", namespaces=self.namespaces
            )
            source = self.tree.find(
                ".//dc:source", namespaces=self.namespaces
            )
        # try:
        all_galleys = self.all_data['publications'][0]['galleys']
        for galley in all_galleys:
            request_link = galley['urlPublished'], "application/pdf"
            content_type = requests.get(galley['urlPublished']).headers.get('content-type')
            if "application/pdf" not in content_type:
                request_link = galley['file']['url'], content_type
            r = requests.get(request_link[0])
            if r.status_code == 200:
                published_galley = request_link
                break
            else:
                print(r.status_code)
                print('Galley Not Published! Trying another.')
        # except:
        #     print('No Galley Found!')
        #     published_galley = None
        if published_galley:
            original = published_galley
        else:
            print('No Pdf :(')
            # Should this go away?
            try:
                original = self.all_data['publications'][0]['urlPublished'], "missing"
            except KeyError:
                original = "", ""
        bundles = self.get_bundles(original)
        return {
            "bundle:ORIGINAL": bundles["original"],
            "bundle:THUMBNAIL": bundles["thumbnail"],
            'dc.title': title.text if title is not None else "",
            'dc.creator': creator.text if creator is not None else "",
            'dc.date': date.text if date is not None else "",
            'dc.source': source.text if source is not None else "",
            "dspace.entity.type": "Publication",
            'published': self.all_data.get('statusLabel', ''),
            "relation.isJournalIssueOfPublication": ""
        }

    def get_bundles(self, current):
        if current[1] == "application/pdf":
            return {
                "original": current[0],
                "thumbnail": self.get_thumbnail(current[0], self.base_thumb),
            }
        else:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
            if not os.path.exists(f"{self.output_dir}/originals"):
                os.makedirs(f"{self.output_dir}/originals")
            if not os.path.exists(f"{self.output_dir}/thumbnails"):
                os.makedirs(f"{self.output_dir}/thumbnails")
            r = requests.get(current[0], stream=True)
            original_filename = r.headers.get('content-disposition').split('attachment;filename="')[1].split('"')[0]
            with open(f"{self.output_dir}/originals/{original_filename}", 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)
            images = convert_from_path(f'{current_dir}/originals/{original_filename}', first_page=1, last_page=1)
            images[0].save(f'{current_dir}/thumbnails/{original_filename.replace("pdf", ".jpg")}', 'JPEG')
            return {
                'original': f"{self.output_dir}/originals/{original_filename}",
                'thumbnail': f'{current_dir}/thumbnails/{original_filename.replace("pdf", ".jpg")}'
            }

    @staticmethod
    def get_thumbnail(url, base=""):
        encoded = base64.urlsafe_b64encode(url.encode()).decode()
        thumbnail = f"https://api.library.tamu.edu/iiif/2/{encoded};1/full/159,/0/default.jpg"
        r = requests.get(thumbnail)
        if r.status_code == 200:
            return thumbnail
        else:
            return base


class Issue:
    def __init__(self, issue_data, journal_title):
        self.all_data = issue_data
        try:
            cover_image = issue_data["coverImageUrl"]["en"]
        except TypeError:
            cover_image = ""
        self.for_csv = {
            "bundle:THUMBNAIL": cover_image,
            "dcterms.available": issue_data["datePublished"],
            "dc.description": bs(issue_data["description"]["en"], "html.parser").get_text(),
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
        print(f"Getting Articles from OJS for Issue {identification}.")
        return [
            Article(
                article,
                self.oai_endpoint, identification,
                self.journal_config.get('default_thumbnail', ''),
                self.journal_config.get("output_directory", "")
            ) for article in tqdm(all_articles.get("articles", []))]

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
        os.makedirs(self.output, exist_ok=True)
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
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--yaml', help='Path to your yaml config file', default='config/config.yml')
    parser.add_argument('-j', '--journal', help='Specify the journal key you want to process')
    args = parser.parse_args()
    with open(args.yaml, 'r') as stream:
        yml = yaml.safe_load(stream)
    x = OJSnake(yml.get(args.journal))
    x.write_issues()
    x.write_volumes()
    x.write_articles()
    x.write_title_data()
