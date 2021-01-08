# Refer: https://docs.github.com/en/free-pro-team@latest/rest/reference/repos#list-commits

import requests
from requests import utils
from datetime import timedelta
import datetime
from urllib.parse import urlparse, parse_qs
import json


class GitConnection:
    def __init__(self, user_name: str = '', access_token: str = ''):
        self.user_name = user_name
        self.access_token = access_token

    def get_last_commit_repo(self
                             , repo_name: str = ''
                             , since: str = (datetime.datetime.utcnow() - timedelta(hours=30))
                             .strftime("%Y-%m-%dT%H:%M:%SZ")
                             , page: int = 1) -> list:
        payload = {'access_token': self.access_token, 'since': since, 'page': page}
        commits = []
        response = requests.get("https://api.github.com/repos/{user_name}/{repo_name}/commits"
                                .format(user_name=self.user_name, repo_name=repo_name), params=payload)
        pagination = utils.parse_header_links(response.headers.get('Link', ''))

        for page in pagination:
            if page.get('rel') == 'next':
                o = urlparse(page.get('url'))
                query = parse_qs(o.query)
                next_page = query.get('page')[0]

                commits = self.get_last_commit_repo(repo_name=repo_name, since=since, page=next_page)

        return commits + json.loads(response.content.decode('utf8'))
