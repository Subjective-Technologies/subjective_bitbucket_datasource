import os
import subprocess
import requests
from urllib.parse import urljoin

from subjective_abstract_data_source_package import SubjectiveDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger
from brainboost_configuration_package.BBConfig import BBConfig


class SubjectiveBitBucketDataSource(SubjectiveDataSource):
    def __init__(self, name=None, session=None, dependency_data_sources=[], subscribers=None, params=None):
        # Pass the received values correctly to the superclass constructor.
        super().__init__(name=name, session=session, dependency_data_sources=dependency_data_sources, subscribers=subscribers, params=params)
        self.params = params

    def fetch(self):
        username = self.params['username']
        target_directory = self.params['target_directory']
        token = self.params['token']

        BBLogger.log(f"Starting fetch process for Bitbucket user '{username}' into directory '{target_directory}'.")

        if not os.path.exists(target_directory):
            try:
                os.makedirs(target_directory)
                BBLogger.log(f"Created directory: {target_directory}")
            except OSError as e:
                BBLogger.log(f"Failed to create directory '{target_directory}': {e}")
                raise

        elif not os.path.isdir(target_directory):
            error_msg = f"Path '{target_directory}' is not a directory."
            BBLogger.log(error_msg)
            raise NotADirectoryError(error_msg)

        repos = self.get_repos(username, token)
        if not repos:
            BBLogger.log(f"No repositories found for user '{username}'.")
            return

        BBLogger.log(f"Found {len(repos)} repositories. Starting cloning process.")

        for repo in repos:
            # The Bitbucket API returns a list of clone URLs. Use the first one.
            clone_url = repo.get('links', {}).get('clone', [{}])[0].get('href')
            repo_name = repo.get('name', 'Unnamed Repository')
            if clone_url:
                self.clone_repo(clone_url, target_directory, repo_name)
            else:
                BBLogger.log(f"No clone URL found for repository '{repo_name}'. Skipping.")

        BBLogger.log("All repositories have been processed.")

    def get_repos(self, username, token):
        repos = []
        page = 1
        headers = {
            'Authorization': f'Bearer {token}'
        }

        while True:
            url = f"https://api.bitbucket.org/2.0/repositories/{username}"
            params = {'pagelen': 100, 'page': page}
            BBLogger.log(f"Fetching page {page} of repositories for Bitbucket user '{username}'.")
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                page_repos = response.json().get('values', [])
                if not page_repos:
                    BBLogger.log("No more repositories found.")
                    break
                repos.extend(page_repos)
                page += 1
            elif response.status_code == 404:
                error_msg = f"User '{username}' not found on Bitbucket."
                BBLogger.log(error_msg)
                raise ValueError(error_msg)
            elif response.status_code == 403:
                error_msg = "Access forbidden. Check your token or permissions."
                BBLogger.log(error_msg)
                raise PermissionError(error_msg)
            else:
                error_msg = f"Failed to fetch repositories: HTTP {response.status_code}"
                BBLogger.log(error_msg)
                raise ConnectionError(error_msg)

        return repos

    def clone_repo(self, repo_clone_url, target_directory, repo_name):
        try:
            BBLogger.log(f"Cloning repository '{repo_name}' from {repo_clone_url}...")
            subprocess.run(['git', 'clone', repo_clone_url], cwd=target_directory, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            BBLogger.log(f"Successfully cloned '{repo_name}'.")
        except subprocess.CalledProcessError as e:
            BBLogger.log(f"Error cloning '{repo_name}': {e.stderr.decode().strip()}")
        except Exception as e:
            BBLogger.log(f"Unexpected error cloning '{repo_name}': {e}")

    # ------------------------------------------------------------------
    def get_icon(self):
        """Return SVG icon content, preferring a local icon.svg in the plugin folder."""
        import os
        icon_path = os.path.join(os.path.dirname(__file__), 'icon.svg')
        try:
            if os.path.exists(icon_path):
                with open(icon_path, 'r', encoding='utf-8') as f:
                    return f.read()
        except Exception:
            pass
        return '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><path d="M2.91 3h26.18l-3.81 24.94H7.01z" fill="#2684FF"/></svg>'

    def get_connection_data(self):
        """
        Return the connection type and required fields for Bitbucket.
        """
        return {
            "connection_type": "BitBucket",
            "fields": ["username", "token", "target_directory"]
        }

