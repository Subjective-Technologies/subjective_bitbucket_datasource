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
        """Return the SVG code for the Bitbucket icon."""
        return """
<svg viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
  <g id="SVGRepo_bgCarrier" stroke-width="0"></g>
  <g id="SVGRepo_tracerCarrier" stroke-linecap="round" stroke-linejoin="round"></g>
  <g id="SVGRepo_iconCarrier">
    <path d="M2.9087 3.00008C2.64368 2.99655 2.3907 3.11422 2.21764 3.32152C2.04458 3.52883 1.96915 3.80455 2.01158 4.07472L5.81987 27.9484C5.91782 28.5515 6.42093 28.9949 7.01305 28.9999H25.283C25.7274 29.0058 26.109 28.6748 26.1801 28.2217L29.9884 4.07935C30.0309 3.80918 29.9554 3.53346 29.7824 3.32615C29.6093 3.11885 29.3563 3.00118 29.0913 3.00471L2.9087 3.00008ZM18.9448 20.2546H13.1135L11.5346 11.7362H20.3578L18.9448 20.2546Z" fill="#2684FF" data-darkreader-inline-fill="" style="--darkreader-inline-fill: #004eb5;"></path>
    <path fill-rule="evenodd" clip-rule="evenodd" d="M28.7778 11.7363H20.3582L18.9453 20.2547H13.114L6.22852 28.6944C6.44675 28.8892 6.725 28.9976 7.0135 29.0001H25.2879C25.7324 29.006 26.114 28.675 26.1851 28.2219L28.7778 11.7363Z" fill="url(#paint0_linear_87_7932)"></path>
    <defs>
      <linearGradient id="paint0_linear_87_7932" x1="30.7245" y1="14.1218" x2="20.5764" y2="28.0753" gradientUnits="userSpaceOnUse">
        <stop offset="0.18" stop-color="#0052CC" data-darkreader-inline-stopcolor="" style="--darkreader-inline-stopcolor: #0042a3;"></stop>
        <stop offset="1" stop-color="#2684FF" data-darkreader-inline-stopcolor="" style="--darkreader-inline-stopcolor: #004eb5;"></stop>
      </linearGradient>
    </defs>
  </g>
</svg>
        """

    def get_connection_data(self):
        """
        Return the connection type and required fields for Bitbucket.
        """
        return {
            "connection_type": "BitBucket",
            "fields": ["username", "token", "target_directory"]
        }

