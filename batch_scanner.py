"""
batch_scanner.py — Batch GitHub fetch layer for BDH/ResidualStore seeding.

Zone 3 purity: no imports from server.py.
This module fetches file lists from GitHub only.
Zone 2 (_orchestrate in server.py) performs all scanning and residual writes.

§48 boundary: stdlib only (json, time) + allowed project imports.
Imports allowed: github_fetcher, config.
"""
from __future__ import annotations

from github_fetcher import fetch_repo, GitHubFetchError


class BatchScanner:
    """
    Pure data-preparation layer.
    fetch_all() fetches file lists from GitHub URLs.
    server.py feeds the results through _orchestrate().
    Never raises — per-URL errors are captured in the result dict.
    """

    def fetch_all(
        self,
        urls:      list[str],
        max_files: int = 500,
        max_bytes: int = 5_242_880,
    ) -> list[dict]:
        """
        Attempt to fetch each GitHub URL.

        Returns a list of per-URL result dicts:
          { "url": str, "status": "FETCHED"|"SKIPPED"|"ERROR",
            "project_name": str,  # present when FETCHED
            "files": list,        # present when FETCHED
            "file_count": int,    # present when FETCHED
            "reason": str         # present when SKIPPED or ERROR
          }

        Never raises.
        """
        output: list[dict] = []

        for url in urls:
            try:
                project_name, files = fetch_repo(url)

                if len(files) > max_files:
                    output.append({
                        "url":    url,
                        "status": "SKIPPED",
                        "reason": f"Too many files ({len(files)})",
                    })
                    continue

                total = sum(len(f.get("content", "")) for f in files)
                if total > max_bytes:
                    output.append({
                        "url":    url,
                        "status": "SKIPPED",
                        "reason": "Project too large",
                    })
                    continue

                output.append({
                    "url":          url,
                    "status":       "FETCHED",
                    "project_name": project_name,
                    "files":        files,
                    "file_count":   len(files),
                })

            except GitHubFetchError as e:
                output.append({
                    "url":    url,
                    "status": "ERROR",
                    "reason": str(e),
                })
            except Exception as e:
                output.append({
                    "url":    url,
                    "status": "ERROR",
                    "reason": f"Unexpected: {str(e)}",
                })

        return output
