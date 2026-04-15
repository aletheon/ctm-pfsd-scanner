"""
github_fetcher.py — fetches Python files from a public GitHub repository.

Uses the GitHub Contents API (no authentication — public repos only).
Returns a list of {path, content} dicts compatible with ScaffoldGenerator.scan().

Limits:
  MAX_FILES    = 200   — max .py files per repo
  MAX_BYTES    = 5MB   — max total content size
  FILE_TIMEOUT = 10s   — per-file fetch timeout

§48 boundary: imports only stdlib.
"""
from __future__ import annotations

import json
import os
import re
import urllib.request
import urllib.error
from typing import Optional

MAX_FILES    = 200
MAX_BYTES    = 5 * 1024 * 1024
FILE_TIMEOUT = 10


class GitHubFetchError(Exception):
    """Raised for any fetch failure. Message is user-facing."""
    pass


def parse_github_url(url: str) -> tuple[str, str, str]:
    """
    Parse a GitHub URL into (owner, repo, ref).
    Supports:
      https://github.com/owner/repo
      https://github.com/owner/repo/tree/branch
    Returns ref = "HEAD" if no branch specified.
    """
    url = url.strip().rstrip("/")
    pattern = r"^https?://github\.com/([^/]+)/([^/]+)(?:/tree/([^/]+))?(?:/.*)?$"
    match = re.match(pattern, url)
    if not match:
        raise GitHubFetchError(
            f"Not a valid GitHub repository URL: {url}. "
            "Expected format: https://github.com/owner/repo"
        )
    owner = match.group(1)
    repo  = match.group(2).replace(".git", "")
    ref   = match.group(3) or "HEAD"
    return owner, repo, ref


def fetch_file_list(owner: str, repo: str, ref: str,
                    path: str = "") -> list[dict]:
    """
    Recursively list all .py files in the repo via GitHub Contents API.
    Returns list of {path, download_url} dicts.
    Stops once MAX_FILES is reached.
    """
    api_url = (
        f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
        f"?ref={ref}"
    )
    token = os.environ.get("GITHUB_TOKEN")
    headers = {
        "Accept":     "application/vnd.github.v3+json",
        "User-Agent": "ctm-pfsd-scanner/1.1",
    }
    if token:
        headers["Authorization"] = f"token {token}"
    req = urllib.request.Request(api_url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=FILE_TIMEOUT) as resp:
            entries = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise GitHubFetchError(
                f"Repository not found or is private: "
                f"https://github.com/{owner}/{repo}"
            )
        raise GitHubFetchError(f"GitHub API error {e.code} for {api_url}")
    except urllib.error.URLError as e:
        raise GitHubFetchError(f"Network error fetching file list: {e.reason}")

    if not isinstance(entries, list):
        return []   # single-file response — not a directory

    results = []
    for entry in entries:
        if len(results) >= MAX_FILES:
            break
        name  = entry.get("name", "")
        etype = entry.get("type", "")
        if etype == "file" and name.endswith(".py") and not name.startswith("."):
            results.append({
                "path":         entry.get("path", ""),
                "download_url": entry.get("download_url", ""),
            })
        elif etype == "dir" and not name.startswith(".") \
                and name not in ("__pycache__", ".git", "node_modules", "venv"):
            sub = fetch_file_list(owner, repo, ref, entry.get("path", ""))
            results.extend(sub[:MAX_FILES - len(results)])

    return results[:MAX_FILES]


def fetch_file_content(download_url: str) -> Optional[str]:
    """Fetch one file's content. Returns None on error."""
    if not download_url:
        return None
    try:
        token = os.environ.get("GITHUB_TOKEN")
        headers = {"User-Agent": "ctm-pfsd-scanner/1.1"}
        if token:
            headers["Authorization"] = f"token {token}"
        req = urllib.request.Request(download_url, headers=headers)
        with urllib.request.urlopen(req, timeout=FILE_TIMEOUT) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return None


def fetch_repo(url: str) -> tuple[str, list[dict]]:
    """
    Main entry point. Fetches all .py files from a GitHub repo URL.

    Returns:
        (project_name, files)  where files = [{path, content}]

    Raises GitHubFetchError on any unrecoverable error.
    """
    owner, repo, ref = parse_github_url(url)
    project_name = repo

    file_list = fetch_file_list(owner, repo, ref)
    if not file_list:
        raise GitHubFetchError(
            f"No Python (.py) files found in "
            f"https://github.com/{owner}/{repo}"
        )

    files = []
    total_bytes = 0
    for entry in file_list:
        content = fetch_file_content(entry["download_url"])
        if content is None:
            continue
        total_bytes += len(content)
        if total_bytes > MAX_BYTES:
            raise GitHubFetchError(
                "Repository too large (max 5MB total Python content)."
            )
        files.append({"path": entry["path"], "content": content})

    if not files:
        raise GitHubFetchError(
            f"No readable Python files found in "
            f"https://github.com/{owner}/{repo}"
        )

    return project_name, files
