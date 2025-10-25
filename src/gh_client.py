import os
import re
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests


def _get_session() -> requests.Session:
    session = requests.Session()
    token = "ENTER_TOKEN_HERE"
    headers = {
        'Accept': 'application/vnd.github+json',
        'User-Agent': 'cnpq_project/gh_client',
    }
    if token:
        headers['Authorization'] = f'Bearer {token}'
    session.headers.update(headers)
    return session


def _respect_rate_limit(resp: requests.Response) -> None:
    if resp.status_code == 403:
        remaining = resp.headers.get('X-RateLimit-Remaining')
        reset = resp.headers.get('X-RateLimit-Reset')
        if remaining == '0' and reset:
            reset_ts = int(reset)
            now = int(time.time())
            sleep_sec = max(0, reset_ts - now) + 1
            time.sleep(sleep_sec)


def _safe_request(session: requests.Session, url: str, params: dict | None = None) -> requests.Response:
    for _ in range(5):
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 403:
                _respect_rate_limit(resp)
                continue
            if resp.status_code in (200, 201):
                return resp
            time.sleep(1.5)
        except requests.exceptions.RequestException:
            time.sleep(1.5)
    return resp


def _get_total_commits_count(session: requests.Session, commits_url: str) -> int | None:
    resp = _safe_request(session, commits_url, params={'per_page': 1})
    if resp.status_code != 200:
        return None
    link = resp.headers.get('Link')
    if not link:
        data = resp.json()
        return len(data)
    last_page = None
    for part in link.split(','):
        if 'rel="last"' in part:
            try:
                url_part = part.split(';')[0].strip().lstrip('<').rstrip('>')
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(url_part).query)
                last_page = int(q.get('page', [None])[0]) if q.get('page') else None
            except Exception:
                last_page = None
    return last_page


def _get_latest_commit_date(session: requests.Session, commits_url: str) -> str | None:
    resp = _safe_request(session, commits_url, params={'per_page': 1})
    if resp.status_code != 200:
        return None
    data = resp.json()
    if not data:
        return None
    return data[0].get('commit', {}).get('committer', {}).get('date')


def _collect_commits_since(session: requests.Session, commits_url: str, since_iso: str) -> list:
    commits: list = []
    page = 1
    while True:
        resp = _safe_request(session, commits_url, params={'since': since_iso, 'per_page': 100, 'page': page})
        if resp.status_code != 200:
            break
        batch = resp.json()
        if not batch:
            break
        commits.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return commits


def _collect_all_commits(session: requests.Session, commits_url: str) -> list:
    commits: list = []
    page = 1
    while True:
        resp = _safe_request(session, commits_url, params={'per_page': 100, 'page': page})
        if resp.status_code != 200:
            break
        batch = resp.json()
        if not batch:
            break
        commits.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return commits


def _search_issues_count(session: requests.Session, owner: str, repo: str, q: str) -> int | None:
    base = 'https://api.github.com/search/issues'
    query = f"repo:{owner}/{repo} {q}"
    resp = _safe_request(session, base, params={'q': query, 'per_page': 1})
    if resp.status_code != 200:
        return None
    return resp.json().get('total_count')


def _fetch_closed_issues_for_average(session: requests.Session, owner: str, repo: str, closed_since_iso: str) -> list:
    base = 'https://api.github.com/search/issues'
    query = f"repo:{owner}/{repo} type:issue is:closed closed:>={closed_since_iso.split('T')[0]}"
    page = 1
    results: list = []
    while True:
        resp = _safe_request(session, base, params={'q': query, 'sort': 'updated', 'order': 'desc', 'per_page': 100, 'page': page})
        if resp.status_code != 200:
            break
        data = resp.json()
        items = data.get('items', [])
        if not items:
            break
        results.extend(items)
        if len(items) < 100 or page >= 10:
            break
        page += 1
    return results


def get_gh_data():
    df = pd.read_csv('./data/filtered_itch_data.csv')

    repo_data: list[dict] = []
    session = _get_session()
    six_months_ago = datetime.now(timezone.utc) - timedelta(days=180)
    six_months_iso = six_months_ago.isoformat()

    pattern = re.compile(r'github\.com/([^/]+)/([^/]+)')

    for _, row in df.iterrows():
        github_url = row.get('Github_links')
        if not isinstance(github_url, str):
            continue
        match = pattern.search(github_url)
        if not match:
            continue

        owner = match.group(1)
        repo = match.group(2)
        repo_api = f"https://api.github.com/repos/{owner}/{repo}"
        commits_api = f"https://api.github.com/repos/{owner}/{repo}/commits"

        repo_resp = _safe_request(session, repo_api)
        if repo_resp.status_code != 200:
            continue
        repo_info = repo_resp.json()

        total_commits = _get_total_commits_count(session, commits_api)
        latest_commit_date = _get_latest_commit_date(session, commits_api)

        recent_commits = _collect_commits_since(session, commits_api, since_iso=six_months_iso)
        commits_last_6m = 0
        unique_commit_days: set[str] = set()
        for c in recent_commits:
            date_str = c.get('commit', {}).get('committer', {}).get('date')
            if not date_str:
                continue
            try:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except ValueError:
                continue
            if dt >= six_months_ago:
                commits_last_6m += 1
                unique_commit_days.add(dt.date().isoformat())

        all_commits = _collect_all_commits(session, commits_api)
        unique_committers: set[str] = set()
        for c in all_commits:
            login = (c.get('author') or {}).get('login') or (c.get('committer') or {}).get('login')
            if not login:
                login = (c.get('commit', {}).get('committer', {}).get('email')) or 'unknown'
            unique_committers.add(login)

        total_issues = _search_issues_count(session, owner, repo, 'type:issue')
        open_issues = _search_issues_count(session, owner, repo, 'type:issue state:open')
        closed_issues = _search_issues_count(session, owner, repo, 'type:issue state:closed')
        issues_opened_last_6m = _search_issues_count(session, owner, repo, f"type:issue created:>={six_months_ago.date().isoformat()}")

        closed_recent_items = _fetch_closed_issues_for_average(session, owner, repo, six_months_ago.isoformat())
        close_deltas: list[float] = []
        for it in closed_recent_items:
            created_at = it.get('created_at')
            closed_at = it.get('closed_at')
            if not created_at or not closed_at:
                continue
            try:
                cdt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                xdt = datetime.fromisoformat(closed_at.replace('Z', '+00:00'))
            except ValueError:
                continue
            if xdt >= six_months_ago:
                delta_days = (xdt - cdt).total_seconds() / 86400.0
                if delta_days >= 0:
                    close_deltas.append(delta_days)
        avg_issue_close_time_days = round(sum(close_deltas) / len(close_deltas), 2) if close_deltas else None

        created_at = repo_info.get('created_at')
        repo_age_days = None
        days_of_activity = None
        if created_at:
            try:
                created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                repo_age_days = (datetime.now(timezone.utc) - created_dt).days
                if latest_commit_date:
                    latest_commit_dt = datetime.fromisoformat(latest_commit_date.replace('Z', '+00:00'))
                    days_of_activity = (latest_commit_dt - created_dt).days
            except ValueError:
                repo_age_days = None

        repo_data.append(
            row.to_dict() | {
                'language': repo_info.get('language'),
                'createdAt': created_at,
                'repoAge': repo_age_days,
                'forks': repo_info.get('forks_count'),
                'watchers': repo_info.get('watchers_count'),
                'stars': repo_info.get('stargazers_count'),
                'openIssues': open_issues if open_issues is not None else repo_info.get('open_issues_count'),
                'closedIssues': closed_issues if closed_issues is not None else 0,
                'totalIssues': total_issues if total_issues is not None else 0,
                'issuesOpenedLast6Months': issues_opened_last_6m if issues_opened_last_6m is not None else 0,
                'AvgIssueCloseTime': avg_issue_close_time_days if avg_issue_close_time_days is not None else 0,
                'commits': total_commits,
                'dateOfLastCommit': latest_commit_date,
                'daysOfActivity': days_of_activity,
                'commitsInLast6Months': commits_last_6m if commits_last_6m is not None else 0,
                'committersParticipation': len(unique_committers) if unique_committers else 0,
            }
        )

    repo_data_df = pd.DataFrame(repo_data)
    repo_data_df.to_csv('./data/repo_data.csv', index=False)


if __name__ == "__main__":
    get_gh_data()