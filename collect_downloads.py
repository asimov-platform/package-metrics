import os
import time
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from operator import itemgetter
from playwright.sync_api import sync_playwright
from github import Github
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_latest_downloads_map():
    yesterday = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
    response = supabase.table("downloads") \
        .select("*") \
        .eq("collected_at", yesterday) \
        .execute()

    latest = {}
    for row in response.data:
        key = (row["source"], row["owner"], row["name"])
        latest[key] = int(row["downloads"])
    return latest


def fetch_pypistats_downloads(name):
    url = f"https://pypistats.org/api/packages/{name}/recent"
    for attempt in range(3):
        try:
            r = requests.get(url, headers={"Accept": "application/json"}, timeout=5)
            if r.ok:
                stats = r.json().get("data", {})
                return stats.get("last_day", 0), stats.get("last_month", 0)
        except Exception:
            pass
        time.sleep(1 + attempt * 0.5)
    return 0, 0


def fetch_crates_downloads(crate):
    try:
        r = requests.get(f"https://crates.io/api/v1/crates/{crate}", timeout=5)
        if r.ok:
            return r.json()["crate"]["downloads"]
    except Exception:
        pass
    return 0


def fetch_pypi_packages(page, user):
    url = f"https://pypi.org/user/{user}/"
    try:
        page.goto(url, wait_until="load", timeout=10000)
        page.wait_for_selector("a.package-snippet", timeout=5000)
        names = page.eval_on_selector_all(
            "a.package-snippet h3.package-snippet__title",
            "els => els.map(el => el.textContent.trim())"
        )
        return [{"source": "pypi", "owner": user, "name": name} for name in names if name]
    except Exception:
        return []


def fetch_pypi_data(users, page):
    packages = []
    for user in users:
        packages.extend(fetch_pypi_packages(page, user))

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_pypistats_downloads, pkg["name"]): pkg for pkg in packages}
        for future in as_completed(futures):
            pkg = futures[future]
            daily, monthly = future.result()
            pkg["daily_downloads"] = daily
            pkg["downloads"] = monthly
    return packages


def fetch_rubygems_data(users):
    results = []
    for user in users:
        try:
            r = requests.get(f"https://rubygems.org/api/v1/owners/{user}/gems.json", timeout=5)
            if r.ok:
                for gem in r.json():
                    results.append({
                        "source": "rubygems",
                        "owner": user,
                        "name": gem["name"],
                        "downloads": gem["downloads"]
                    })
        except Exception:
            pass
    return results


def fetch_crates_data(users):
    results, seen = [], set()
    with sync_playwright() as p:
        page = p.chromium.launch(headless=True).new_page()
        for user in users:
            page_num = 1
            while True:
                url = f"https://crates.io/teams/github:{user}:rust"
                if page_num > 1:
                    url += f"?page={page_num}"
                try:
                    page.goto(url, wait_until="networkidle", timeout=10000)
                    page.wait_for_selector("a[href^='/crates/']", timeout=5000)
                except Exception:
                    break

                crates = page.eval_on_selector_all(
                    "a[href^='/crates/']",
                    "els => els.map(e => e.innerText.trim())"
                )
                new_crates = [c for c in crates if c and c not in seen]
                if not new_crates:
                    break
                for name in new_crates:
                    seen.add(name)
                    results.append({"source": "crates", "owner": user, "name": name})
                page_num += 1
        page.context.close()
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_crates_downloads, crate["name"]): crate for crate in results}
        for future in as_completed(futures):
            crate = futures[future]
            crate["downloads"] = future.result()
    return results


def fetch_github_release_downloads(token, orgs):
    gh = Github(token)
    entries = []
    for org in orgs:
        for repo in gh.get_organization(org).get_repos():
            try:
                total_dl = sum(asset.download_count for r in repo.get_releases() for asset in r.get_assets())
                entries.append({
                    "source": "github",
                    "owner": org,
                    "name": repo.name,
                    "downloads": total_dl,
                    "daily_downloads": None
                })
            except Exception:
                continue
    return entries


def compute_deltas(data, prev_map):
    for row in data:
        key = (row["source"], row["owner"], row["name"])
        current = int(row.get("downloads") or 0)
        prev = prev_map.get(key)

        if row["source"] == "pypi":
            if prev is not None:
                row["downloads"] = prev + row.get("daily_downloads", 0)
        else:
            row["daily_downloads"] = max(current - prev, 0) if prev is not None else 0
    return data


def upsert_into_supabase(data):
    collected_at = datetime.utcnow().date().isoformat()
    payload = [
        {
            "source": row["source"],
            "owner": row["owner"],
            "name": row["name"],
            "downloads": row.get("downloads", 0),
            "daily_downloads": row.get("daily_downloads", 0),
            "collected_at": collected_at
        } for row in data
    ]

    supabase.table("downloads").upsert(
        payload,
        on_conflict="source, owner, name, collected_at"
    ).execute()


def main():
    users = ["asimov-platform", "asimov-modules"]
    all_data = []
    prev_map = fetch_latest_downloads_map()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        all_data.extend(fetch_pypi_data(users, page))
        page.context.close()
        browser.close()

    all_data.extend(fetch_rubygems_data(users))
    all_data.extend(fetch_crates_data(users))
    all_data.extend(fetch_github_release_downloads(GITHUB_TOKEN, users))

    compute_deltas(all_data, prev_map)
    all_data.sort(key=itemgetter("source", "owner", "name"))

    upsert_into_supabase(all_data)
    print(f"✅ Inserted {len(all_data)} rows into Supabase")


if __name__ == "__main__":
    main()
