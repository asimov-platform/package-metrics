import os
from playwright.sync_api import sync_playwright
import requests
import csv
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from operator import itemgetter
from github import Github

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BUCKET = os.environ.get("SUPABASE_BUCKET", "downloads")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")


def get_today_filename():
    return f"downloads-{datetime.now().strftime('%Y-%m-%d')}.csv"


def get_yesterday_filename():
    yesterday = datetime.now() - timedelta(days=1)
    return f"downloads-{yesterday.strftime('%Y-%m-%d')}.csv"


def fetch_previous_downloads():
    url = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{get_yesterday_filename()}"
    headers = {"Authorization": f"Bearer {SUPABASE_KEY}"}
    r = requests.get(url, headers=headers)
    if not r.ok:
        print(f"⚠️ Couldn't fetch previous file: {r.status_code}")
        return {}

    lines = r.text.splitlines()
    reader = csv.DictReader(lines)
    return {(row["source"], row["owner"], row["name"]): int(row["downloads"]) for row in reader}


def fetch_pypistats_downloads(name):
    url = f"https://pypistats.org/api/packages/{name}/recent"
    retries = 3
    for attempt in range(retries):
        try:
            r = requests.get(url, headers={"Accept": "application/json"}, timeout=5)
            if r.ok:
                stats = r.json().get("data", {})
                daily = stats.get("last_day", 0)
                monthly = stats.get("last_month", 0)
                return daily, monthly
        except Exception:
            pass
        time.sleep(1 + attempt * 0.5)
    return 0, 0


def fetch_crates_downloads(crate):
    url = f"https://crates.io/api/v1/crates/{crate}"
    try:
        r = requests.get(url, timeout=5)
        if r.ok:
            return r.json()["crate"]["downloads"]
    except Exception:
        pass
    return 0


def fetch_pypi_packages(page, user):
    url = f"https://pypi.org/user/{user}/"
    print(f"🔍 PyPI user: {user}")
    try:
        page.goto(url, wait_until="load", timeout=10000)
        page.wait_for_selector("a.package-snippet", timeout=5000)
    except Exception:
        return []

    names = page.eval_on_selector_all(
        "a.package-snippet h3.package-snippet__title",
        "els => els.map(el => el.textContent.trim())"
    )

    return [{"source": "pypi", "owner": user, "name": name} for name in names if name]


def fetch_pypi_data(users, page):
    packages = []
    for user in users:
        packages.extend(fetch_pypi_packages(page, user))

    print("⏳ Fetching PyPI daily downloads...")
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
        url = f"https://rubygems.org/api/v1/owners/{user}/gems.json"
        print(f"🔍 RubyGems user: {user}")
        try:
            r = requests.get(url, timeout=5)
            if r.ok:
                for gem in r.json():
                    results.append({
                        "source": "rubygems",
                        "owner": user,
                        "name": gem["name"],
                        "downloads": gem["downloads"]
                    })
        except Exception as e:
            print(f"❌ RubyGems failed: {e}")
    return results


def fetch_crates_data(users):
    results = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for user in users:
            page_num = 1
            while True:
                url = f"https://crates.io/teams/github:{user}:rust"
                if page_num > 1:
                    url += f"?page={page_num}"

                print(f"🔄 Crates.io user: {user} | Page: {page_num}")
                try:
                    page.goto(url, wait_until="networkidle", timeout=10000)
                    page.wait_for_selector("a[href^='/crates/']", timeout=5000)
                except Exception:
                    print("⚠️ Page load or selector timeout.")
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
                    results.append({
                        "source": "crates",
                        "owner": user,
                        "name": name
                    })

                page_num += 1

        browser.close()

    print("⏳ Fetching Crates.io total downloads...")
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
        print(f"🔍 GitHub org: {org}")
        for repo in gh.get_organization(org).get_repos():
            total_dl = 0
            try:
                for release in repo.get_releases():
                    for asset in release.get_assets():
                        total_dl += asset.download_count
                entries.append({
                    "source": "github",
                    "owner": org,
                    "name": repo.name,
                    "downloads": total_dl,
                    "daily_downloads": None
                })
            except Exception as e:
                print(f"⚠️ Skipped {repo.full_name}: {e}")

    return entries


def compute_deltas(data, prev_downloads):
    for row in data:
        key = (row["source"], row["owner"], row["name"])
        current = int(row.get("downloads") or 0)
        prev = prev_downloads.get(key)

        if row["source"] == "pypi":
            if prev is not None:
                current = prev + int(row.get("daily_downloads") or 0)
                row["downloads"] = current
        else:
            row["daily_downloads"] = max(current - prev, 0) if prev is not None else 0
    return data


def write_csv(data, filename):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["source", "owner", "name", "downloads", "daily_downloads"])
        writer.writeheader()
        writer.writerows(data)


def main():
    users = ["asimov-platform", "asimov-modules"]
    all_data = []

    prev_downloads = fetch_previous_downloads()

    print("🚀 Starting data collection...")
    start = time.perf_counter()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        all_data.extend(fetch_pypi_data(users, page))

        context.close()
        browser.close()

    all_data.extend(fetch_rubygems_data(users))
    all_data.extend(fetch_crates_data(users))
    all_data.extend(fetch_github_release_downloads(GITHUB_TOKEN, users))

    compute_deltas(all_data, prev_downloads)
    all_data.sort(key=itemgetter("source", "owner", "name"))

    filename = get_today_filename()
    write_csv(all_data, filename)

    print(f"\n✅ Saved {len(all_data)} records to {filename}")
    print(f"⏱ Total time: {time.perf_counter() - start:.2f}s")


if __name__ == "__main__":
    main()
