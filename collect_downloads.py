from playwright.sync_api import sync_playwright
import requests
import csv
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from operator import itemgetter


def get_today_filename():
    return f"downloads-{datetime.now().strftime('%Y-%m-%d')}.csv"


def fetch_pypistats_downloads(name):
    url = f"https://pypistats.org/api/packages/{name}/recent"
    try:
        r = requests.get(url, headers={"Accept": "application/json"}, timeout=5)
        if r.ok:
            return r.json()["data"]["last_month"]
    except Exception:
        pass
    return 0


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
    except Exception as e:
        print(f"❌ Failed to load {url}: {e}")
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

    print("⏳ Fetching PyPI download counts...")
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_pypistats_downloads, pkg["name"]): pkg for pkg in packages}
        for future in as_completed(futures):
            pkg = futures[future]
            pkg["downloads"] = future.result()

    print(f"✅ PyPI downloads complete in {time.perf_counter() - start:.2f}s")
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
            print(f"❌ RubyGems failed for {user}: {e}")

    return results


def fetch_crates_data(team_url):
    results = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page_num = 1

        while True:
            url = team_url if page_num == 1 else f"{team_url}?page={page_num}"
            print(f"🔄 Crates.io: {url}")
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
                    "owner": "asimov-modules",
                    "name": name
                })

            page_num += 1

        browser.close()

    print("⏳ Fetching Crates.io download counts...")
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_crates_downloads, crate["name"]): crate for crate in results}
        for future in as_completed(futures):
            crate = futures[future]
            crate["downloads"] = future.result()

    print(f"✅ Crates.io downloads complete in {time.perf_counter() - start:.2f}s")
    return results


def write_csv(data, filename):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["source", "owner", "name", "downloads"])
        writer.writeheader()
        writer.writerows(data)


def main():
    users = ["asimov-platform", "asimov-modules"]
    crates_team_url = "https://crates.io/teams/github:asimov-modules:rust"
    all_data = []

    print("🚀 Starting data collection...\n")
    start = time.perf_counter()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        all_data.extend(fetch_pypi_data(users, page))

        context.close()
        browser.close()

    all_data.extend(fetch_rubygems_data(users))
    all_data.extend(fetch_crates_data(crates_team_url))

    all_data.sort(key=itemgetter("source", "owner", "name"))

    filename = get_today_filename()
    write_csv(all_data, filename)

    print(f"\n✅ Saved {len(all_data)} records to {filename}")
    print(f"⏱ Total time: {time.perf_counter() - start:.2f}s")


if __name__ == "__main__":
    main()
