import os
import time
import logging
from typing import List, Dict, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from operator import itemgetter
import requests
from github import Github
from playwright.sync_api import sync_playwright
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Configuration
class Config:
    SUPABASE_URL = os.environ["SUPABASE_URL"]
    SUPABASE_KEY = os.environ["SUPABASE_KEY"]
    GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
    USERS = ["asimov-platform", "asimov-modules"]
    MAX_RETRIES = 3
    REQUEST_TIMEOUT = 5
    PAGE_LOAD_TIMEOUT = 10000
    SELECTOR_TIMEOUT = 5000
    MAX_WORKERS = 8


# Type aliases
PackageKey = Tuple[str, str, str]  # (source, owner, name)
PackageData = Dict[str, any]


def initialize_supabase() -> Client:
    """Initialize and return Supabase client."""
    if not Config.SUPABASE_URL or not Config.SUPABASE_KEY:
        raise ValueError("Supabase URL and Key must be set in environment variables")
    return create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)


def fetch_latest_downloads_map(supabase: Client) -> Dict[PackageKey, int]:
    """Fetch yesterday's download counts from Supabase."""
    try:
        yesterday = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
        response = supabase.table("downloads") \
            .select("*") \
            .eq("collected_at", yesterday) \
            .execute()

        return {
            (row["source"], row["owner"], row["name"]): int(row["downloads"])
            for row in response.data
        }
    except Exception as e:
        logger.error(f"Failed to fetch latest downloads: {e}")
        return {}


def fetch_pypistats_downloads(name: str) -> Tuple[int, int]:
    """Fetch PyPI download statistics for a package."""
    url = f"https://pypistats.org/api/packages/{name}/recent"
    for attempt in range(Config.MAX_RETRIES):
        try:
            response = requests.get(
                url,
                headers={"Accept": "application/json"},
                timeout=Config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            stats = response.json().get("data", {})
            return stats.get("last_day", 0), stats.get("last_month", 0)
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for PyPI stats {name}: {e}")
            time.sleep(1 + attempt * 0.5)
    logger.error(f"Failed to fetch PyPI stats for {name} after {Config.MAX_RETRIES} attempts")
    return 0, 0


def fetch_crates_downloads(crate: str) -> int:
    """Fetch download count for a Rust crate."""
    try:
        response = requests.get(
            f"https://crates.io/api/v1/crates/{crate}",
            timeout=Config.REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()["crate"]["downloads"]
    except Exception as e:
        logger.error(f"Failed to fetch crate downloads for {crate}: {e}")
        return 0


def fetch_pypi_packages(page, user: str) -> List[PackageData]:
    """Fetch package names for a PyPI user."""
    try:
        page.goto(f"https://pypi.org/user/{user}/",
                  wait_until="load",
                  timeout=Config.PAGE_LOAD_TIMEOUT)
        page.wait_for_selector("a.package-snippet",
                               timeout=Config.SELECTOR_TIMEOUT)
        names = page.eval_on_selector_all(
            "a.package-snippet h3.package-snippet__title",
            "els => els.map(el => el.textContent.trim())"
        )
        return [{"source": "pypi", "owner": user, "name": name}
                for name in names if name]
    except Exception as e:
        logger.error(f"Failed to fetch PyPI packages for {user}: {e}")
        return []


def fetch_pypi_data(users: List[str], page) -> List[PackageData]:
    """Fetch PyPI package data with download counts."""
    packages = []
    for user in users:
        packages.extend(fetch_pypi_packages(page, user))

    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_pypistats_downloads, pkg["name"]): pkg
                   for pkg in packages}
        for future in as_completed(futures):
            pkg = futures[future]
            try:
                daily, monthly = future.result()
                pkg["daily_downloads"] = daily
                pkg["downloads"] = monthly
            except Exception as e:
                logger.error(f"Error processing PyPI downloads for {pkg['name']}: {e}")
                pkg["daily_downloads"] = 0
                pkg["downloads"] = 0
    return packages


def fetch_rubygems_data(users: List[str]) -> List[PackageData]:
    """Fetch RubyGems package data."""
    results = []
    for user in users:
        try:
            response = requests.get(
                f"https://rubygems.org/api/v1/owners/{user}/gems.json",
                timeout=Config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            for gem in response.json():
                results.append({
                    "source": "rubygems",
                    "owner": user,
                    "name": gem["name"],
                    "downloads": gem["downloads"],
                    "daily_downloads": None
                })
        except Exception as e:
            logger.error(f"Failed to fetch RubyGems for {user}: {e}")
    return results


def fetch_crates_data(users: List[str]) -> List[PackageData]:
    """Fetch Rust crates data."""
    results, seen = [], set()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for user in users:
            page_num = 1
            while True:
                url = f"https://crates.io/teams/github:{user}:rust"
                if page_num > 1:
                    url += f"?page={page_num}"
                try:
                    page.goto(url, wait_until="networkidle",
                              timeout=Config.PAGE_LOAD_TIMEOUT)
                    page.wait_for_selector("a[href^='/crates/']",
                                           timeout=Config.SELECTOR_TIMEOUT)
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
                except Exception as e:
                    logger.error(f"Failed to fetch crates for {user}, page {page_num}: {e}")
                    break
        browser.close()

    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_crates_downloads, crate["name"]): crate
                   for crate in results}
        for future in as_completed(futures):
            crate = futures[future]
            try:
                crate["downloads"] = future.result()
                crate["daily_downloads"] = None
            except Exception as e:
                logger.error(f"Error processing crate downloads for {crate['name']}: {e}")
                crate["downloads"] = 0
                crate["daily_downloads"] = None
    return results


def fetch_github_release_downloads(token: str, orgs: List[str]) -> List[PackageData]:
    """Fetch GitHub release download counts."""
    try:
        gh = Github(token)
        entries = []
        for org in orgs:
            try:
                for repo in gh.get_organization(org).get_repos():
                    try:
                        total_dl = sum(asset.download_count
                                       for r in repo.get_releases()
                                       for asset in r.get_assets())
                        entries.append({
                            "source": "github",
                            "owner": org,
                            "name": repo.name,
                            "downloads": total_dl,
                            "daily_downloads": None
                        })
                    except Exception as e:
                        logger.error(f"Failed to process GitHub repo {org}/{repo.name}: {e}")
            except Exception as e:
                logger.error(f"Failed to fetch GitHub org {org}: {e}")
        return entries
    except Exception as e:
        logger.error(f"Failed to initialize GitHub client: {e}")
        return []


def compute_deltas(data: List[PackageData], prev_map: Dict[PackageKey, int]) -> List[PackageData]:
    """Compute daily download deltas."""
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


def upsert_into_supabase(supabase: Client, data: List[PackageData]) -> None:
    """Upsert package data into Supabase."""
    try:
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
            on_conflict="source,owner,name,collected_at"
        ).execute()
        logger.info(f"Inserted {len(payload)} rows into Supabase")
    except Exception as e:
        logger.error(f"Failed to upsert into Supabase: {e}")
        raise


def main():
    """Main function to collect and store package download statistics."""
    try:
        supabase = initialize_supabase()
        prev_map = fetch_latest_downloads_map(supabase)

        all_data = []
        all_data.extend(fetch_rubygems_data(Config.USERS))
        all_data.extend(fetch_crates_data(Config.USERS))
        all_data.extend(fetch_github_release_downloads(Config.GITHUB_TOKEN, Config.USERS))

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            all_data.extend(fetch_pypi_data(Config.USERS, page))
            browser.close()

        all_data = compute_deltas(all_data, prev_map)
        all_data.sort(key=itemgetter("source", "owner", "name"))

        upsert_into_supabase(supabase, all_data)
        logger.info("Data collection and storage completed successfully")
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        raise


if __name__ == "__main__":
    main()