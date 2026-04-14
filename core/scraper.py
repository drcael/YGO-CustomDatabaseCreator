import os
import time
import requests
import cloudscraper
import zipfile
import io
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from core.background_worker import WorkerThread

class LinkScraperWorker(WorkerThread):
    def __init__(self, target_urls, save_folder, filename, delay_ms, grab_neuron=False, on_progress=None, on_complete=None, on_error=None):
        super().__init__(self.scrape_links, on_progress=on_progress, on_complete=on_complete, on_error=on_error)
        self.target_urls = target_urls
        self.save_folder = save_folder
        self.filename = filename
        self.delay_ms = delay_ms
        self.grab_neuron = grab_neuron

    def scrape_links(self, worker_instance):
        all_links = set()
        neuron_links = set()
        delay_sec = self.delay_ms / 1000.0

        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )

        for base_url in self.target_urls:
            current_url = base_url
            page_count = 1

            while current_url:
                if worker_instance.is_stopped():
                    return "Stopped by user"
                worker_instance.check_pause()

                self.on_progress(f"Scraping {current_url} (Page {page_count})...")
                
                try:
                    response = scraper.get(current_url, timeout=15)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    self.on_progress(f"Network Error: {e}")
                    return f"Stopped due to Network Error: {e}"

                soup = BeautifulSoup(response.content, 'html.parser')
                mw_pages = soup.find('div', id='mw-pages')

                if not mw_pages:
                    err = f"DOM Error: '<div id=\"mw-pages\">' not found on {current_url}"
                    self.on_progress(err)
                    
                    # Save local debug file
                    os.makedirs(self.save_folder, exist_ok=True)
                    debug_path = os.path.join(self.save_folder, "debug_error.html")
                    with open(debug_path, "w", encoding="utf-8") as f:
                        f.write(response.text)
                    
                    return err

                # Replaced inline to be safe
                links_found = mw_pages.find_all('a', href=True)
                if not links_found:
                    err = f"DOM Error: Found mw-pages, but no <a> tags inside on {current_url}"
                    self.on_progress(err)
                    return err

                # Extract links
                for a_tag in links_found:
                    # Exclude the pagination links themselves from the extracted card list
                    href = a_tag['href'].lower()
                    if "pagefrom=" in href or "subcatfrom=" in href:
                        continue
                    if 'next page' in a_tag.text.lower() or 'previous page' in a_tag.text.lower():
                        continue
                        
                    full_link = urljoin(current_url, a_tag['href'])
                    all_links.add(full_link)

                # Pagination: find "next page"
                next_page_link = None
                for a_tag in links_found:
                    if 'next page' in a_tag.text.lower():
                        next_page_link = urljoin(current_url, a_tag['href'])
                        break

                current_url = next_page_link
                page_count += 1
                time.sleep(delay_sec)

        # Handle Neuron Links specific to TCG if requested
        if self.grab_neuron and all_links:
            self.on_progress("Grabbing Neuron links...")
            for idx, link in enumerate(all_links, 1):
                if worker_instance.is_stopped():
                    return "Stopped by user"
                worker_instance.check_pause()
                
                self.on_progress(f"Scanning for neuron link {idx}/{len(all_links)}: {link}")
                try:
                    res = scraper.get(link, timeout=15)
                    res.raise_for_status()
                    
                    s = BeautifulSoup(res.content, 'html.parser')
                    # Neuron link usually under <div class="below hlist plainlinks"> ... <a ... href="...">en</a>
                    plainlinks_divs = s.find_all('div', class_='below hlist plainlinks')
                    for div in plainlinks_divs:
                        for a in div.find_all('a', href=True):
                            if "yugiohdb/card_search.action" in a['href'] and a.text.strip() == "en":
                                neuron_links.add(a['href'])
                    time.sleep(delay_sec)
                except requests.exceptions.RequestException as e:
                    self.on_progress(f"Network Error on Neuron grab {link}: {e}")

        # Save to file
        os.makedirs(self.save_folder, exist_ok=True)
        out_path = os.path.join(self.save_folder, self.filename)
        with open(out_path, 'w', encoding='utf-8') as f:
            for link in sorted(all_links):
                f.write(f"{link}\n")

        if self.grab_neuron and neuron_links:
            neuron_path = os.path.join(self.save_folder, "neuron_links.txt")
            with open(neuron_path, 'w', encoding='utf-8') as f:
                for link in sorted(neuron_links):
                    f.write(f"{link}\n")
                    
        return f"Saved {len(all_links)} links to {self.filename}"


class ZipDownloadWorker(WorkerThread):
    def __init__(self, download_urls, save_folder, on_progress=None, on_complete=None, on_error=None):
        super().__init__(self.download_zips, on_progress=on_progress, on_complete=on_complete, on_error=on_error)
        self.download_urls = download_urls
        self.save_folder = save_folder

    def download_zips(self, worker_instance):
        os.makedirs(self.save_folder, exist_ok=True)
        
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        
        for idx, url in enumerate(self.download_urls, 1):
            if worker_instance.is_stopped():
                return "Stopped by user"
            worker_instance.check_pause()

            # For Github blob URLs, convert strictly to raw
            # Example: https://github.com/drcael/misc/blob/ygo/all_links.zip 
            # -> https://raw.githubusercontent.com/drcael/misc/ygo/all_links.zip
            raw_url = url
            if "github.com" in url and "/blob/" in url:
                raw_url = url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")

            self.on_progress(f"Downloading ZIP {idx}/{len(self.download_urls)}: {url}...")
            
            try:
                response = scraper.get(raw_url, timeout=30)
                
                if response.status_code != 200:
                    self.on_progress(f"Error: File not found on server ({response.status_code}) for URL: {raw_url}")
                    return f"Stopped due to error ({response.status_code})"

                # Extract zip from memory
                with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
                    thezip.extractall(path=self.save_folder)
                    
                self.on_progress(f"Extracted {url} successfully.")
            except Exception as e:
                self.on_progress(f"Error processing {url}: {e}")
                return f"Stopped due to exception: {e}"

        return "ZIP downloads completed."
