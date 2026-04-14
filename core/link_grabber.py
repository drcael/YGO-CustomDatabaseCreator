import os
import time
import cloudscraper
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from core.background_worker import WorkerThread
from core.data_manager import DataManager

class LinkGrabberThread(WorkerThread):
    def __init__(self, categories, grab_neuron, delay_ms, app_root, on_log=None, on_progress=None, on_complete=None, on_error=None):
        """
        Link Grabber specialized thread for automated Yugipedia scraping.
        """
        super().__init__(self.execute_grab, on_progress=on_progress, on_complete=on_complete, on_error=on_error)
        self.categories = categories # List of stable English keys from UI
        self.grab_neuron = grab_neuron
        self.delay_ms = delay_ms
        self.app_root = app_root
        self.on_log = on_log
        
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        self.scraper.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://yugipedia.com/'
        })
        
        # Complete Tag Mapping matching links_pool.txt [Link-Grab-Adress] tags
        self.CAT_MAP = {
            "OCG+TCG": "-OCG+TCG-",
            "TCG": "-TCG-",
            "OCG": "-OCG-",
            "TCG Speed Duel": "-TCG Speed Duel-",
            "Rush Duel": "-Rush Duel-",
            "Other (Token+Skill)": "-Other (Token+Skill)-",
            "Anime": "-Anime-"
        }

    def _normalize(self, text):
        """Helper to strip spaces, dashes, and lowercase for fuzzy matching."""
        if not text: return ""
        return text.replace("-", "").replace(" ", "").lower()

    def execute_grab(self, worker):
        """
        Main task logic for card link extraction.
        """
        # 1. Load starting points from links_pool.txt
        config = DataManager.load_config()
        pool_path = config.get("links_pool_path")
        grab_points = DataManager.parse_grab_pool(pool_path)
        
        if not grab_points:
            return "Error: Could not parse [Link-Grab-Adress] from links_pool.txt"

        # 2. Fuzzy Matching & Deduplication
        # Normalize grab_points keys (from links_pool.txt)
        normalized_pool = {self._normalize(k): v for k, v in grab_points.items()}
        
        pool_tags_to_process = set()
        
        # Determine which internal keys to check
        keys_to_check = self.CAT_MAP.keys() if "ALL" in self.categories else self.categories

        for cat in keys_to_check:
            mapped_tag = self.CAT_MAP.get(cat, "")
            norm_tag = self._normalize(mapped_tag)
            
            if norm_tag in normalized_pool:
                # Find the original key in grab_points that produced this normalized tag
                original_keys = [k for k in grab_points.keys() if self._normalize(k) == norm_tag]
                if original_keys:
                    pool_tags_to_process.add(original_keys[0])
            else:
                self._safe_log(f"Warning: Category '{cat}' (tag {mapped_tag}) not found in links_pool.txt")

        if not pool_tags_to_process:
            return "Error: No matching categories found to scrape. Check UI selection or links_pool.txt formatting."

        all_grabbed_links = set()

        # 3. Category Scraping Loop
        for tag in sorted(list(pool_tags_to_process)):
            if worker.is_stopped(): break
            
            raw_urls = grab_points[tag]
            
            # SAFEGUARD: Prevent iterating over characters if it's a single string
            if isinstance(raw_urls, str):
                # Split by whitespace to catch multiple URLs in one string, or just make it a list
                target_urls = [u for u in raw_urls.split() if u.startswith("http")]
                if not target_urls:
                    target_urls = [raw_urls] # Fallback
            elif isinstance(raw_urls, list):
                target_urls = raw_urls
            else:
                continue

            for base_url in target_urls:
                current_url = base_url
                page_count = 1
                
                while current_url:
                    if worker.is_stopped(): break
                    worker.check_pause()
                    
                    self._safe_log(f"Scraping category {tag} - Page {page_count}...")
                    
                    # Anti-Ban Safety: Mandatory Delay
                    time.sleep(self.delay_ms / 1000.0)
                    
                    try:
                        res = self.scraper.get(current_url, timeout=30)
                        res.raise_for_status()
                        
                        soup = BeautifulSoup(res.content, 'html.parser')
                        mw_pages = soup.find('div', id='mw-pages')
                        
                        if mw_pages:
                            links = mw_pages.find_all('a', href=True)
                            for a in links:
                                href = a['href'].lower()
                                if any(x in href for x in ["pagefrom=", "subcatfrom=", "mw-pages"]):
                                    continue
                                if "next page" in a.text.lower() or "previous page" in a.text.lower():
                                    continue
                                
                                full_link = urljoin(current_url, a['href'])
                                all_grabbed_links.add(full_link)
                            
                            # Pagination: "next page"
                            next_page = mw_pages.find('a', string=lambda t: t and 'next page' in t.lower())
                            if next_page:
                                current_url = urljoin(current_url, next_page['href'])
                                page_count += 1
                            else:
                                current_url = None
                        else:
                            self._safe_log(f"Warning: No mw-pages found on {current_url}")
                            current_url = None
                            
                    except Exception as e:
                        self._safe_log(f"Network Error on {tag}: {e}")
                        break

        if worker.is_stopped(): return "Stopped by user"

        # 4. Optional Deep Neuron Scan
        neuron_links = set()
        if self.grab_neuron and all_grabbed_links:
            total = len(all_grabbed_links)
            self._safe_log(f"Starting Deep Scan for Neuron Links ({total} cards)...")
            
            for idx, link in enumerate(sorted(all_grabbed_links), 1):
                if worker.is_stopped(): break
                worker.check_pause()
                
                # LOG FORMAT MATCH: X/Y url processing....
                self._safe_log(f"{idx}/{total} {link} processing....")
                
                # Anti-Ban Safety: Mandatory Delay
                time.sleep(self.delay_ms / 1000.0)
                
                try:
                    res = self.scraper.get(link, timeout=12)
                    if res.status_code == 200:
                        s = BeautifulSoup(res.content, 'html.parser')
                        # Precise DOM Targeting
                        plainlinks = s.find('div', class_='below hlist plainlinks')
                        if plainlinks:
                            anc = plainlinks.find('a', href=True, string=lambda t: t and t.strip() == 'en')
                            if anc and "yugiohdb/card_search.action" in anc['href']:
                                neuron_links.add(anc['href'])
                except Exception:
                    continue 

        # 5. Output Management
        if hasattr(self, 'custom_save_dir') and self.custom_save_dir:
            links_dir = self.custom_save_dir
        else:
            links_dir = os.path.join(self.app_root, "links")
        os.makedirs(links_dir, exist_ok=True)
        
        # Save aggregated card links
        timestamp = time.strftime("%Y%H%M")
        cat_tag = "ALL" if "ALL" in self.categories else "_".join([cat.replace(" ", "") for cat in self.categories])
        out_path = os.path.join(links_dir, f"links_{cat_tag}_{timestamp}.txt")
        
        with open(out_path, 'w', encoding='utf-8') as f:
            for l in sorted(all_grabbed_links):
                f.write(f"{l}\n")
        
        # Save separate neuron links
        if neuron_links:
            neuron_path = os.path.join(links_dir, "neuron_links.txt")
            with open(neuron_path, 'w', encoding='utf-8') as f:
                for nl in sorted(neuron_links):
                    f.write(f"{nl}\n")

        msg = f"Completed! Grabbed {len(all_grabbed_links)} card links."
        if neuron_links:
            msg += f" Extracted {len(neuron_links)} Neuron links."
        
        return msg

    def _safe_log(self, msg):
        if self.on_progress:
            self.on_progress(msg)
        if self.on_log:
            self.on_log(msg)
