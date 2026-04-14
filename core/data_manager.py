import os
import json
import requests
import zipfile
import shutil

class DataManager:
    DEFAULT_CONFIG = {
        "default_save_folder": "",
        "default_db_folder": "",
        "links_pool_path": "",
        "default_links_txt": "",
        "appearance_mode": "System",
        "theme_variant": "Standard",
        "language": "English"
    }

    @staticmethod
    def get_config_path():
        # App root
        return os.path.join(os.getcwd(), "config.json")

    @classmethod
    def load_config(cls):
        path = cls.get_config_path()
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # Safety check: Reset folder if it doesn't exist
                    for key in ["default_save_folder", "default_db_folder"]:
                        folder = config.get(key, "")
                        if folder and not os.path.exists(folder):
                            config[key] = os.getcwd()
                    return config
            except:
                return cls.DEFAULT_CONFIG.copy()
        return cls.DEFAULT_CONFIG.copy()

    @classmethod
    def save_config(cls, config):
        path = cls.get_config_path()
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
        except Exception as e:
            print(f"Failed to save config: {e}")

    @staticmethod
    def download_prepared_data(url, save_path, progress_callback=None):
        """
        Safe Download: Uses .tmp extension and renames on success.
        """
        temp_path = save_path + ".tmp"
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_callback and total_size > 0:
                            progress_callback(downloaded / total_size)
            
            # Success: Rename
            if os.path.exists(save_path): os.remove(save_path)
            os.rename(temp_path, save_path)
            return True, "Download Successful."
        except Exception as e:
            if os.path.exists(temp_path): os.remove(temp_path)
            return False, f"Download Failed: {str(e)}"

    @staticmethod
    def extract_zip(zip_path, extract_to):
        try:
            os.makedirs(extract_to, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            return True, "Extraction Successful."
        except Exception as e:
            return False, f"Extraction Failed: {str(e)}"

    @staticmethod
    def parse_links_pool(file_path):
        """
        Robust Multi-line Parser:
        1. Finds the [Link-Download-Adress] block.
        2. Specifically captures every -TAG- and URL pair within.
        """
        import re
        if not file_path or not os.path.exists(file_path):
            return {}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            results = {}
            # 1. Find the main block(s)
            blocks = re.findall(r"\[Link-Download-Adress\](.*?)\[/?Link-Download-Adress\]", content, re.DOTALL)
            
            for block in blocks:
                # 2. Extract -TAG- and URL pairs
                pairs = re.findall(r"-(.*?)-\s*(https?://[^\s\n\]]+)", block)
                for tag_name, url in pairs:
                    clean_tag = f"- {tag_name.strip()} -"
                    results[clean_tag] = url.strip()
            
            return results
        except Exception as e:
            print(f"Regex Parsing Error: {e}")
            return {}

    @staticmethod
    def parse_grab_pool(file_path):
        """
        Ironclad Line-by-Line State Machine Parser for [Link-Grab-Adress].
        Handles cases where opening and closing tags are identical.
        Returns: { "-TAG-": [url1, url2], ... }
        """
        if not file_path or not os.path.exists(file_path):
            return {}
        
        grab_points = {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            in_block = False
            current_tag = None

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Toggle state on exact match (handles [Link-Grab-Adress] as both open/close)
                if line == "[Link-Grab-Adress]":
                    in_block = not in_block 
                    continue

                if in_block:
                    if line.startswith("-") and line.endswith("-"):
                        current_tag = line
                        if current_tag not in grab_points:
                            grab_points[current_tag] = []
                    elif line.startswith("http") and current_tag:
                        grab_points[current_tag].append(line)

            return grab_points
        except Exception as e:
            print(f"Ironclad Parsing Error: {e}")
            return {}
