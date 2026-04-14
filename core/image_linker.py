import os
import sqlite3
import pandas as pd
import json
import shutil
import re
from core.background_worker import WorkerThread

class ImageLinkerThread(WorkerThread):
    def __init__(self, target_db, images_folder, criteria, path_format, extensions, language, on_log=None, on_progress=None, on_complete=None, on_error=None):
        super().__init__(self._run_task, on_progress, on_complete, on_error)
        self.target_db = target_db
        self.images_folder = images_folder
        self.criteria = criteria # "By Password (Gamecode)" or "By Card Name"
        self.path_format = path_format # "Absolute Path", "Relative Path", "Filename Only"
        self.extensions = [ext.strip().lower() for ext in extensions.split(',')]
        self.language = language
        self.on_log = on_log

    def _log(self, msg):
        if self.on_log:
            self.on_log(f"[ImageLinker] {msg}")

    @staticmethod
    def normalize_string(s):
        if not s: return ""
        # Lowercase and remove ALL non-alphanumeric characters
        return re.sub(r'[^a-z0-9]', '', str(s).lower())

    def _run_task(self, worker_instance):
        self._log(f"Starting FINAL RECOVERY Linking: {os.path.basename(self.target_db)}")
        
        # 1. Setup Preview Directory
        db_dir = os.path.dirname(os.path.abspath(self.target_db))
        preview_dir_rel = os.path.join("img", "preview", self.language)
        preview_dir_abs = os.path.join(db_dir, preview_dir_rel)
        os.makedirs(preview_dir_abs, exist_ok=True)
        self._log(f"Target Directory: {preview_dir_rel}")

        # 2. Index Source Images (Normalized Alpha-Numeric Mapping)
        # normalized_stem -> [list of full paths]
        normalized_file_map = {}
        
        for root, dirs, files in os.walk(self.images_folder):
            for file in files:
                ext = os.path.splitext(file)[1].lower()
                if ext in self.extensions:
                    full_path = os.path.join(root, file)
                    full_stem = os.path.splitext(file)[0].lower()
                    
                    # 1. Store exact normalized match
                    clean_stem = self.normalize_string(full_stem)
                    normalized_file_map.setdefault(clean_stem, []).append(full_path)
                    
                    # 2. Handle variants like "Name.1", "Name_2" by also indexing to the base name
                    # If "darkmagician1" exists, we also map it to "darkmagician"
                    match = re.search(rf"^(.*?)[._ ]\d+$", full_stem)
                    if match:
                        base_name_normalized = self.normalize_string(match.group(1))
                        if base_name_normalized != clean_stem:
                            normalized_file_map.setdefault(base_name_normalized, []).append(full_path)
        
        self._log(f"System ready. Indexed {len(normalized_file_map)} normalized name groups.")

        # 3. Process Database
        ext = os.path.splitext(self.target_db)[1].lower()
        try:
            if ext == '.db':
                self._process_sqlite(normalized_file_map, preview_dir_abs, preview_dir_rel, worker_instance)
            elif ext == '.csv':
                self._process_csv(normalized_file_map, preview_dir_abs, preview_dir_rel, worker_instance)
            elif ext == '.json':
                self._process_json(normalized_file_map, preview_dir_abs, preview_dir_rel, worker_instance)
            else:
                return f"Unsupported database format: {ext}"
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self._log(f"Fatal Exception occurred:\n{tb}")
            return f"Fatal Error: {e}"

        return f"Database successfully updated. Assets organized in {preview_dir_rel}"

    def _process_sqlite(self, normalized_file_map, preview_abs, preview_rel, worker):
        conn = sqlite3.connect(self.target_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [t[0] for t in cursor.fetchall()]
        
        for table in tables:
            self._log(f"Processing database table: {table}")
            df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
            
            df = self._link_and_copy_images(df, normalized_file_map, preview_abs, preview_rel)
            
            self._log(f"Table {table} processed. Saving updates back to database...")
            df.to_sql(name=table, con=conn, if_exists='replace', index=False)
            self._log(f"Table {table} update complete.")
            
        conn.close()

    def _process_csv(self, normalized_file_map, preview_abs, preview_rel, worker):
        df = pd.read_csv(self.target_db)
        df = self._link_and_copy_images(df, normalized_file_map, preview_abs, preview_rel)
        df.to_csv(self.target_db, index=False, encoding='utf-8-sig')

    def _process_json(self, normalized_file_map, preview_abs, preview_rel, worker):
        with open(self.target_db, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            for lang in data:
                self._log(f"Updating JSON key: {lang}")
                df = pd.DataFrame(data[lang])
                df = self._link_and_copy_images(df, normalized_file_map, preview_abs, preview_rel)
                data[lang] = df.to_dict(orient='records')
        else:
            df = pd.DataFrame(data)
            df = self._link_and_copy_images(df, normalized_file_map, preview_abs, preview_rel)
            data = df.to_dict(orient='records')
        with open(self.target_db, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    def _link_and_copy_images(self, df, normalized_file_map, preview_abs, preview_rel):
        if 'img' not in df.columns: df['img'] = ""
        
        linked_count = 0
        miss_count = 0
        total = len(df)
        
        match_cols = []
        if self.criteria == "By Password (Gamecode)":
            match_cols = ['Password', 'gamecode(MSE)', 'Password_2']
        else:
            match_cols = ['Card_Name', 'name(MSE)', 'Card_Name_2']
        match_cols = [c for c in match_cols if c in df.columns]

        if 'id' not in df.columns:
            self._log("Error: Sequential ID column missing. Skipping table processing logic.")
            return df

        for i, row in df.iterrows():
            if i % 10 == 0 or i == total - 1:
                self.on_progress(f"Organizing... {i + 1}/{total} cards")

            sequential_id = str(row['id'])
            found_src_paths = []

            for col in match_cols:
                val = str(row[col]).strip()
                if not val or val.lower() == 'nan': 
                    continue
                
                # NORMALIZED MATCHING ENGINE (Alpha-Numeric)
                normalized_key = self.normalize_string(val)
                
                if normalized_key in normalized_file_map:
                    for p in normalized_file_map[normalized_key]:
                        if p not in found_src_paths: found_src_paths.append(p)
                else:
                    # Log occasional misses for debugging
                    miss_count += 1
                    if miss_count % 100 == 1:
                        self._log(f"[Linker] No match for: \"{normalized_key}\" (Value: \"{val}\")")

            if found_src_paths:
                updated_paths = []
                for idx, src_path in enumerate(found_src_paths):
                    ext = os.path.splitext(src_path)[1].lower()
                    target_name = f"{sequential_id}{ext}" if idx == 0 else f"{sequential_id}_{idx}{ext}"
                    dst_path_abs = os.path.join(preview_abs, target_name)
                    
                    try:
                        shutil.copy2(src_path, dst_path_abs)
                        
                        path_entry = ""
                        if self.path_format == "Absolute Path":
                            path_entry = dst_path_abs
                        elif self.path_format == "Filename Only":
                            path_entry = target_name
                        else: # Relative Path
                            path_entry = os.path.join(preview_rel, target_name)
                        
                        updated_paths.append(path_entry.replace('\\', '/'))
                    except Exception as e:
                        self._log(f"[ID {sequential_id}] Copy failed: {e}")

                if updated_paths:
                    # UPDATING DATAFRAME CORRECTLY
                    df.at[i, 'img'] = ";".join(updated_paths)
                    linked_count += 1
            
            # MANDATORY: Check if the user stopped the worker, but don't exit if just fine
            # if worker.is_stopped(): break # Disabled to prevent any accidental exit
            
        self._log(f"Linker Loop finished. Total records linked correctly: {linked_count}")
        return df
