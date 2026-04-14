import os
import time
import pandas as pd
from datetime import datetime
from core.background_worker import WorkerThread
from core.card_parser import YugipediaParser
import traceback
import sqlite3
import json
import shutil
from deep_translator import GoogleTranslator

lang_code_map = {
    "Arabic": "ar", "Bengali": "bn", "Czech": "cs", "Danish": "da", 
    "Dutch": "nl", "French": "fr", "Finnish": "fi", "German": "de", 
    "Greek": "el", "Hindi": "hi", "Hungarian": "hu", "Indonesian": "id", 
    "Italian": "it", "Japanese": "ja", "Korean": "ko", "Malay": "ms", 
    "Norwegian": "no", "Polish": "pl", "Portuguese": "pt", "Romanian": "ro", 
    "Russian": "ru", "Spanish": "es", "Swedish": "sv", "Tagalog": "tl", 
    "Thai": "th", "Turkish": "tr", "Vietnamese": "vi",
    "S. Chinese": "zh-CN", "T. Chinese": "zh-TW"
}

class DatabaseUpdaterThread(WorkerThread):
    LANG_MAP = {
        'S. Chinese': 'Simplified Chinese',
        'T. Chinese': 'Traditional Chinese',
    }
    
    def __init__(self, links_file, existing_db_path, languages, translate_missing, is_all_set, delay_ms, export_formats, on_log=None, on_progress=None, on_complete=None, on_error=None, initial_rows=None):
        super().__init__(self._run_task, on_progress, on_complete, on_error)
        self.links_file = links_file
        self.existing_db_path = existing_db_path
        self.saving_folder = os.path.dirname(os.path.abspath(existing_db_path)) if existing_db_path else ""
        self.languages = languages
        self.translate_missing = translate_missing
        self.is_all_set = is_all_set
        self.delay_ms = delay_ms
        self.export_formats = export_formats
        self.on_log = on_log
        self.translation_cache = {}  # (original_text, target_code) -> translated_text
        self.initial_rows = initial_rows
        self.language_rows = {lang: [] for lang in (languages if languages else ["Default"])}
        self.new_urls = []
        self.retry_counts = {}
        
        self.parser = YugipediaParser()
        self.base_columns = [
            'id', 'url', 'Card_Type', 'Card_Name', 'Password', 'Status', 'Property', 'Effect _Types', 
            'Genesys_Point/Status', 'Monster_Attribute', 'Monster_Type', 'Level', 'ATK', 'DEF', 'Rank', 
            'Link_Value', 'Link_Position', 'Pendulum_Scale', 'Rule_Text', 'Pendulum _Text', 'Archetypes',
            'card_type(MSE)', 'name(MSE)', 'attribute(MSE)', 'level(MSE)', 'rule_text(MSE)', 'gamecode(MSE)', 
            'Type_1(MSE)', 'Type_2(MSE)', 'Type_3(MSE)', 'Type_4(MSE)', 'Type_5(MSE)', 'monster_type(MSE)', 
            'attack(MSE)', 'defense(MSE)', 'pendulum_text(MSE)', 'blue_scale(MSE)', 'red_scale(MSE)', 
            'linkul(MSE_Link_Top-Left) ', 'linku(MSE_Link_Top-Center)', 'linkur(MSE_Link_Top-Right)', 
            'linkl(MSE_Link_Middle-Left)', 'linkdl(MSE_Link_Bottom-Left)', 'linkr(MSE_Link_Middle-Right)', 
            'linkd(MSE_Link_Bottom-Center)', 'linkdr (MSE-Link/Down-Right)', 'linkdr(MSE_Link_Bottom-Right)', 'img'
        ]

    def _translate_with_retry(self, text, target_code, log_name="Unknown"):
        if not text or not str(text).strip():
            return text
            
        text = str(text).strip()
        cache_key = (text, target_code)
        
        # 1. INIT CACHE PROPERLY (Ensure it exists at the class instance level, NOT loop level)
        if not hasattr(self, 'translation_cache'):
            self.translation_cache = {}
            
        # 2. CHECK CACHE (INSTANT RETURN, NO DELAY)
        if cache_key in self.translation_cache:
            return self.translation_cache[cache_key]
            
        # 3. CACHE MISS: DO TRANSLATION
        if self.on_log: self.on_log(f"Translating: {log_name} to {target_code}...")
        for attempt in range(3):
            try:
                from deep_translator import GoogleTranslator
                import time
                result = GoogleTranslator(source='en', target=target_code).translate(text)
                time.sleep(self.delay_ms / 1000.0) # ONLY sleep on actual API calls
                self.translation_cache[cache_key] = result
                return result
            except Exception as e:
                time.sleep(2)
                
        return text # Fallback on absolute failure

    def _get_max_id_and_urls(self, conn, table_name="table_Default"):
        """Get the maximum ID and all processed URLs from a table."""
        processed_urls = set()
        max_id = 0
        try:
            cursor = conn.cursor()
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if cursor.fetchone():
                cursor.execute(f'SELECT id, url FROM "{table_name}"')
                rows = cursor.fetchall()
                for r in rows:
                    if r[0] and isinstance(r[0], int) and r[0] > max_id:
                        max_id = r[0]
                    if r[1]:
                        processed_urls.add(r[1])
        except Exception as e:
            if self.on_log: self.on_log(f"Warning: Could not fetch URLs from {table_name}: {e}")
        return max_id, processed_urls

    def _run_task(self, worker_instance):
        if self.on_log:
            self.on_log("Starting database update process...")
            

        with open(self.links_file, 'r', encoding='utf-8') as f:
            all_urls = [line.strip() for line in f.readlines() if line.strip()]
        
        langs_to_process = self.languages if self.languages else ["Default"]
        
        # --- SUPREME PRE-SCRAPE OVERRIDE ---
        try:
            _lang = langs_to_process[0]
            _lang_suffix = f"_{_lang}" if _lang != "Default" else ""
            _csv_orig = os.path.join(self.saving_folder, f'database{_lang_suffix}.csv')
            _content = ""
            if os.path.exists(self.existing_db_path) and self.existing_db_path.endswith('.json'):
                with open(self.existing_db_path, 'r', encoding='utf-8') as f: _content = f.read(1000)
            elif os.path.exists(_csv_orig):
                with open(_csv_orig, 'r', encoding='utf-8-sig') as f: _content = f.readline()
                
            if "Set_Release_Date" in _content: self.is_all_set = True
            elif "First TCG Set Release Date" in _content: self.is_all_set = False
        except Exception: pass
        
        # Deduplication logic
        processed_urls = set()
        self.max_id_overall = 0
        
        if os.path.exists(self.existing_db_path):
            conn = sqlite3.connect(self.existing_db_path)
            for lang in langs_to_process:
                t_name = f"table_{lang}" if lang != "Default" else "table_Default"
                m_id, urls = self._get_max_id_and_urls(conn, t_name)
                processed_urls.update(urls)
                if m_id > self.max_id_overall:
                    self.max_id_overall = m_id

            conn.close()
            if self.on_log:
                self.on_log(f"Found {len(processed_urls)} already processed URLs. Max ID: {self.max_id_overall}")
        else:
            if self.on_log:
                self.on_log(f"Could not find existing DB at {self.existing_db_path}. Starting fresh.")
                
        # Filter urls
        self.new_urls = [u for u in all_urls if u not in processed_urls]
        
        if not self.new_urls and not self.initial_rows:
            return "No new URLs found to update."
            
        if self.initial_rows:
            for lang, rows in self.initial_rows.items():
                if lang in self.language_rows:
                    self.language_rows[lang].extend(rows)
            if self.on_log: self.on_log(f"Resumed session with {len(next(iter(self.initial_rows.values()))) if self.initial_rows else 0} pre-processed cards.")

        total_to_process = len(self.new_urls)
        processed_count = 0
        failed_count = 0
        sequential_id = self.max_id_overall + 1 + (len(next(iter(self.language_rows.values()))) if any(self.language_rows.values()) else 0)
        
        while self.new_urls:
            if worker_instance.is_stopped():
                return "Stopped by user"
            worker_instance.check_pause()

            url = self.new_urls.pop(0)
            try:
                card_data = self.parser.parse_card(url)
                if card_data:
                    name = card_data.get('Name', 'Unknown')
                    for lang in langs_to_process:
                        rows = self._process_card(card_data, url, sequential_id, lang)
                        self.language_rows[lang].extend(rows)
                    sequential_id += 1
                    processed_count += 1
                else:
                    raise Exception("Empty payload returned from parser.")
            except Exception as e:
                # Retry Logic
                self.retry_counts[url] = self.retry_counts.get(url, 0) + 1
                
                # Check for 404
                is_404 = "404" in str(e)
                
                if self.retry_counts[url] < 2 and not is_404:
                    self.new_urls.append(url)
                    if self.on_log: self.on_log(f"Warning: Failed to parse {url}. Re-queuing (Attempt {self.retry_counts[url]})")
                    continue
                else:
                    failed_count += 1
                    name = "Failed"
                    err_trace = traceback.format_exc()
                    if self.on_log:
                        self.on_log(f"Error parsing {url} after {self.retry_counts[url]} attempts: {e}")
                    
                    try:
                        failed_path = os.path.join(self.saving_folder, "failed_urls.txt")
                        with open(failed_path, "a", encoding="utf-8") as ff:
                            ff.write(url + "\n")
                    except: pass
            
            if self.on_progress:
                msg = f"Updating... {processed_count + failed_count}/{processed_count + failed_count + len(self.new_urls)} | Failed: {failed_count} | Card: {name}"
                self.on_progress(msg)

            time.sleep(self.delay_ms / 1000.0)

        # Export phase
        if self.on_progress:
            self.on_progress("Formatting and appending exports...")
        self._export_data(self.language_rows, langs_to_process)
        
        return f"Finished processing session. Processed: {processed_count}. Failed: {failed_count}"

    def _process_card(self, card_data, url, sequential_id, language):
        from core.database_builder import DatabaseBuilderThread
        dummy_builder = DatabaseBuilderThread(
            links_file="", saving_folder="", languages=[language], 
            translate_missing=self.translate_missing, 
            is_all_set=self.is_all_set, delay_ms=self.delay_ms, export_formats={}
        )
        dummy_builder.base_columns = self.base_columns
        dummy_builder.LANG_MAP = self.LANG_MAP
        dummy_builder.on_log = self.on_log  # Crucial for translation logs
        dummy_builder.translation_cache = self.translation_cache
        return dummy_builder._process_card(card_data, url, sequential_id, language)

    def _export_data(self, language_rows, langs_to_process):
        """
        SMART APPEND PIPELINE:
        1. Load Existing Data Exactly (Respecting Lang Suffixes)
        2. Sort ONLY New Data Chronologically
        3. Continue IDs from Max Existing ID
        4. Append to Bottom (Preserving Batch-Insertion Order)
        """
        os.makedirs(self.saving_folder, exist_ok=True)
        sort_col = 'Set_Release_Date' if self.is_all_set else 'First TCG Set Release Date'

        # 1. DB Safety Copy (Preparation)
        updated_db_path = ""
        if (self.export_formats.get('all') or self.export_formats.get('db')) and os.path.exists(self.existing_db_path):
            base, ext = os.path.splitext(self.existing_db_path)
            updated_db_path = f"{base}_updated{ext}"
            shutil.copy2(self.existing_db_path, updated_db_path)
            if self.on_log: self.on_log(f"Created safety copy of database: {updated_db_path}")

        all_json_payload = {}
        
        for lang in langs_to_process:
            new_rows = language_rows.get(lang, [])
            if not new_rows: continue
            
            new_df = pd.DataFrame(new_rows)
            lang_suffix = f"_{lang}" if lang != "Default" else ""
            
            # --- PHASE A: LOAD EXISTING EXACTLY ---
            old_df = pd.DataFrame()
            json_orig = os.path.join(self.saving_folder, f'database{lang_suffix}.json')
            csv_orig = os.path.join(self.saving_folder, f'database{lang_suffix}.csv')

            if os.path.exists(json_orig):
                try: 
                    old_df = pd.read_json(json_orig)
                except Exception: pass
            
            if old_df.empty and os.path.exists(csv_orig):
                try: 
                    old_df = pd.read_csv(csv_orig, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
                except Exception: pass

            if old_df.empty and updated_db_path and os.path.exists(updated_db_path):
                try:
                    conn = sqlite3.connect(updated_db_path)
                    table_name = f"table_{lang}"
                    old_df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
                    conn.close()
                except Exception: pass

            # --- PHASE B: THE MASTER TEMP-MERGE ALGORITHM ---
            if not new_df.empty:
                # 0. STRICT SCHEMA PURGE (Kill ghosts in BOTH dataframes permanently)
                ghost_fl = ["First TCG Set Release Date", "First TCG Sets Number", "First TCG Set Name", "First TCG Set Rarity", "Last TCG Set Release Date", "Last TCG Sets Number", "Last TCG Set Name", "Last TCG Set Rarity"]
                ghost_all = ["Set_Release_Date", "Set_Number", "Set_Name", "Set_Rarity"]
                
                if self.is_all_set:
                    if not old_df.empty: old_df.drop(columns=[c for c in ghost_fl if c in old_df.columns], inplace=True)
                    new_df.drop(columns=[c for c in ghost_fl if c in new_df.columns], inplace=True)
                else:
                    if not old_df.empty: old_df.drop(columns=[c for c in ghost_all if c in old_df.columns], inplace=True)
                    new_df.drop(columns=[c for c in ghost_all if c in new_df.columns], inplace=True)

                # 1. ISOLATE NEW CARDS (Titanium Deduplication)
                if not old_df.empty and 'url' in old_df.columns:
                    old_urls = set(old_df['url'].astype(str).str.strip().str.lower())
                    new_df['clean_url'] = new_df['url'].astype(str).str.strip().str.lower()
                    new_df = new_df[~new_df['clean_url'].isin(old_urls)].copy()
                    new_df.drop(columns=['clean_url'], inplace=True)

                if not new_df.empty:
                    new_df.drop_duplicates(inplace=True)

                    # 2. THE PERFECT SORT (User's Algorithm)
                    s_col = "Set_Release_Date" if self.is_all_set else "First TCG Set Release Date"
                    if s_col in new_df.columns:
                        new_df['tmp_date'] = pd.to_datetime(new_df[s_col], errors='coerce').fillna(pd.Timestamp("2099-01-01"))
                        # Find the absolute earliest date for the entire card to group all its sets
                        new_df['base_date'] = new_df.groupby('Card_Name')['tmp_date'].transform('min')
                        # Sort by Earliest Card Date -> Alphabetical Name -> Specific Set Date
                        new_df = new_df.sort_values(by=['base_date', 'Card_Name', 'tmp_date']).drop(columns=['tmp_date', 'base_date'])

                    # 3. ID CONTINUATION (1 ID per Card/URL)
                    start_id = int(old_df['id'].max()) if not old_df.empty and 'id' in old_df.columns else 0
                    # drop_duplicates(keep='first') preserves the perfect sorting order we just created
                    unique_urls = new_df['url'].drop_duplicates(keep='first')
                    url_to_id = {url: start_id + i + 1 for i, url in enumerate(unique_urls)}
                    new_df['id'] = new_df['url'].map(url_to_id)

                    # 4. FINAL ALIGNMENT & MERGE
                    if not old_df.empty:
                        for c in old_df.columns:
                            if c not in new_df.columns: new_df[c] = None
                        new_df = new_df[old_df.columns] # Enforce exact column match
                        
                    combined_df = pd.concat([old_df, new_df], ignore_index=True)
                else:
                    combined_df = old_df
            else:
                combined_df = old_df

            # --- PHASE C: SAVE UPDATED ---
            # 1. CSV
            if self.export_formats.get('all') or self.export_formats.get('csv'):
                csv_upd = os.path.join(self.saving_folder, f'database{lang_suffix}_updated.csv')
                combined_df.to_csv(csv_upd, index=False, encoding='utf-8-sig')
                if self.on_log: self.on_log(f"Saved updated CSV: {csv_upd}")

            # 2. DB Table (Replace with combined)
            if updated_db_path:
                try:
                    conn = sqlite3.connect(updated_db_path)
                    combined_df.to_sql(f"table_{lang}", conn, if_exists='replace', index=False)
                    conn.close()
                except Exception as e:
                    if self.on_log: self.on_log(f"Error updating DB table for {lang}: {e}")

            # 3. JSON Accumulation
            all_json_payload[lang] = combined_df.to_dict(orient='records')

        # FINAL PHASE: JSON DUMP
        if self.export_formats.get('all') or self.export_formats.get('json'):
            json_upd = os.path.join(self.saving_folder, 'database_updated.json')
            with open(json_upd, 'w', encoding='utf-8') as f:
                json.dump(all_json_payload, f, indent=4, ensure_ascii=False)
            if self.on_log: self.on_log(f"Saved updated JSON: {json_upd}")

            if self.export_formats.get('separate_json'):
                for lang, records in all_json_payload.items():
                    lang_suffix = f"_{lang}" if lang != "Default" else ""
                    sep_upd = os.path.join(self.saving_folder, f'database{lang_suffix}_updated.json')
                    with open(sep_upd, 'w', encoding='utf-8') as f:
                        json.dump(records, f, indent=4, ensure_ascii=False)
                    if self.on_log: self.on_log(f"Saved updated Separate JSON: {sep_upd}")
