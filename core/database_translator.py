import os
import time
import json
import sqlite3
import pandas as pd
import traceback
from deep_translator import GoogleTranslator
from core.background_worker import WorkerThread

class DatabaseTranslatorThread(WorkerThread):
    def __init__(self, input_file, target_langs, delay_ms, separate_files=True, on_log=None, on_progress=None, on_complete=None, on_error=None, initial_results=None):
        super().__init__(self._run_task, on_progress, on_complete, on_error)
        self.input_file = input_file
        self.target_langs = target_langs  # Dict mapping Display Name -> ISO code
        self.delay_ms = delay_ms
        self.separate_files = separate_files
        self.on_log = on_log
        self.saving_folder = os.path.dirname(input_file)
        self.translation_cache = {} # (original_text, target_code) -> translated_text
        self.english_data = [] # The full queue
        self.results = initial_results if initial_results else {lang: [] for lang in target_langs.keys()}

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

    def _run_task(self, worker_instance):
        if self.on_log: self.on_log(f"Starting bulk translation for {self.input_file}...")
        
        # 1. Load English Data
        self.english_data = self._load_english_data()
        if not self.english_data:
            raise Exception("No English data found in the selected file.")
        
        # SLICE queue if some are already translated
        already_done = len(next(iter(self.results.values()))) if self.results else 0
        if already_done > 0:
            if self.on_log: self.on_log(f"Resuming translation session. Skipping {already_done} cards.")
            remaining_data = self.english_data[already_done:]
        else:
            remaining_data = self.english_data

        total_cards = len(self.english_data)
        if self.on_log: self.on_log(f"Found {total_cards} cards total ({len(remaining_data)} remaining).")
        
        for i, card in enumerate(remaining_data):
            # global count for progress
            current_idx = already_done + i
            if worker_instance.is_stopped():
                return "Stopped by user"
            worker_instance.check_pause()
            
            card_name_en = card.get('Card_Name', 'Unknown')
            
            for lang_name, lang_code in self.target_langs.items():
                if self.on_progress: 
                    self.on_progress(f"Translating: '{card_name_en}' to {lang_name} ({current_idx+1}/{total_cards})")
                
                translated_card = card.copy()
                
                # 1. Translate Name
                translated_name = self._translate_with_retry(card_name_en, lang_code, f"{card_name_en} (Name)")
                translated_card['Card_Name'] = translated_name
                if 'name(MSE)' in translated_card:
                    translated_card['name(MSE)'] = translated_name
                
                # 2. Translate Lore
                if card.get('Rule_Text'):
                    translated_lore = self._translate_with_retry(card['Rule_Text'], lang_code, f"{card_name_en} (Lore)")
                    translated_card['Rule_Text'] = translated_lore
                    if 'rule_text(MSE)' in translated_card:
                         translated_card['rule_text(MSE)'] = translated_lore

                # 3. Translate Pendulum
                if card.get('Pendulum _Text'):
                    translated_pen = self._translate_with_retry(card['Pendulum _Text'], lang_code, f"{card_name_en} (Pendulum)")
                    translated_card['Pendulum _Text'] = translated_pen
                    if 'pendulum_text(MSE)' in translated_card:
                        translated_card['pendulum_text(MSE)'] = translated_pen
                
                self.results[lang_name].append(translated_card)

        # 2. Export Results
        self._export_translated_data(self.results)
        return "Translation task completed successfully."

    def _load_english_data(self):
        ext = os.path.splitext(self.input_file)[1].lower()
        if ext == '.db':
            conn = sqlite3.connect(self.input_file)
            try:
                df = pd.read_sql("SELECT * FROM table_Default", conn)
                return df.to_dict(orient='records')
            except:
                try:
                    df = pd.read_sql("SELECT * FROM table_English", conn)
                    return df.to_dict(orient='records')
                except:
                    return None
            finally:
                conn.close()
        elif ext == '.csv':
            df = pd.read_csv(self.input_file)
            return df.to_dict(orient='records')
        elif ext == '.json':
            with open(self.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    if 'English' in data: return data['English']
                    if 'Default' in data: return data['Default']
                elif isinstance(data, list):
                    return data
        return None

    def _export_translated_data(self, results):
        ext = os.path.splitext(self.input_file)[1].lower()
        
        # 1. Handle DB separately (always adds tables)
        if ext == '.db':
            conn = sqlite3.connect(self.input_file)
            for lang_name, records in results.items():
                df = pd.DataFrame(records)
                table_name = f"table_{lang_name}"
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                if self.on_log: self.on_log(f"Added table {table_name} to {self.input_file}")
            conn.close()
            return

        # 2. Handle CSV/JSON (Single vs Separate)
        if self.separate_files:
            for lang_name, records in results.items():
                safe_lang = lang_name.replace(" ", "_").lower()
                out_name = f"translated_{safe_lang}_database"
                if ext == '.csv':
                    out_path = os.path.join(self.saving_folder, f"{out_name}.csv")
                    pd.DataFrame(records).to_csv(out_path, index=False, encoding='utf-8-sig')
                else:
                    out_path = os.path.join(self.saving_folder, f"{out_name}.json")
                    with open(out_path, 'w', encoding='utf-8') as f:
                        json.dump(records, f, indent=4, ensure_ascii=False)
                if self.on_log: self.on_log(f"Saved {out_path}")
        else:
            # Single combined file
            if ext == '.csv':
                # For CSV, combine all results? Actually CSV is usually 1 language per file. 
                # If they want combined CSV, we can stack them, but separate is standard for CSV.
                # If they select 'Combined' for CSV, we'll just name it 'translated_combined_database.csv'
                combined_df = pd.concat([pd.DataFrame(recs) for recs in results.values()])
                out_path = os.path.join(self.saving_folder, "translated_combined_database.csv")
                combined_df.to_csv(out_path, index=False, encoding='utf-8-sig')
                if self.on_log: self.on_log(f"Saved {out_path}")
            else:
                out_path = os.path.join(self.saving_folder, "translated_combined_database.json")
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=4, ensure_ascii=False)
                if self.on_log: self.on_log(f"Saved {out_path}")
