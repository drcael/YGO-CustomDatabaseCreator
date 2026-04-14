import os
import sqlite3
import pandas as pd
import json
import re
from core.background_worker import WorkerThread

class DatabaseExtractorThread(WorkerThread):
    def __init__(self, source_path, filters, export_formats, output_name, 
                 languages, process_mode, mse_strict, include_sets, 
                 on_log=None, on_progress=None, on_complete=None, on_error=None):
        super().__init__(self._run_task, on_progress, on_complete, on_error)
        self.source_path = source_path
        self.filters = filters
        self.export_formats = export_formats
        self.output_name = output_name
        self.languages = languages # list: ['English', 'French'...]
        self.process_mode = process_mode # "Filter & Extract" or "Convert Format Only"
        self.mse_strict = mse_strict
        self.include_sets = include_sets
        self.on_log = on_log

    def _log(self, msg):
        if self.on_log:
            self.on_log(f"[Extractor] {msg}")

    def _run_task(self, worker_instance):
        self._log(f"Starting Database Extraction: {os.path.basename(self.source_path)}")
        
        if not self.languages:
            return "Error: No languages selected."

        # Load all requested DataFrames
        lang_dfs = {}
        for lang in self.languages:
            if worker_instance.is_stopped(): return "Stopped"
            
            self._log(f"Loading data for language: {lang}")
            df = self._load_data_for_lang(lang)
            if df is not None:
                # Apply Filters if not in conversion mode
                if self.process_mode == "Filter & Extract":
                    df = self._apply_filters(df, worker_instance)
                
                # Apply MSE Strict Logic
                if self.mse_strict:
                    df = self._apply_mse_strict(df)
                
                if df is not None and not df.empty:
                    lang_dfs[lang] = df
                else:
                    self._log(f"No results found for {lang} after filtering.")

        if not lang_dfs:
            return "Extraction complete (0 matches found across all languages)."

        # 3. Export all DFs
        self._export_data_multi(lang_dfs, worker_instance)
        
        return f"Success! Extracted data for {len(lang_dfs)} languages."

    def _load_data_for_lang(self, lang):
        ext = os.path.splitext(self.source_path)[1].lower()
        try:
            if ext == '.csv':
                # CSV is usually single language, but we'll check if Card_Name exists
                df = pd.read_csv(self.source_path)
                return df if 'Card_Name' in df.columns else None
            elif ext == '.json':
                with open(self.source_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    if lang in data: return pd.DataFrame(data[lang])
                    # Fallback to English/Default if requested lang not found
                    fallback = 'English' if 'English' in data else ('Default' if 'Default' in data else list(data.keys())[0])
                    self._log(f"Warning: {lang} not in JSON. Falling back to {fallback}")
                    return pd.DataFrame(data[fallback])
                return pd.DataFrame(data)
            elif ext == '.db':
                conn = sqlite3.connect(self.source_path)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [t[0] for t in cursor.fetchall()]
                
                # Routing priority
                target = f"table_{lang}"
                if target not in tables:
                    if lang == "English" and "table_Default" in tables: target = "table_Default"
                    else:
                        self._log(f"Warning: {target} not found in DB. Skipping {lang}.")
                        conn.close()
                        return None
                
                df = pd.read_sql_query(f"SELECT * FROM {target}", conn)
                conn.close()
                return df
        except Exception as e:
            self._log(f"Error loading {lang}: {e}")
            return None

    def _apply_filters(self, df, worker):
        # reuse logic from 6.1
        ctype = self.filters.get('Card_Type', 'All')
        if ctype != 'All':
            df = df[df['Card_Type'].astype(str).str.contains(ctype, case=False, na=False)]

        mtype = self.filters.get('Monster_Type', 'All')
        if mtype != 'All':
            df = df[df['Monster_Type'].astype(str).str.contains(mtype, case=False, na=False)]

        attr = self.filters.get('Monster_Attribute', 'All')
        if attr != 'All':
            df = df[df['Monster_Attribute'].astype(str).str.contains(attr, case=False, na=False)]

        lvl = self.filters.get('Level', 'All')
        if lvl != 'All':
            if lvl == '13+':
                df = df[pd.to_numeric(df['Level'], errors='coerce') >= 13]
            else:
                target_lvl = str(lvl)
                mask = (df['Level'].astype(str) == target_lvl) | (df.get('Rank', pd.Series()).astype(str) == target_lvl) | (df.get('Link_Value', pd.Series()).astype(str) == target_lvl)
                df = df[mask]

        set_query = self.filters.get('Set', '').strip()
        if set_query:
            set_cols = [c for c in df.columns if 'Set' in c]
            mask = pd.Series([False] * len(df), index=df.index)
            for col in set_cols: mask |= df[col].astype(str).str.contains(set_query, case=False, na=False)
            df = df[mask]

        arch = self.filters.get('Archetype', '').strip()
        if arch:
            df = df[df['Archetypes'].astype(str).str.contains(arch, case=False, na=False)]

        return df

    def _apply_mse_strict(self, df):
        self._log("Applying MSE Strict cleanup...")
        
        # 1. Identify columns containing 'MSE'
        mse_cols = [c for c in df.columns if 'MSE' in c]
        
        keep_cols = mse_cols.copy()
        
        # 2. Set Numbers retention
        set_num_col = None
        if self.include_sets:
            # Look for common set number columns
            candidates = ['Set_Number', 'First TCG Sets Number', 'Last TCG Sets Number']
            for cand in candidates:
                if cand in df.columns:
                    set_num_col = cand
                    keep_cols.append(cand)
                    break
                    
        # Filter DF
        final_df = df[keep_cols].copy()
        
        # Rename set number column
        if set_num_col:
            final_df.rename(columns={set_num_col: 'set_numbers'}, inplace=True)
            
        # 3. Regex cleanup: Remove parentheses and content, then trim, AND Deduplicate
        new_cols = []
        for col in final_df.columns:
            new_name = re.sub(r'\(.*?\)', '', col).strip()
            
            # Prevent duplicate column names to avoid JSON export crashes
            base_name = new_name
            counter = 1
            while new_name in new_cols:
                new_name = f"{base_name}_{counter}"
                counter += 1
            new_cols.append(new_name)
            
        final_df.columns = new_cols
        
        return final_df

    def _export_data_multi(self, lang_dfs, worker):
        base_dir = os.path.dirname(self.source_path)
        
        for lang, df in lang_dfs.items():
            lang_suffix = f"_{lang}"
            out_base = os.path.join(base_dir, f"{self.output_name}{lang_suffix}")
            
            if self.export_formats.get('csv'):
                df.to_csv(f"{out_base}.csv", index=False, encoding='utf-8-sig')
                
            if self.export_formats.get('json'):
                df.to_json(f"{out_base}.json", orient='records', indent=4, force_ascii=False)
                
            if self.export_formats.get('db'):
                conn = sqlite3.connect(f"{os.path.join(base_dir, self.output_name)}.db")
                table_name = f"table_{lang}"
                df.to_sql(table_name, conn, if_exists='append' if lang != list(lang_dfs.keys())[0] else 'replace', index=False)
                conn.close()
                
        self._log(f"Exported files to {base_dir}")
