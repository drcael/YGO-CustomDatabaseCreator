import os
import time
import pandas as pd
from datetime import datetime
from core.background_worker import WorkerThread
from core.card_parser import YugipediaParser
import traceback
import sqlite3
import json
import re
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

class DatabaseBuilderThread(WorkerThread):
    # Map UI language names to Yugipedia's internal language names
    LANG_MAP = {
        'S. Chinese': 'Simplified Chinese',
        'T. Chinese': 'Traditional Chinese',
    }
    
    def __init__(self, links_file, saving_folder, languages, translate_missing, is_all_set, delay_ms, export_formats, on_log=None, on_progress=None, on_complete=None, on_error=None, initial_rows=None):
        super().__init__(self._run_task, on_progress, on_complete, on_error)
        self.links_file = links_file
        self.saving_folder = saving_folder
        self.languages = languages
        self.translate_missing = translate_missing
        self.is_all_set = is_all_set
        self.delay_ms = delay_ms
        self.export_formats = export_formats
        self.on_log = on_log
        self.translation_cache = {}  # (original_text, target_code) -> translated_text
        
        self.initial_rows = initial_rows
        self.language_rows = {lang: [] for lang in (languages if languages else ["Default"])}
        self.urls = []
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

    def _run_task(self, worker_instance):
        if self.on_log:
            self.on_log("Starting database generation process...")
            
        with open(self.links_file, 'r', encoding='utf-8') as f:
            self.urls = [line.strip() for line in f.readlines() if line.strip()]
        
        if self.initial_rows:
            for lang, rows in self.initial_rows.items():
                if lang in self.language_rows:
                    self.language_rows[lang].extend(rows)
            if self.on_log: self.on_log(f"Resumed session with {len(next(iter(self.initial_rows.values()))) if self.initial_rows else 0} pre-processed cards.")

        processed_count = 0
        failed_count = 0
        sequential_id = 1 + (len(next(iter(self.language_rows.values()))) if any(self.language_rows.values()) else 0)
        
        # We process separately for each selected language
        # If no languages selected, fallback to a "Default" catch-all
        langs_to_process = self.languages if self.languages else ["Default"]

        while self.urls:
            if worker_instance.is_stopped():
                return "Stopped by user"
            worker_instance.check_pause()

            url = self.urls.pop(0)
            # Process Card Data
            try:
                card_data = self.parser.parse_card(url)
                if card_data:
                    name = card_data.get('Name', 'Unknown')
                    
                    # Generate rows for each selected language
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
                    self.urls.append(url)
                    if self.on_log: self.on_log(f"Warning: Failed to parse {url}. Re-queuing (Attempt {self.retry_counts[url]})")
                    continue
                else:
                    failed_count += 1
                    name = "Failed"
                    # Log traceback precisely
                    err_trace = traceback.format_exc()
                    if self.on_log:
                        self.on_log(f"Error parsing {url} after {self.retry_counts[url]} attempts: {e}")
                    
                    # Append to failed_urls.txt
                    try:
                        os.makedirs(self.saving_folder, exist_ok=True)
                        failed_path = os.path.join(self.saving_folder, "failed_urls.txt")
                        with open(failed_path, "a", encoding="utf-8") as ff:
                            ff.write(url + "\n")
                    except:
                        pass
            
            # Formatted Progress UI
            if self.on_progress:
                msg = f"Processing... {processed_count + failed_count}/{processed_count + failed_count + len(self.urls)} | Failed: {failed_count} | Card: {name}"
                self.on_progress(msg)

            time.sleep(self.delay_ms / 1000.0)

        # Export phase
        if self.on_progress:
            self.on_progress(f"Exporting database files... Please wait.", "blue")
            
        self._export_data(self.language_rows, langs_to_process)
            
        if self.on_log:
            self.on_log("Database generation completely finished!")
            
        return "Completed"

    def _process_card(self, data, url, seq_id, target_lang):
        # 1. Base Mapping
        row = {col: "" for col in self.base_columns}
        row['id'] = str(seq_id)
        row['url'] = url
        
        # --- FIX: HANDLE DICTIONARY LORE FROM PARSER ---
        raw_lore = data.get('Lore', '')
        raw_pend = data.get('Pendulum_Lore', '')
        if isinstance(raw_lore, dict):
            raw_pend = raw_lore.get('Pendulum Effect', raw_pend)
            raw_lore = raw_lore.get('Monster Effect', raw_lore.get('Flavor Text', ''))
            
        # Override with Localized Data
        localized = data.get('Localized_Data', {})
        card_name = data.get('Name', '')
        localized_lore = None
        localized_pend_lore = None
        
        lookup_lang = self.LANG_MAP.get(target_lang, target_lang)
        
        if lookup_lang in localized:
            sub = localized[lookup_lang]
            if isinstance(sub, dict):
                if 'Name' in sub: card_name = sub['Name']
                
                loc_lore = sub.get('Lore')
                if isinstance(loc_lore, dict):
                    localized_pend_lore = loc_lore.get('Pendulum Effect', '')
                    localized_lore = loc_lore.get('Monster Effect', loc_lore.get('Flavor Text', ''))
                else:
                    if 'Lore' in sub: localized_lore = sub['Lore']
                    if 'Pendulum_Lore' in sub: localized_pend_lore = sub['Pendulum_Lore']
                    
        # STRICT TRANSLATION ENFORCEMENT (ONLY FOR MISSING DATA)
        if target_lang not in ["English", "Default"] and self.translate_missing:
            iso_code = lang_code_map.get(target_lang)
            if iso_code:
                has_loc_name = isinstance(localized.get(lookup_lang), dict) and 'Name' in localized[lookup_lang]
                if not has_loc_name:
                    card_name = self._translate_with_retry(data.get('Name', card_name), iso_code, f"{card_name} (Name)")
                
                if not localized_lore and raw_lore:
                    localized_lore = self._translate_with_retry(raw_lore, iso_code, f"{card_name} (Lore)")
                    
                if not localized_pend_lore and raw_pend:
                    localized_pend_lore = self._translate_with_retry(raw_pend, iso_code, f"{card_name} (Pendulum)")

        row['Card_Name'] = card_name
        row['name(MSE)'] = card_name
        
        # Final Rule Text Resolution
        final_lore = localized_lore if localized_lore else raw_lore
        row['Rule_Text'] = final_lore
        row['rule_text(MSE)'] = final_lore 

        # Final Pendulum Resolution
        final_pend = localized_pend_lore if localized_pend_lore else raw_pend
        row['Pendulum _Text'] = final_pend
        row['pendulum_text(MSE)'] = final_pend
        
        row['Password'] = data.get('Passcode', '')
        row['gamecode(MSE)'] = data.get('Passcode', '')
        row['img'] = ""  # Left empty deliberately
        
        # Status, Effects, Archetypes
        row['Status'] = data.get('Status', '')
        row['Genesys_Point/Status'] = data.get('Genesys_Status', '')
        row['Effect _Types'] = data.get('Effect_Types', '')
        
        archetypes = data.get('Archetypes', [])
        row['Archetypes'] = ', '.join(archetypes) if isinstance(archetypes, list) else str(archetypes)
        
        # Asian Name sub-row variations (Name_2 = Romaji, Name_3 = translated)
        if lookup_lang in localized and isinstance(localized.get(lookup_lang), dict):
            sub = localized[lookup_lang]
            if 'Name_2' in sub:
                row['Card_Name_2'] = sub['Name_2']
            if 'Name_3' in sub:
                row['Card_Name_3'] = sub['Name_3']
        
        c_type = data.get('Card Type', '')
        row['Card_Type'] = c_type
        
        # Determine specific card types for MSE
        is_spell = "Spell" in c_type
        is_trap = "Trap" in c_type
        is_monster = "Monster" in c_type
        
        types_str = data.get('Types', '')
        # Check and Inject Normal status
        is_normal_monster = False
        if is_monster and types_str:
            if 'Effect' not in types_str and 'Normal' not in types_str and 'Token' not in types_str:
                types_str += ' / Normal'
            
            if 'Effect' not in types_str:
                is_normal_monster = True
                
        # Assign basic MSE parts
        if is_monster:
            row['attribute(MSE)'] = str(data.get('Attribute', '')).lower()
            row['Monster_Attribute'] = data.get('Attribute', '')
            row['Monster_Type'] = types_str
            
            # Level/Rank mapping
            level_val = data.get('Level', '')
            rank_val = data.get('Rank', '')
            lv = level_val if level_val else rank_val
            row['Level'] = level_val
            row['Rank'] = rank_val
            if lv and lv.isdigit():
                row['level(MSE)'] = "<sym-auto>*</sym-auto>" * int(lv)
                
            # Pendulum
            ps = data.get('Pendulum Scale', '')
            if ps:
                row['Pendulum_Scale'] = ps
                row['blue_scale(MSE)'] = ps
                row['red_scale(MSE)'] = ps
                
            # Links
            lr = data.get('Link Rating', '')
            if lr:
                row['Link_Value'] = lr
            arrows = data.get('Link Arrows', [])
            if arrows:
                row['Link_Position'] = ", ".join(arrows)
                # Strict Link Arrow mapping to "on"
                arrow_col_map = {
                    'Top-Left': 'linkul(MSE_Link_Top-Left) ',
                    'Top-Center': 'linku(MSE_Link_Top-Center)',
                    'Top-Right': 'linkur(MSE_Link_Top-Right)',
                    'Middle-Left': 'linkl(MSE_Link_Middle-Left)',
                    'Bottom-Left': 'linkdl(MSE_Link_Bottom-Left)',
                    'Middle-Right': 'linkr(MSE_Link_Middle-Right)',
                    'Bottom-Center': 'linkd(MSE_Link_Bottom-Center)',
                    'Bottom-Right': 'linkdr(MSE_Link_Bottom-Right)'
                }
                for position in arrows:
                    col = arrow_col_map.get(position)
                    if col and col in self.base_columns:
                        row[col] = 'on'
                    if position == 'Bottom-Right':
                        row['linkdr (MSE-Link/Down-Right)'] = 'on'
            
            # Types breakdown
            type_parts = [t.strip() for t in types_str.split('/')] if types_str else []
            for j in range(5):
                if j < len(type_parts):
                    if j == 0:
                        row[f'Type_{j+1}(MSE)'] = f'<word-list-monster>{type_parts[j]}</word-list-monster>'
                    else:
                        row[f'Type_{j+1}(MSE)'] = f'<word-list-card>{type_parts[j]}</word-list-card>'
                else:
                    row[f'Type_{j+1}(MSE)'] = '<word-list-card></word-list-card>'
                    
            # Monster Type Construction
            if type_parts:
                m_type = '<prefix>[</prefix>'
                for j, tp in enumerate(type_parts):
                    if j == 0:
                        m_type += f'<word-list-monster>{tp}</word-list-monster>'
                    else:
                        m_type += f'<sep><color:black>/</color:black></sep><word-list-card>{tp}</word-list-card>'
                
                softs = 5 - len(type_parts)
                m_type += '<sep-soft></sep-soft><word-list-card></word-list-card>' * softs
                m_type += '<sep>]<soft></soft></sep><word-list-card></word-list-card>'
                row['monster_type(MSE)'] = m_type

            # Determine MSE specific Card Type word (e.g. effect monster, link monster)
            card_types = [t.lower() for t in type_parts]
            cname = "normal monster"
            if "effect" in card_types: cname = "effect monster"
            if "fusion" in card_types: cname = "fusion monster"
            if "synchro" in card_types: cname = "synchro monster"
            if "xyz" in card_types: cname = "xyz monster"
            if "link" in card_types: cname = "link monster"
            if "ritual" in card_types: cname = "ritual monster"
            
            if "pendulum" in card_types:
                if cname == "normal monster": cname = "pendulum normal monster" # fallback if needed
                elif cname == "effect monster": cname = "pendulum effect monster"
                else: cname = f"pendulum {cname}" # pendulum fusion monster etc.
                
            row['card_type(MSE)'] = cname
            
            # ATK/DEF — Link Monsters put LINK rating into defense(MSE)
            atk_val = str(data.get('ATK', ''))
            def_val = str(data.get('DEF', ''))
            link_val = str(data.get('LINK', ''))
            
            row['ATK'] = atk_val
            row['DEF'] = def_val
            row['attack(MSE)'] = atk_val
            
            # If it's a Link Monster, MSE expects the Link Rating in the defense column
            if link_val:
                row['defense(MSE)'] = link_val
                print(f"[{data.get('Name', 'Unknown')}] Link Monster -> attack(MSE): {atk_val}, defense(MSE): {link_val}")
            else:
                row['defense(MSE)'] = def_val

        elif is_spell or is_trap:
            row['attribute(MSE)'] = 'spell' if is_spell else 'trap'
            row['Property'] = data.get('Property', 'Normal')
            prop = row['Property']
            
            sym = ""
            if "Continuous" in prop: sym = "<sym><sym-auto>%</sym-auto></sym>"
            elif "Counter" in prop: sym = "<sym><sym-auto>!</sym-auto></sym>"
            elif "Equip" in prop: sym = "<sym><sym-auto>+</sym-auto></sym>"
            elif "Field" in prop: sym = "<sym><sym-auto>&</sym-auto></sym>"
            elif "Quick-Play" in prop: sym = "<sym><sym-auto>$</sym-auto></sym>"
            elif "Ritual" in prop: sym = "<sym><sym-auto>#</sym-auto></sym>"
            
            ctype = "Spell Card" if is_spell else "Trap Card"
            row['level(MSE)'] = f"<b>[{ctype}{sym}]</b>"
            row['card_type(MSE)'] = f"{ctype.lower()}"
        
        # Handle Italics for Normal Monsters in rule_text(MSE)
        if is_normal_monster and row['rule_text(MSE)']:
            row['rule_text(MSE)'] = f"<i>{row['rule_text(MSE)']}</i>"
            
        # 2. Sets logic & Region filtering
        sets = data.get('Sets', [])
        rows_to_return = []
        
        # Filter Sets dynamically by Target Language if not "Default"
        if target_lang != "Default" and target_lang != "All":
            # Resolve UI language to match Yugipedia's Region/Language labels
            filter_lang = self.LANG_MAP.get(target_lang, target_lang)
            filtered_sets = []
            for s in sets:
                reg_lang = s.get('Region/Language', '')
                if filter_lang.lower() in reg_lang.lower():
                    filtered_sets.append(s)
            sets = filtered_sets
            
        # If no sets remained, return an empty row variant just to track the card exists
        if len(sets) == 0:
            if self.is_all_set:
                row['Set_Release_Date'] = ''
                row['Set_Number'] = ''
                row['Set_Name'] = ''
                row['Set_Rarity'] = ''
            else:
                row['Last TCG Set Release Date'] = ''
                row['Last TCG Sets Number'] = ''
                row['Last TCG Set Name'] = ''
                row['Last TCG Set Rarity'] = ''
                row['First TCG Set Release Date'] = ''
                row['First TCG Sets Number'] = ''
                row['First TCG Set Name'] = ''
                row['First TCG Set Rarity'] = ''
            rows_to_return.append(row)
            return rows_to_return

        if self.is_all_set:
            for s in sets:
                new_row = row.copy()
                new_row['Set_Release_Date'] = s.get('Release', '')
                new_row['Set_Number'] = s.get('Number', '')
                new_row['Set_Name'] = s.get('Set', '')
                new_row['Set_Rarity'] = s.get('Rarity', '')
                rows_to_return.append(new_row)
        else:
            # First and Last Set Logic
            first_set = {}
            last_set = {}
            
            # Sort by date
            valid_sets = []
            for s in sets:
                rel = str(s.get('Release', ''))
                if rel:
                    try:
                        date_obj = datetime.strptime(rel, '%Y-%m-%d')
                        valid_sets.append((date_obj, s))
                    except Exception:
                        valid_sets.append((datetime.min, s)) 
            
            if valid_sets:
                valid_sets.sort(key=lambda x: x[0])
                first_set = valid_sets[0][1]
                last_set = valid_sets[-1][1]

            row['Last TCG Set Release Date'] = last_set.get('Release', '')
            row['Last TCG Sets Number'] = last_set.get('Number', '')
            row['Last TCG Set Name'] = last_set.get('Set', '')
            row['Last TCG Set Rarity'] = last_set.get('Rarity', '')

            row['First TCG Set Release Date'] = first_set.get('Release', '')
            row['First TCG Sets Number'] = first_set.get('Number', '')
            row['First TCG Set Name'] = first_set.get('Set', '')
            row['First TCG Set Rarity'] = first_set.get('Rarity', '')
            rows_to_return.append(row)

        return rows_to_return

    def _export_data(self, language_rows, langs_to_process):
        os.makedirs(self.saving_folder, exist_ok=True)
        
        # Prepare DataFrames internally
        dfs = {}
        for language in langs_to_process:
            rows = language_rows.get(language, [])
            if not rows:
                continue
                
            df = pd.DataFrame(rows)
            dfs[language] = df

        if not dfs:
            if self.on_log: self.on_log("No data available to export.")
            return

        # MASTER SORTING PATCH
        # 1. Identify Master Language (English base or Japanese)
        master_lang = "English" if "English" in dfs else ("Japanese" if "Japanese" in dfs else list(dfs.keys())[0])
        master_df = dfs[master_lang]
        
        date_col = 'Set_Release_Date' if self.is_all_set else 'First TCG Set Release Date'
        
        if date_col in master_df.columns:
            # Temporarily fill NaT with 2099-01-01 for reliable sorting to the bottom
            temp_dates = pd.to_datetime(master_df[date_col], errors='coerce').fillna(pd.Timestamp("2099-01-01"))
            master_df['Base_Release_Date'] = temp_dates.groupby(master_df['Card_Name']).transform('min')
            master_df['Sort_Date'] = temp_dates
            
            master_df.sort_values(by=['Base_Release_Date', 'Card_Name', 'Sort_Date'], ascending=[True, True, True], inplace=True)
            
            # FOOLPROOF ID GENERATION
            unique_urls = master_df['url'].drop_duplicates().tolist()
            url_to_id = {url: i + 1 for i, url in enumerate(unique_urls)}
            
            master_df['id'] = master_df['url'].map(url_to_id).astype(int)
            
            # Create a unique row identifier for synchronization (url + date + set + rarity)
            # Use 'Subset_Index' to handle duplicates of the exact same set data
            master_df['row_sync_key'] = master_df.groupby(['url', date_col]).cumcount().astype(str)
            master_df['sync_id'] = master_df['url'] + "_" + master_df[date_col].astype(str) + "_" + master_df['row_sync_key']
            
            sync_order = {sid: i for i, sid in enumerate(master_df['sync_id'])}

            for lang, df in dfs.items():
                if lang == master_lang: continue
                
                # Map ID
                df['id'] = df['url'].map(url_to_id).astype(int)
                
                # ULTRA-FAST Sync Sort
                df.sort_values(by=['id', date_col], ascending=[True, True], inplace=True)
                
                dfs[lang] = df
                
            # Clean up master_df temp columns
            master_df.drop(columns=['Base_Release_Date', 'Sort_Date', 'row_sync_key', 'sync_id'], inplace=True)
            dfs[master_lang] = master_df

        # Final Formatting for each DF
        for language, df in dfs.items():
            # Ordering columns based on mode
            if self.is_all_set:
                final_columns = self.base_columns[:20] + ['Set_Release_Date', 'Set_Number', 'Set_Name', 'Set_Rarity'] + self.base_columns[20:]
            else:
                first_last_cols = [
                    'Last TCG Set Release Date', 'Last TCG Sets Number', 'Last TCG Set Name', 'Last TCG Set Rarity', 
                    'First TCG Set Release Date', 'First TCG Sets Number', 'First TCG Set Name', 'First TCG Set Rarity'
                ]
                final_columns = self.base_columns[:20] + first_last_cols + self.base_columns[20:]
                
            # Identify extra dynamic columns from this build
            extra_cols = []
            for col in ['Card_Name_2', 'Card_Name_3', 'Card_Name_Base', 'Card_Name_Kana', 'Card_Name_Romaji']:
                if col in df.columns:
                    extra_cols.append(col)
            
            # Insert name variations right after Card_Name (index 3) instead of appending at end
            card_name_idx = final_columns.index('Card_Name') + 1 if 'Card_Name' in final_columns else 4
            ordered_columns = [col for col in final_columns if col in df.columns]
            # Insert extra cols after Card_Name position
            for i, ec in enumerate(extra_cols):
                if ec not in ordered_columns:
                    ordered_columns.insert(card_name_idx + i, ec)
            # Add any remaining columns not yet included
            for col in df.columns:
                if col not in ordered_columns:
                    ordered_columns.append(col)
            
            df = df.reindex(columns=ordered_columns)
            dfs[language] = df

        if not dfs:
            if self.on_log: self.on_log("No data available to export.")
            return

        # 1. Output CSV files (One per language)
        if self.export_formats.get('all') or self.export_formats.get('csv'):
            for lang, df in dfs.items():
                lang_suffix = f"_{lang}" if lang and lang != "Default" else ""
                csv_path = os.path.join(self.saving_folder, f'database{lang_suffix}.csv')
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                if self.on_log: self.on_log(f"Saved {csv_path}")

        # 2. Output JSON
        if self.export_formats.get('all') or self.export_formats.get('json'):
            # Always output the combined JSON
            json_path = os.path.join(self.saving_folder, 'database.json')
            payload = {}
            for lang, df in dfs.items():
                payload[lang] = df.to_dict(orient='records')
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, indent=4, ensure_ascii=False)
            if self.on_log: self.on_log(f"Saved {json_path}")
            
            # Additionally output separate JSONs per language if requested
            if self.export_formats.get('separate_json'):
                for lang, df in dfs.items():
                    lang_suffix = f"_{lang}" if lang and lang != "Default" else ""
                    sep_json_path = os.path.join(self.saving_folder, f'database{lang_suffix}.json')
                    sep_payload = df.to_dict(orient='records')
                    with open(sep_json_path, 'w', encoding='utf-8') as f:
                        json.dump(sep_payload, f, indent=4, ensure_ascii=False)
                    if self.on_log: self.on_log(f"Saved {sep_json_path}")

        # 3. Output SQLite DB (One file, Multiple Tables)
        if self.export_formats.get('all') or self.export_formats.get('db'):
            db_path = os.path.join(self.saving_folder, 'database.db')
            # remove existing DB if exists to start fresh
            if os.path.exists(db_path):
                try: os.remove(db_path)
                except: pass
                
            conn = sqlite3.connect(db_path)
            for lang, df in dfs.items():
                table_name = f"table_{lang}" if lang and lang != "Default" else "table_Default"
                df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            if self.on_log: self.on_log(f"Saved {db_path} containing {len(dfs)} tables.")
