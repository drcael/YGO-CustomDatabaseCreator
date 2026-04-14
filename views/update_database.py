import tkinter.filedialog as filedialog
import tkinter.messagebox as messagebox
import customtkinter as ctk
import os
import json
from core.database_updater import DatabaseUpdaterThread
from core.localization import Localization

class UpdateDatabaseFrame(ctk.CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        
        self.worker = None
        self.loaded_initial_rows = None

        # 1. Header
        self.title_label = ctk.CTkLabel(self, font=ctk.CTkFont(size=20, weight="bold"))
        self.title_label.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="nw")
        self.title_label.text_key = "main_sidebar_update"

        # 2. Path Settings
        self.path_frame = ctk.CTkFrame(self)
        self.path_frame.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        self.path_frame.grid_columnconfigure(1, weight=1)

        self.links_file_entry = ctk.CTkEntry(self.path_frame, placeholder_text="New Links Txt File")
        self.links_file_entry.grid(row=0, column=0, columnspan=2, padx=(10, 0), pady=10, sticky="ew")

        self.links_file_btn = ctk.CTkButton(self.path_frame, text="Browse", width=80, command=self.browse_links_file)
        self.links_file_btn.grid(row=0, column=2, padx=(10, 10), pady=10)
        self.links_file_btn.text_key = "btn_browse"

        self.db_file_entry = ctk.CTkEntry(self.path_frame, placeholder_text="Existing database.db File")
        self.db_file_entry.grid(row=1, column=0, columnspan=2, padx=(10, 0), pady=10, sticky="ew")

        self.db_file_btn = ctk.CTkButton(self.path_frame, width=80, command=self.browse_db_file)
        self.db_file_btn.grid(row=1, column=2, padx=(10, 10), pady=10)
        self.db_file_btn.text_key = "btn_browse"

        # 3. Languages Checkboxes
        self.lang_frame = ctk.CTkFrame(self)
        self.lang_frame.grid(row=2, column=0, padx=20, pady=10, sticky="ew")
        
        self.lang_label = ctk.CTkLabel(self.lang_frame, font=ctk.CTkFont(weight="bold"))
        self.lang_label.grid(row=0, column=0, padx=10, pady=(10, 5), sticky="w")
        self.lang_label.text_key = "lbl_language"

        languages = ["All", "English", "Japanese", "French", "German", "Italian", "Portuguese", "Spanish", "Korean", "S. Chinese", "T. Chinese"]
        self.lang_vars = {}
        self.lang_cbs = {}
        row_idx, col_idx = 1, 0
        for lang in languages:
            var = ctk.BooleanVar()
            self.lang_vars[lang] = var
            cb = ctk.CTkCheckBox(self.lang_frame, text=lang, variable=var,
                                 command=lambda l=lang: self._on_lang_checkbox_changed(l))
            cb.grid(row=row_idx, column=col_idx, padx=10, pady=5, sticky="w")
            self.lang_cbs[lang] = cb
            if lang == "All": cb.text_key = "cb_all_langs"
            
            col_idx += 1
            if col_idx > 3:
                col_idx = 0
                row_idx += 1

        self.trans_var = ctk.BooleanVar()
        self.trans_checkbox = ctk.CTkCheckBox(self.lang_frame, variable=self.trans_var, command=self._on_translate_toggled)
        self.trans_checkbox.grid(row=row_idx+1, column=0, columnspan=4, padx=10, pady=(10, 10), sticky="w")
        self.trans_checkbox.text_key = "cb_translate_long"

        # 4. Configuration Row
        self.conf_frame = ctk.CTkFrame(self)
        self.conf_frame.grid(row=3, column=0, padx=20, pady=10, sticky="ew")
        
        self.set_choice_var = ctk.IntVar(value=0)
        self.rb1 = ctk.CTkRadioButton(self.conf_frame, variable=self.set_choice_var, value=0)
        self.rb1.grid(row=0, column=0, padx=10, pady=10)
        self.rb1.text_key = "rb_first_last"
        
        self.rb2 = ctk.CTkRadioButton(self.conf_frame, variable=self.set_choice_var, value=1)
        self.rb2.grid(row=0, column=1, padx=10, pady=10)
        self.rb2.text_key = "rb_all_set"

        self.delay_label = ctk.CTkLabel(self.conf_frame)
        self.delay_label.grid(row=0, column=2, padx=(20, 5), pady=10)
        self.delay_label.text_key = "lbl_delay"
        
        self.delay_entry = ctk.CTkEntry(self.conf_frame, width=80)
        self.delay_entry.insert(0, "1000")
        self.delay_entry.grid(row=0, column=3, padx=(0, 10), pady=10)

        # 5. Export frame
        self.export_frame = ctk.CTkFrame(self)
        self.export_frame.grid(row=4, column=0, padx=20, pady=10, sticky="ew")

        self.export_label = ctk.CTkLabel(self.export_frame, font=ctk.CTkFont(weight="bold"))
        self.export_label.grid(row=0, column=0, padx=10, pady=10)
        self.export_label.text_key = "lbl_export"

        self.export_all_var = ctk.BooleanVar(value=True)
        self.export_all_cb = ctk.CTkCheckBox(self.export_frame, variable=self.export_all_var, command=self._on_export_checkbox_changed)
        self.export_all_cb.grid(row=0, column=1, padx=10, pady=10)
        self.export_all_cb.text_key = "cb_export_all"

        self.export_csv_var = ctk.BooleanVar(value=False)
        self.export_csv_cb = ctk.CTkCheckBox(self.export_frame, text="CSV", variable=self.export_csv_var, command=self._on_export_checkbox_changed)
        self.export_csv_cb.grid(row=0, column=2, padx=10, pady=10)

        self.export_json_var = ctk.BooleanVar(value=False)
        self.export_json_cb = ctk.CTkCheckBox(self.export_frame, text="JSON", variable=self.export_json_var, command=self._on_export_checkbox_changed)
        self.export_json_cb.grid(row=0, column=3, padx=10, pady=10)

        self.export_db_var = ctk.BooleanVar(value=False)
        self.export_db_cb = ctk.CTkCheckBox(self.export_frame, text="DB", variable=self.export_db_var, command=self._on_export_checkbox_changed)
        self.export_db_cb.grid(row=0, column=4, padx=10, pady=10)

        self.export_sep_var = ctk.BooleanVar(value=False)
        self.export_sep_cb = ctk.CTkCheckBox(self.export_frame, variable=self.export_sep_var)
        self.export_sep_cb.grid(row=1, column=0, columnspan=3, padx=10, pady=(0, 10), sticky="w")
        self.export_sep_cb.text_key = "cb_separate_json"
        
        self._on_export_checkbox_changed()

        # 6. Buttons
        self.btn_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.btn_frame.grid(row=5, column=0, padx=20, pady=10, sticky="ew")

        self.btn_start = ctk.CTkButton(self.btn_frame, text="Start", command=self.start_process, fg_color="green", hover_color="darkgreen")
        self.btn_start.text_key = "btn_start"

        self.btn_pause = ctk.CTkButton(self.btn_frame, text="Pause", command=self.pause_process, state="disabled")
        self.btn_pause.text_key = "btn_pause"

        self.btn_stop = ctk.CTkButton(self.btn_frame, text="Stop", command=self.stop_process, fg_color="red", hover_color="darkred", state="disabled")
        self.btn_stop.text_key = "btn_stop"

        self.btn_try_failed = ctk.CTkButton(self.btn_frame, text="Try Failed", command=self.load_failed_urls, state="disabled")
        self.btn_try_failed.text_key = "btn_try_failed"

        self.btn_link_grabber = ctk.CTkButton(self.btn_frame, command=lambda: self.master.select_page("Link Grabber"))
        self.btn_link_grabber.text_key = "main_sidebar_grabber"

        self.btn_logs = ctk.CTkButton(self.btn_frame, command=lambda: self.master.select_page("Log"))
        self.btn_logs.text_key = "main_sidebar_log"

        self.btn_save_progress = ctk.CTkButton(self.btn_frame, text="Save Progress", command=self.save_progress, fg_color="#34495e", hover_color="#2c3e50")
        self.btn_save_progress.text_key = "btn_save_progress"

        self.btn_load_progress = ctk.CTkButton(self.btn_frame, text="Load Progress", command=self.load_progress, fg_color="#34495e", hover_color="#2c3e50")
        self.btn_load_progress.text_key = "btn_load_progress"

        # Apply Grid Layout
        self.btn_start.grid(row=0, column=0, padx=5, pady=5, sticky="ew")
        self.btn_pause.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        self.btn_stop.grid(row=0, column=2, padx=5, pady=5, sticky="ew")
        self.btn_try_failed.grid(row=0, column=3, padx=5, pady=5, sticky="ew")
        
        self.btn_save_progress.grid(row=1, column=0, columnspan=2, padx=5, pady=5, sticky="ew")
        self.btn_load_progress.grid(row=1, column=2, columnspan=2, padx=5, pady=5, sticky="ew")
        
        self.btn_link_grabber.grid(row=2, column=0, columnspan=2, padx=5, pady=5, sticky="ew")
        self.btn_logs.grid(row=2, column=2, columnspan=2, padx=5, pady=5, sticky="ew")

        self.btn_frame.grid_columnconfigure((0,1,2,3), weight=1)


        # 7. Status
        self.status_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.status_frame.grid(row=6, column=0, padx=20, pady=(10, 20), sticky="sew")
        self.grid_rowconfigure(6, weight=1)

        self.status_label = ctk.CTkLabel(self.status_frame, font=ctk.CTkFont(size=14, weight="bold"))
        self.status_label.pack(side="left")
        self.status_label.text_key = "status_ready"

        self.grid_columnconfigure(0, weight=1)
        self.update_localization(self.master.config.get("language", "English"))

    def update_localization(self, lang):
        Localization.refresh_widgets(self, lang)

    def _on_lang_checkbox_changed(self, changed_lang):
        if changed_lang == "All":
            if self.lang_vars["All"].get():
                for lang, var in self.lang_vars.items():
                    if lang != "All":
                        var.set(False)
                        self.lang_cbs[lang].configure(state="disabled")
            else:
                for lang in self.lang_vars:
                    if lang != "All":
                        self.lang_cbs[lang].configure(state="normal")
        else:
            any_individual = any(var.get() for lang, var in self.lang_vars.items() if lang != "All")
            if any_individual:
                self.lang_vars["All"].set(False)
                self.lang_cbs["All"].configure(state="disabled")
            else:
                self.lang_cbs["All"].configure(state="normal")

    def _on_export_checkbox_changed(self):
        all_checked = self.export_all_var.get()
        individual_cbs = [(self.export_csv_var, self.export_csv_cb), (self.export_json_var, self.export_json_cb), (self.export_db_var, self.export_db_cb)]
        if all_checked:
            for var, cb in individual_cbs:
                var.set(False)
                cb.configure(state="disabled")
            self.export_all_cb.configure(state="normal")
        else:
            any_individual = any(var.get() for var, cb in individual_cbs)
            self.export_all_cb.configure(state="disabled" if any_individual else "normal")
            for var, cb in individual_cbs: cb.configure(state="normal")

    def _on_translate_toggled(self):
        if self.trans_var.get():
            msg = Localization.get_text("msg_trans_warn", self.master.config.get("language", "English"))
            if not messagebox.askokcancel("Translation", msg):
                self.trans_var.set(False)

    def browse_db_file(self):
        filename = filedialog.askopenfilename()
        if filename:
            self.db_file_entry.delete(0, 'end')
            self.db_file_entry.insert(0, filename)
            self._detect_and_lock_schema(filename)

    def _detect_and_lock_schema(self, file_path):
        try:
            content = ""
            if file_path.endswith('.json'):
                import json
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Handle both combined and separate JSON formats
                    first_record = None
                    if isinstance(data, dict):
                        for lang in data:
                            if data[lang]:
                                first_record = data[lang][0]
                                break
                    elif isinstance(data, list) and data:
                        first_record = data[0]
                    
                    if first_record:
                        content = str(first_record.keys())
            elif file_path.endswith('.csv'):
                with open(file_path, 'r', encoding='utf-8-sig') as f:
                    content = f.readline()
            elif file_path.endswith('.db'):
                import sqlite3
                conn = sqlite3.connect(file_path)
                cursor = conn.cursor()
                # Grab the first valid table to check columns
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'table_%' LIMIT 1")
                table_match = cursor.fetchone()
                if table_match:
                    cursor.execute(f"PRAGMA table_info('{table_match[0]}')")
                    columns = [col[1] for col in cursor.fetchall()]
                    content = str(columns)
                conn.close()
            
            # Apply Locks based on actual UI configuration
            if "Set_Release_Date" in content:
                self.set_choice_var.set(1) # All Sets (rb2)
                try:
                    self.rb1.configure(state="disabled")
                    self.rb2.configure(state="disabled")
                except: pass
                self.update_status("Schema Locked: All Sets", "blue")
            elif "First TCG Set Release Date" in content:
                self.set_choice_var.set(0) # First/Last (rb1)
                try:
                    self.rb1.configure(state="disabled")
                    self.rb2.configure(state="disabled")
                except: pass
                self.update_status("Schema Locked: First/Last Sets", "blue")
        except Exception as e:
            print(f"Auto-detect failed: {e}")

    def browse_links_file(self):
        filename = filedialog.askopenfilename()
        if filename:
            self.links_file_entry.delete(0, 'end')
            self.links_file_entry.insert(0, filename)

    def start_process(self):
        links_f = self.links_file_entry.get()
        db_f = self.db_file_entry.get()
        if not links_f or not db_f: return

        # 1. Extraction: Languages
        langs = [l for l, v in self.lang_vars.items() if v.get() and l != "All"]
        if self.lang_vars["All"].get() or not langs:
            langs = ["English", "Japanese", "French", "German", "Italian", "Portuguese", "Spanish", "Korean", "S. Chinese", "T. Chinese"]

        # 2. Extraction: Formatting
        try:
            delay_ms = int(self.delay_entry.get())
        except:
            delay_ms = 1000

        export_formats = {
            'all': self.export_all_var.get(),
            'csv': self.export_csv_var.get(),
            'json': self.export_json_var.get(),
            'db': self.export_db_var.get(),
            'separate_json': self.export_sep_var.get()
        }

        # 3. Callbacks
        def safe_log(m):
            try: self.master.frames["Log"].log_message(m)
            except: pass

        # 4. State Management
        self.btn_start.configure(state="disabled")
        self.btn_pause.configure(state="normal")
        self.btn_stop.configure(state="normal")
        self.btn_try_failed.configure(state="disabled")
        self.status_label.configure(text=Localization.get_text("status_running", self.master.config.get("language", "English")), text_color="#3498db")

        # 5. Thread Execution
        # Safely evaluate CTkRadioButton variable
        raw_val = str(self.set_choice_var.get()).strip().lower()
        is_all = (raw_val == "1" or "all" in raw_val)

        self.worker = DatabaseUpdaterThread(
            links_file=links_f,
            existing_db_path=db_f,
            languages=langs,
            translate_missing=self.trans_var.get(),
            is_all_set=is_all,
            delay_ms=delay_ms,
            export_formats=export_formats,
            on_log=safe_log,
            on_progress=self.update_status,
            on_complete=self.on_task_complete,
            on_error=self.on_task_error,
            initial_rows=self.loaded_initial_rows
        )
        self.loaded_initial_rows = None
        self.worker.start()

    def update_status(self, text, color="gray"):
        self.after(0, lambda: self.status_label.configure(text=text, text_color=color))

    def on_task_complete(self, result):
        lang = self.master.config.get("language", "English")
        self.after(0, lambda: self.status_label.configure(text=Localization.get_text("status_done", lang), text_color="green"))
        self.after(0, self.reset_buttons)

    def on_task_error(self, err_msg):
        self.after(0, lambda: self.status_label.configure(text=f"Error: {err_msg}", text_color="red"))
        self.after(0, self.reset_buttons)

    def pause_process(self):
        if not self.worker: return
        
        lang = self.master.config.get("language", "English")
        # HOTFIX: Determine state by localized button text
        if self.btn_pause.cget("text") == Localization.get_text("btn_pause", lang):
            self.worker.pause()
            self.btn_pause.configure(text=Localization.get_text("btn_resume", lang))
            self.btn_pause.text_key = "btn_resume"
        else:
            self.worker.resume()
            self.btn_pause.configure(text=Localization.get_text("btn_pause", lang))
            self.btn_pause.text_key = "btn_pause"

    def stop_process(self):
        if self.worker:
            self.worker.stop()
            self.status_label.configure(text="Stopping...")

    def reset_buttons(self):
        self.btn_start.configure(state="normal")
        self.btn_pause.configure(state="disabled")
        self.btn_pause.text_key = "btn_pause"
        self.btn_stop.configure(state="disabled")
        self.update_localization(self.master.config.get("language", "English"))

    def save_progress(self):
        if self.worker and not self.worker.is_stopped():
            self.worker.pause()
            self.btn_pause.configure(text=Localization.get_text("btn_resume", self.master.config.get("language", "English")))
            self.btn_pause.text_key = "btn_resume"

        filename = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON Files", "*.json")])
        if not filename: return

        # Capture State
        state = {
            "ui_config": {
                "languages": [l for l, v in self.lang_vars.items() if v.get()],
                "translate_missing": self.trans_var.get(),
                "is_all_set": self.set_choice_var.get(),
                "delay_ms": self.delay_entry.get(),
                "db_file": self.db_file_entry.get(),
                "export_formats": {
                    'all': self.export_all_var.get(),
                    'csv': self.export_csv_var.get(),
                    'json': self.export_json_var.get(),
                    'db': self.export_db_var.get(),
                    'separate_json': self.export_sep_var.get()
                }
            },
            "worker_state": {
                "remaining_urls": self.worker.new_urls if self.worker else [],
                "processed_rows": self.worker.language_rows if self.worker else {}
            }
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
        
        self.update_status(f"Progress Saved: {os.path.basename(filename)}", "green")

    def load_progress(self):
        filename = filedialog.askopenfilename(filetypes=[("JSON Files", "*.json")])
        if not filename: return

        with open(filename, 'r', encoding='utf-8') as f:
            state = json.load(f)

        ui = state.get("ui_config", {})
        ws = state.get("worker_state", {})

        # Restore UI
        for lang, var in self.lang_vars.items():
            var.set(lang in ui.get("languages", []))
        self.trans_var.set(ui.get("translate_missing", False))
        self.set_choice_var.set(ui.get("is_all_set", 0))
        self.delay_entry.delete(0, 'end')
        self.delay_entry.insert(0, ui.get("delay_ms", "1000"))
        self.db_file_entry.delete(0, 'end')
        self.db_file_entry.insert(0, ui.get("db_file", ""))

        exp = ui.get("export_formats", {})
        self.export_all_var.set(exp.get('all', True))
        self.export_csv_var.set(exp.get('csv', False))
        self.export_json_var.set(exp.get('json', False))
        self.export_db_var.set(exp.get('db', False))
        self.export_sep_var.set(exp.get('separate_json', False))
        self._on_export_checkbox_changed()

        # Restore Worker State
        self.loaded_initial_rows = ws.get("processed_rows", {})
        remaining = ws.get("remaining_urls", [])

        if remaining:
            db_path = self.db_file_entry.get()
            save_dir = os.path.dirname(os.path.abspath(db_path)) if db_path else "."
            retry_path = os.path.join(save_dir, "retry_links.txt")
            with open(retry_path, 'w', encoding='utf-8') as f:
                f.write("\n".join(remaining))
            self.links_file_entry.delete(0, 'end')
            self.links_file_entry.insert(0, retry_path)

        self.update_status("Progress Loaded. Ready to Continue.", "blue")

    def load_failed_urls(self):
        db_path = self.db_file_entry.get()
        save_dir = os.path.dirname(os.path.abspath(db_path)) if db_path else "."
        
        # Look for failed_urls.txt in DB folder or current dir
        failed_path = os.path.join(save_dir, "failed_urls.txt")
        if not os.path.exists(failed_path):
            failed_path = "failed_urls.txt"

        if os.path.exists(failed_path):
            # FIXED: Create retry_links.txt to process seamlessly
            with open(failed_path, 'r', encoding='utf-8') as f:
                urls = f.readlines()
            
            retry_path = os.path.join(save_dir, "retry_links.txt")
            with open(retry_path, 'w', encoding='utf-8') as f:
                f.writelines(urls)
                
            self.links_file_entry.delete(0, 'end')
            self.links_file_entry.insert(0, os.path.abspath(retry_path))
            self.update_status(f"Loaded {len(urls)} failed URLs into retry queue.", "blue")
        else:
            messagebox.showinfo("Try Failed", "No failed_urls.txt found.")
