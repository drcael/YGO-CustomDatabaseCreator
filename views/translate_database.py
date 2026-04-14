import os
import json
import customtkinter as ctk
import tkinter.filedialog as filedialog
import tkinter.messagebox as messagebox
from core.localization import Localization
from core.database_translator import DatabaseTranslatorThread

# ISO map for translation
LANG_CODE_MAP = {
    "Arabic": "ar", "Bengali": "bn", "Czech": "cs", "Danish": "da", 
    "Dutch": "nl", "French": "fr", "Finnish": "fi", "German": "de", 
    "Greek": "el", "Hindi": "hi", "Hungarian": "hu", "Indonesian": "id", 
    "Italian": "it", "Japanese": "ja", "Korean": "ko", "Malay": "ms", 
    "Norwegian": "no", "Polish": "pl", "Portuguese": "pt", "Romanian": "ro", 
    "Russian": "ru", "Spanish": "es", "Swedish": "sv", "Tagalog": "tl", 
    "Thai": "th", "Turkish": "tr", "Vietnamese": "vi",
    "S. Chinese": "zh-CN", "T. Chinese": "zh-TW"
}

class TranslateDatabaseFrame(ctk.CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.worker = None
        self.lang_vars = {}
        self.last_selected_langs = []
        self.loaded_initial_results = None

        # 1. Header
        self.title_label = ctk.CTkLabel(self, font=ctk.CTkFont(size=20, weight="bold"))
        self.title_label.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="nw")
        self.title_label.text_key = "main_sidebar_translate"

        # 2. Source Selection
        self.path_frame = ctk.CTkFrame(self)
        self.path_frame.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        self.path_frame.grid_columnconfigure(1, weight=1)

        self.lbl_source = ctk.CTkLabel(self.path_frame)
        self.lbl_source.grid(row=0, column=0, padx=10, pady=10)
        self.lbl_source.text_key = "lbl_source_file"

        self.source_entry = ctk.CTkEntry(self.path_frame, placeholder_text="Select Database, CSV, or JSON file...")
        self.source_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        
        self.btn_browse = ctk.CTkButton(self.path_frame, width=80, command=self.browse_source)
        self.btn_browse.grid(row=0, column=2, padx=10, pady=10)
        self.btn_browse.text_key = "btn_browse"

        # 3. Middle Sections (Stacked Vertically)
        # 3.1 Translation Options (Full Width)
        self.opts_frame = ctk.CTkFrame(self)
        self.opts_frame.grid(row=2, column=0, padx=20, pady=10, sticky="ew")

        self.lbl_opts = ctk.CTkLabel(self.opts_frame, font=ctk.CTkFont(weight="bold"))
        self.lbl_opts.grid(row=0, column=0, columnspan=3, pady=10, padx=10, sticky="w")
        self.lbl_opts.text_key = "lbl_output_opts"

        self.cb_add_new_table = ctk.CTkCheckBox(self.opts_frame, text="Add as new table to DB", state="disabled")
        self.cb_add_new_table.grid(row=1, column=0, pady=5, padx=10, sticky="w")
        self.cb_add_new_table.text_key = "cb_add_new_table" 

        self.cb_sep_files = ctk.CTkCheckBox(self.opts_frame, text="Export as Separate Files")
        self.cb_sep_files.grid(row=1, column=1, pady=5, padx=10, sticky="w")
        self.cb_sep_files.text_key = "cb_export_separate"

        # Format Checkboxes
        self.format_frame = ctk.CTkFrame(self.opts_frame, fg_color="transparent")
        self.format_frame.grid(row=1, column=2, padx=10, pady=5, sticky="e")
        
        self.cb_csv = ctk.CTkCheckBox(self.format_frame, text="CSV", width=60)
        self.cb_csv.pack(side="left", padx=2)
        self.cb_json = ctk.CTkCheckBox(self.format_frame, text="JSON", width=60)
        self.cb_json.pack(side="left", padx=2)
        self.cb_db = ctk.CTkCheckBox(self.format_frame, text="DB", width=60)
        self.cb_db.pack(side="left", padx=2)

        self.delay_label = ctk.CTkLabel(self.opts_frame)
        self.delay_label.grid(row=2, column=0, pady=(5, 0), padx=10, sticky="w")
        self.delay_label.text_key = "lbl_delay"

        self.delay_entry = ctk.CTkEntry(self.opts_frame, width=80)
        self.delay_entry.insert(0, "1500")
        self.delay_entry.grid(row=2, column=1, padx=10, pady=5, sticky="w")

        # Saving Folder Row
        self.lbl_save = ctk.CTkLabel(self.opts_frame)
        self.lbl_save.grid(row=4, column=0, padx=10, pady=10, sticky="w")
        self.lbl_save.text_key = "lbl_save_folder"

        self.save_entry = ctk.CTkEntry(self.opts_frame, placeholder_text="Select destination folder for JSON/CSV...")
        self.save_entry.grid(row=4, column=1, columnspan=1, padx=10, pady=10, sticky="ew")
        self.opts_frame.grid_columnconfigure(1, weight=1)

        self.btn_browse_save = ctk.CTkButton(self.opts_frame, width=80, command=self.browse_save)
        self.btn_browse_save.grid(row=4, column=2, padx=10, pady=10)
        self.btn_browse_save.text_key = "btn_browse"

        # 3.2 Multi-Language Selection (Full Width)
        self.lang_sel_frame = ctk.CTkFrame(self)
        self.lang_sel_frame.grid(row=3, column=0, padx=20, pady=10, sticky="nsew")
        self.grid_rowconfigure(3, weight=1)

        self.lbl_lang = ctk.CTkLabel(self.lang_sel_frame, font=ctk.CTkFont(weight="bold"))
        self.lbl_lang.pack(pady=10, padx=10)
        self.lbl_lang.text_key = "lbl_select_lang_max3"

        self.lang_scroll = ctk.CTkScrollableFrame(self.lang_sel_frame)
        self.lang_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        # Populate Languages (3 Columns)
        all_langs = [l for l in Localization.FULL_LANGUAGE_LIST if l != "English"]
        for i, lang in enumerate(all_langs):
            v = ctk.BooleanVar(value=False)
            self.lang_vars[lang] = v
            cb = ctk.CTkCheckBox(self.lang_scroll, text=lang, variable=v, command=self._on_lang_checkbox_click)
            cb.grid(row=i // 3, column=i % 3, sticky="w", pady=2, padx=10)

        # 4. Action Buttons (Grid Layout)
        self.btn_container = ctk.CTkFrame(self, fg_color="transparent")
        self.btn_container.grid(row=4, column=0, padx=20, pady=10, sticky="ew")
        
        # Configure columns to distribute equally
        for i in range(4): self.btn_container.grid_columnconfigure(i, weight=1)

        self.btn_start = ctk.CTkButton(self.btn_container, text="Start Translation", command=self.start_translation, font=ctk.CTkFont(weight="bold"))
        self.btn_start.grid(row=0, column=0, padx=5, pady=5, sticky="ew")
        self.btn_start.text_key = "btn_start"

        self.btn_pause = ctk.CTkButton(self.btn_container, text="Pause", state="disabled", command=self.pause_translation)
        self.btn_pause.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        self.btn_pause.text_key = "btn_pause"

        self.btn_stop = ctk.CTkButton(self.btn_container, text="Stop", state="disabled", fg_color="#e74c3c", hover_color="#c0392b", command=self.stop_translation)
        self.btn_stop.grid(row=0, column=2, padx=5, pady=5, sticky="ew")
        self.btn_stop.text_key = "btn_stop"

        self.btn_try_failed = ctk.CTkButton(self.btn_container, text="Try Failed", fg_color="#f39c12", hover_color="#d68910", command=self.load_failed_urls, state="disabled")
        self.btn_try_failed.grid(row=0, column=3, padx=5, pady=5, sticky="ew")
        self.btn_try_failed.text_key = "btn_try_failed"

        self.btn_save_progress = ctk.CTkButton(self.btn_container, text="Save Progress", fg_color="#2ecc71", hover_color="#27ae60", command=self.save_progress)
        self.btn_save_progress.grid(row=1, column=0, columnspan=2, padx=5, pady=5, sticky="ew")
        self.btn_save_progress.text_key = "btn_save_progress"

        self.btn_load_progress = ctk.CTkButton(self.btn_container, text="Load Progress", fg_color="#3498db", hover_color="#2980b9", command=self.load_progress)
        self.btn_load_progress.grid(row=1, column=2, columnspan=2, padx=5, pady=5, sticky="ew")
        self.btn_load_progress.text_key = "btn_load_progress"

        self.btn_logs = ctk.CTkButton(self.btn_container, text="Logs", command=lambda: self.master.select_page("Log"))
        self.btn_logs.grid(row=2, column=0, columnspan=4, padx=5, pady=5, sticky="ew")
        self.btn_logs.text_key = "main_sidebar_log"

        # 5. Status
        self.status_label = ctk.CTkLabel(self, text="Ready", font=ctk.CTkFont(size=14, weight="bold"))
        self.status_label.grid(row=5, column=0, padx=20, pady=(0, 20), sticky="w")
        self.status_label.text_key = "status_ready"

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=0) # Options frame shouldn't expand
        self.grid_rowconfigure(3, weight=1) # Language scroll SHOULD expand
        self.grid_rowconfigure(4, weight=0) # Button container shouldn't expand
        self.update_localization(self.master.config.get("language", "English"))

    def update_localization(self, lang):
        Localization.refresh_widgets(self, lang)

    def _on_lang_checkbox_click(self):
        current_selected = [lang for lang, v in self.lang_vars.items() if v.get()]
        if len(current_selected) > 3:
            # Revert the one that was just added
            newly_added = [l for l in current_selected if l not in self.last_selected_langs]
            if newly_added:
                self.lang_vars[newly_added[0]].set(False)
                messagebox.showwarning("Limit Reached", "You can select a maximum of 3 languages for bulk translation to avoid API bans.")
            
        self.last_selected_langs = [lang for lang, v in self.lang_vars.items() if v.get()]

    def browse_source(self):
        file = filedialog.askopenfilename(filetypes=[("Database Files", "*.db"), ("Data Files", "*.csv;*.json"), ("All Files", "*.*")])
        if file:
            self.source_entry.delete(0, 'end')
            self.source_entry.insert(0, file)
            
            ext = os.path.splitext(file)[1].lower()
            if ext in ['.csv', '.json']:
                # CSV/JSON cannot have multiple tables: Force separate files
                self.cb_add_new_table.deselect()
                self.cb_add_new_table.configure(state="disabled")
                self.cb_sep_files.select()
                self.cb_sep_files.configure(state="disabled")
            elif ext == '.db':
                # Database source: User can choose to add tables or separate
                self.cb_add_new_table.configure(state="normal")
                self.cb_add_new_table.select()
                self.cb_sep_files.configure(state="normal")
                self.cb_sep_files.deselect()
            else:
                self.cb_add_new_table.deselect()
                self.cb_add_new_table.configure(state="disabled")
                self.cb_sep_files.configure(state="normal")

    def browse_save(self):
        folder = filedialog.askdirectory()
        if folder:
            self.save_entry.delete(0, 'end')
            self.save_entry.insert(0, folder)

    def start_translation(self):
        source = self.source_entry.get()
        if not source or not os.path.exists(source):
            return

        selected_langs = {l: LANG_CODE_MAP[l] for l, v in self.lang_vars.items() if v.get()}
        if not selected_langs:
            messagebox.showwarning("No Language", "Please select at least one target language.")
            return

        try:
            delay_ms = int(self.delay_entry.get())
        except:
            delay_ms = 1500

        self.btn_start.configure(state="disabled")
        self.btn_pause.configure(state="normal")
        self.btn_stop.configure(state="normal")
        self.btn_try_failed.configure(state="disabled")
        self.status_label.configure(text=Localization.get_text("status_running", self.master.config.get("language", "English")), text_color="blue")

        self.worker = DatabaseTranslatorThread(
            input_file=source,
            target_langs=selected_langs,
            delay_ms=delay_ms,
            separate_files=self.cb_sep_files.get(),
            on_log=lambda m: self.master.frames["Log"].log_message(m),
            on_progress=self.update_status,
            on_complete=self.on_task_complete,
            on_error=self.on_task_error,
            initial_results=self.loaded_initial_results
        )
        self.loaded_initial_results = None
        self.worker.start()

    def update_status(self, text, color="gray"):
        self.after(0, lambda: self.status_label.configure(text=text, text_color=color))

    def on_task_complete(self, result):
        self.update_status(Localization.get_text("status_done", self.master.config.get("language", "English")), color="green")
        self.after(0, self.reset_buttons)

    def on_task_error(self, err):
        self.update_status(f"Error: {err}", color="red")
        self.after(0, self.reset_buttons)

    def stop_translation(self):
        if self.worker:
            self.worker.stop()

    def pause_translation(self):
        if not self.worker: return
        lang = self.master.config.get("language", "English")
        if self.btn_pause.cget("text") == Localization.get_text("btn_pause", lang):
            self.worker.pause()
            self.btn_pause.configure(text=Localization.get_text("btn_resume", lang))
            self.btn_pause.text_key = "btn_resume"
        else:
            self.worker.resume()
            self.btn_pause.configure(text=Localization.get_text("btn_pause", lang))
            self.btn_pause.text_key = "btn_pause"

    def save_progress(self):
        if self.worker and not self.worker.is_stopped():
            self.worker.pause()
            self.btn_pause.configure(text=Localization.get_text("btn_resume", self.master.config.get("language", "English")))

        filename = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON Files", "*.json")])
        if not filename: return

        state = {
            "ui_config": {
                "source_file": self.source_entry.get(),
                "delay_ms": self.delay_entry.get(),
                "sep_files": self.cb_sep_files.get(),
                "target_langs": [l for l, v in self.lang_vars.items() if v.get()]
            },
            "worker_state": {
                "processed_data": self.worker.results if self.worker else {}
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

        self.source_entry.delete(0, 'end')
        self.source_entry.insert(0, ui.get("source_file", ""))
        self.delay_entry.delete(0, 'end')
        self.delay_entry.insert(0, ui.get("delay_ms", "1500"))
        
        if ui.get("sep_files"): self.cb_sep_files.select()
        else: self.cb_sep_files.deselect()

        for lang, var in self.lang_vars.items():
            var.set(lang in ui.get("target_langs", []))

        self.loaded_initial_results = ws.get("processed_data", {})
        self.update_status("Progress Loaded. Ready to Continue.", "blue")

    def load_failed_urls(self):
        save_dir = self.save_entry.get() or (os.path.dirname(os.path.abspath(self.source_entry.get())) if self.source_entry.get() else ".")
        failed_path = os.path.join(save_dir, "failed_urls.txt")
        if not os.path.exists(failed_path): failed_path = "failed_urls.txt"

        if os.path.exists(failed_path):
            with open(failed_path, 'r', encoding='utf-8') as f:
                urls = f.readlines()
            
            retry_path = os.path.join(save_dir, "retry_links.txt")
            with open(retry_path, 'w', encoding='utf-8') as f:
                f.writelines(urls)
                
            self.source_entry.delete(0, 'end')
            self.source_entry.insert(0, os.path.abspath(retry_path))
            self.update_status(f"Loaded {len(urls)} failed URLs into retry queue.", "blue")
        else:
            messagebox.showinfo("Try Failed", "No failed_urls.txt found.")

    def reset_buttons(self):
        self.btn_start.configure(state="normal")
        self.btn_pause.configure(state="disabled")
        self.btn_stop.configure(state="disabled")
