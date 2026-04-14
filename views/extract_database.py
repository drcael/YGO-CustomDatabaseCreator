import os
import customtkinter as ctk
import tkinter.filedialog as filedialog
from tkinter import messagebox
from core.database_extractor import DatabaseExtractorThread
from core.localization import Localization

class ExtractDatabaseFrame(ctk.CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.all_langs = ["English", "Japanese", "German", "French", "Spanish", "Italian", "S. Chinese", "Turkish", "Korean", "Portuguese", "T. Chinese"]
        self.lang_vars = {}

        # 1. Header
        self.title_label = ctk.CTkLabel(self, font=ctk.CTkFont(size=20, weight="bold"))
        self.title_label.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="nw")
        self.title_label.text_key = "main_sidebar_extractor"

        # 2. Source Selection (Top)
        self.top_frame = ctk.CTkFrame(self)
        self.top_frame.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        self.top_frame.grid_columnconfigure(1, weight=1)

        self.lbl_source = ctk.CTkLabel(self.top_frame)
        self.lbl_source.grid(row=0, column=0, padx=10, pady=10)
        self.lbl_source.text_key = "lbl_source_file"

        self.source_entry = ctk.CTkEntry(self.top_frame, placeholder_text="Select source .db, .csv, or .json...")
        self.source_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        
        self.btn_browse = ctk.CTkButton(self.top_frame, width=80, command=self.browse_source)
        self.btn_browse.grid(row=0, column=2, padx=10, pady=10)
        self.btn_browse.text_key = "btn_browse"

        self.lbl_mode = ctk.CTkLabel(self.top_frame)
        self.lbl_mode.grid(row=1, column=0, padx=10, pady=10)
        self.lbl_mode.text_key = "lbl_process_mode"

        self.combo_mode = ctk.CTkOptionMenu(self.top_frame, values=["Filter & Extract", "Convert Format Only"])
        self.combo_mode.grid(row=1, column=1, padx=10, pady=10, sticky="w")

        # 3. Middle Sections (Languages & Filters)
        self.mid_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.mid_frame.grid(row=2, column=0, padx=20, pady=10, sticky="nsew")
        self.grid_rowconfigure(2, weight=1)
        self.mid_frame.grid_columnconfigure(1, weight=1) # Filters take more space

        # 3.1 Language Selection (Left)
        self.lang_frame = ctk.CTkFrame(self.mid_frame, width=200)
        self.lang_frame.grid(row=0, column=0, padx=(0, 10), sticky="nsew")
        
        self.lbl_sel_lang = ctk.CTkLabel(self.lang_frame, font=ctk.CTkFont(weight="bold"))
        self.lbl_sel_lang.pack(pady=5)
        self.lbl_sel_lang.text_key = "lbl_language"

        self.btn_all_langs = ctk.CTkButton(self.lang_frame, height=24, command=self.toggle_all_langs)
        self.btn_all_langs.pack(pady=2, padx=10, fill="x")
        self.btn_all_langs.text_key = "cb_all_langs"

        self.lang_scroll = ctk.CTkScrollableFrame(self.lang_frame, width=150)
        self.lang_scroll.pack(fill="both", expand=True, padx=5, pady=5)
        
        for lang in Localization.FULL_LANGUAGE_LIST:
            # Hotfix: Select ONLY English by default
            is_default = (lang == "English")
            var = ctk.BooleanVar(value=is_default)
            self.lang_vars[lang] = var
            cb = ctk.CTkCheckBox(self.lang_scroll, text=lang, variable=var, font=ctk.CTkFont(size=11), command=self._on_language_toggle)
            cb.pack(anchor="w", pady=2)

        # 3.2 Advanced Filters (Right)
        self.filters_frame = ctk.CTkFrame(self.mid_frame)
        self.filters_frame.grid(row=0, column=1, sticky="nsew")
        self.filters_frame.grid_columnconfigure(1, weight=1)
        self.filters_frame.grid_columnconfigure(3, weight=1)

        self.lbl_filter = ctk.CTkLabel(self.filters_frame, font=ctk.CTkFont(weight="bold"))
        self.lbl_filter.grid(row=0, column=0, columnspan=4, padx=10, pady=10, sticky="w")
        self.lbl_filter.text_key = "lbl_data_filters_title"

        # 1. Card Type
        self.lbl_card_type = ctk.CTkLabel(self.filters_frame)
        self.lbl_card_type.grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.lbl_card_type.text_key = "lbl_filter_card_type"

        self.combo_card_type = ctk.CTkOptionMenu(self.filters_frame, values=["All", "Monster", "Spell", "Trap"])
        self.combo_card_type.grid(row=1, column=1, padx=10, pady=5, sticky="ew")

        # 2. Attribute
        self.lbl_attr = ctk.CTkLabel(self.filters_frame)
        self.lbl_attr.grid(row=1, column=2, padx=10, pady=5, sticky="w")
        self.lbl_attr.text_key = "lbl_filter_attribute"

        self.combo_attr = ctk.CTkOptionMenu(self.filters_frame, values=["All", "DARK", "LIGHT", "EARTH", "FIRE", "WATER", "WIND", "DIVINE"])
        self.combo_attr.grid(row=1, column=3, padx=10, pady=5, sticky="ew")

        # 1b. Monster Type
        self.lbl_monster = ctk.CTkLabel(self.filters_frame)
        self.lbl_monster.grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.lbl_monster.text_key = "lbl_filter_monster_type"

        self.combo_monster_type = ctk.CTkOptionMenu(self.filters_frame, values=["All", "Dragon", "Warrior", "Spellcaster", "Zombie", "Fiend", "Fairy", "Psychic", "Wyrm", "Cyberse", "Beast", "Beast-Warrior", "Winged Beast", "Reptile", "Insect", "Dinosaur", "Sea Serpent", "Fish", "Aqua", "Pyro", "Rock", "Thunder", "Plant", "Machine", "Divine-Beast"])
        self.combo_monster_type.grid(row=2, column=1, padx=10, pady=5, sticky="ew")

        # 2b. Property
        self.lbl_prop = ctk.CTkLabel(self.filters_frame)
        self.lbl_prop.grid(row=2, column=2, padx=10, pady=5, sticky="w")
        self.lbl_prop.text_key = "lbl_filter_property"

        self.combo_property = ctk.CTkOptionMenu(self.filters_frame, values=["All", "Normal", "Continuous", "Equip", "Quick-Play", "Field", "Ritual", "Counter"])
        self.combo_property.grid(row=2, column=3, padx=10, pady=5, sticky="ew")

        # 3. Set Name/Number
        self.lbl_set = ctk.CTkLabel(self.filters_frame)
        self.lbl_set.grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.lbl_set.text_key = "lbl_filter_set" 
        
        self.ent_set = ctk.CTkEntry(self.filters_frame, placeholder_text="e.g. LOB")
        self.ent_set.grid(row=3, column=1, padx=10, pady=5, sticky="ew")

        # 4. Archetype/Link
        self.lbl_arch = ctk.CTkLabel(self.filters_frame)
        self.lbl_arch.grid(row=3, column=2, padx=10, pady=5, sticky="w")
        self.lbl_arch.text_key = "lbl_filter_archetype" 
        
        self.ent_arch = ctk.CTkEntry(self.filters_frame, placeholder_text="e.g. Blue-Eyes")
        self.ent_arch.grid(row=3, column=3, padx=10, pady=5, sticky="ew")

        # 4. Bot Options & Start
        self.bot_frame = ctk.CTkFrame(self)
        self.bot_frame.grid(row=3, column=0, padx=20, pady=10, sticky="ew")
        
        self.lbl_out = ctk.CTkLabel(self.bot_frame, font=ctk.CTkFont(weight="bold"))
        self.lbl_out.grid(row=0, column=0, padx=10, pady=10, sticky="w")
        self.lbl_out.text_key = "lbl_output_opts"

        self.cb_csv = ctk.CTkCheckBox(self.bot_frame, text="CSV")
        self.cb_csv.grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.cb_json = ctk.CTkCheckBox(self.bot_frame, text="JSON")
        self.cb_json.grid(row=1, column=1, padx=10, pady=5, sticky="w")
        self.cb_db = ctk.CTkCheckBox(self.bot_frame, text="SQLite DB")
        self.cb_db.grid(row=1, column=2, padx=10, pady=5, sticky="w")

        self.mse_var = ctk.BooleanVar(value=False)
        self.cb_mse_strict = ctk.CTkCheckBox(self.bot_frame, variable=self.mse_var, command=self._on_mse_toggle)
        self.cb_mse_strict.grid(row=2, column=0, columnspan=2, padx=10, pady=5, sticky="w")
        self.cb_mse_strict.text_key = "cb_mse_strict"

        self.sets_var = ctk.BooleanVar(value=False)
        self.cb_include_sets = ctk.CTkCheckBox(self.bot_frame, variable=self.sets_var, state="disabled")
        self.cb_include_sets.grid(row=2, column=2, columnspan=2, padx=10, pady=5, sticky="w")
        self.cb_include_sets.text_key = "cb_include_sets"

        self.lbl_out_name = ctk.CTkLabel(self.bot_frame)
        self.lbl_out_name.grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.lbl_out_name.text_key = "lbl_out_name"

        self.ent_output_name = ctk.CTkEntry(self.bot_frame)
        self.ent_output_name.grid(row=3, column=1, columnspan=3, padx=10, pady=5, sticky="ew")
        self.ent_output_name.insert(0, "extracted_database")

        # Normalized Button Frame
        self.btn_area = ctk.CTkFrame(self, fg_color="transparent")
        self.btn_area.grid(row=4, column=0, padx=20, pady=10, sticky="ew")

        self.btn_start_container = ctk.CTkFrame(self.btn_area, fg_color="transparent")
        self.btn_start_container.pack(expand=True)

        # Standardized Buttons (150x40 for larger footers)
        self.btn_extract = ctk.CTkButton(self.btn_start_container, width=150, height=40, command=self.start_extraction, font=ctk.CTkFont(weight="bold"))
        self.btn_extract.pack(side="left", padx=10)
        self.btn_extract.text_key = "btn_start"

        self.btn_logs = ctk.CTkButton(self.btn_start_container, width=150, height=40, command=lambda: self.master.select_page("Log"))
        self.btn_logs.pack(side="left", padx=10)
        self.btn_logs.text_key = "main_sidebar_log"

        # 5. Status
        self.status_label = ctk.CTkLabel(self, font=ctk.CTkFont(size=14, weight="bold"))
        self.status_label.grid(row=5, column=0, padx=20, pady=(0, 20), sticky="w")
        self.status_label.text_key = "status_ready"

        self.grid_columnconfigure(0, weight=1)
        self.update_localization(self.master.config.get("language", "English"))

    def update_localization(self, lang):
        Localization.refresh_widgets(self, lang)

    def browse_source(self):
        file = filedialog.askopenfilename(filetypes=[("Database Files", "*.db;*.csv;*.json"), ("All Files", "*.*")])
        if file:
            self.source_entry.delete(0, 'end')
            self.source_entry.insert(0, file)

    def toggle_all_langs(self):
        all_on = all(v.get() for v in self.lang_vars.values())
        for v in self.lang_vars.values(): v.set(not all_on)
        self._on_language_toggle()

    def _on_language_toggle(self):
        count = sum(v.get() for v in self.lang_vars.values())
        if count > 1:
            self.mse_var.set(False)
            self.cb_mse_strict.deselect()
            self.cb_mse_strict.configure(state="disabled")
            # Cascade lock to sub-options
            self._on_mse_toggle()
        else:
            self.cb_mse_strict.configure(state="normal")
            # Re-evaluate cascade state
            self._on_mse_toggle()

    def _on_mse_toggle(self):
        """Handle cascade dependency: Set Numbers require MSE Mode."""
        if self.mse_var.get() and self.cb_mse_strict.cget("state") != "disabled":
            self.cb_include_sets.configure(state="normal")
        else:
            self.sets_var.set(False)
            self.cb_include_sets.deselect()
            self.cb_include_sets.configure(state="disabled")

    def start_extraction(self):
        source = self.source_entry.get()
        if not source or not os.path.exists(source): 
            messagebox.showwarning("Warning", "Select a valid source file first.")
            return
        
        langs = [l for l, v in self.lang_vars.items() if v.get()]
        if not langs:
            messagebox.showwarning("No Language", "Please select at least one language.")
            return

        export_formats = {
            "csv": self.cb_csv.get(),
            "json": self.cb_json.get(),
            "db": self.cb_db.get()
        }
        
        if not any(export_formats.values()):
            messagebox.showwarning("Warning", "Select at least one export format (CSV, JSON, or DB).")
            return

        filters = {
            "Card_Type": self.combo_card_type.get(),
            "Monster_Type": self.combo_monster_type.get(),
            "Monster_Attribute": self.combo_attr.get(),
            "Property": self.combo_property.get(),
            "Set": self.ent_set.get().strip(),
            "Archetype": self.ent_arch.get().strip()
        }

        self.btn_extract.configure(state="disabled")
        self.status_label.configure(text=Localization.get_text("status_running", self.master.config.get("language", "English")), text_color="blue")
        
        self.worker = DatabaseExtractorThread(
            source_path=source,
            filters=filters,
            export_formats=export_formats,
            output_name=self.ent_output_name.get() or "extracted_database",
            languages=langs,
            process_mode=self.combo_mode.get(),
            mse_strict=self.cb_mse_strict.get(),
            include_sets=self.cb_include_sets.get(),
            on_log=lambda m: self.master.frames["Log"].log_message(m),
            on_progress=self.update_status,
            on_complete=self.on_task_complete,
            on_error=self.on_task_error
        )
        self.worker.start()

    def update_status(self, text, color="gray"):
        self.after(0, lambda: self.status_label.configure(text=text, text_color=color))

    def on_task_complete(self, result):
        self.update_status(Localization.get_text("status_done", self.master.config.get("language", "English")), color="green")
        self.after(0, self.reset_buttons)

    def on_task_error(self, err):
        self.update_status(f"Error: {err}", color="red")
        self.after(0, self.reset_buttons)

    def reset_buttons(self):
        self.btn_extract.configure(state="normal")
