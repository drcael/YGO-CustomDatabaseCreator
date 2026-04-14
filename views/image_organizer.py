import os
import customtkinter as ctk
import tkinter.filedialog as filedialog
from core.localization import Localization
from core.image_linker import ImageLinkerThread

class ImageOrganizerFrame(ctk.CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)

        # 1. Header
        self.title_label = ctk.CTkLabel(self, font=ctk.CTkFont(size=20, weight="bold"))
        self.title_label.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="nw")
        self.title_label.text_key = "main_sidebar_img_add"

        # 2. Input Paths
        self.input_frame = ctk.CTkFrame(self)
        self.input_frame.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        self.input_frame.grid_columnconfigure(1, weight=1)

        self.lbl_db = ctk.CTkLabel(self.input_frame)
        self.lbl_db.grid(row=0, column=0, padx=10, pady=10)
        self.lbl_db.text_key = "lbl_target_db"

        self.db_entry = ctk.CTkEntry(self.input_frame, placeholder_text="Path to target project database (.db)...")
        self.db_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        
        self.btn_db = ctk.CTkButton(self.input_frame, width=80, command=self.browse_db)
        self.btn_db.grid(row=0, column=2, padx=10, pady=10)
        self.btn_db.text_key = "btn_browse"

        self.lbl_img = ctk.CTkLabel(self.input_frame)
        self.lbl_img.grid(row=1, column=0, padx=10, pady=10)
        self.lbl_img.text_key = "lbl_img_folder"

        self.img_entry = ctk.CTkEntry(self.input_frame, placeholder_text="Folder containing raw card images...")
        self.img_entry.grid(row=1, column=1, padx=10, pady=10, sticky="ew")
        
        self.btn_img = ctk.CTkButton(self.input_frame, width=80, command=self.browse_img)
        self.btn_img.grid(row=1, column=2, padx=10, pady=10)
        self.btn_img.text_key = "btn_browse"

        self.lbl_lang = ctk.CTkLabel(self.input_frame)
        self.lbl_lang.grid(row=2, column=0, padx=10, pady=10)
        self.lbl_lang.text_key = "lbl_img_lang"

        self.combo_lang = ctk.CTkOptionMenu(self.input_frame, values=Localization.FULL_LANGUAGE_LIST)
        self.combo_lang.grid(row=2, column=1, padx=10, pady=10, sticky="w")
        self.combo_lang.set("English")

        # 3. Match Criteria Frame
        self.criteria_frame = ctk.CTkFrame(self)
        self.criteria_frame.grid(row=2, column=0, padx=20, pady=10, sticky="ew")
        self.criteria_frame.grid_columnconfigure(1, weight=1)

        self.lbl_crit = ctk.CTkLabel(self.criteria_frame, font=ctk.CTkFont(weight="bold"))
        self.lbl_crit.grid(row=0, column=0, columnspan=2, padx=10, pady=10, sticky="w")
        self.lbl_crit.text_key = "lbl_match_criteria_title"

        # 3.1 Algorithm
        self.lbl_match = ctk.CTkLabel(self.criteria_frame)
        self.lbl_match.grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.lbl_match.text_key = "lbl_match_criteria"

        self.combo_criteria = ctk.CTkOptionMenu(self.criteria_frame, values=["By Password (Gamecode)", "By Filename"])
        self.combo_criteria.grid(row=1, column=1, padx=10, pady=5, sticky="w")

        # 3.2 Save Format
        self.lbl_format = ctk.CTkLabel(self.criteria_frame)
        self.lbl_format.grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.lbl_format.text_key = "lbl_path_format"

        self.combo_path = ctk.CTkOptionMenu(self.criteria_frame, values=["Relative Path", "Absolute Path", "Filename Only"])
        self.combo_path.grid(row=2, column=1, padx=10, pady=5, sticky="w")

        # 3.3 Extensions
        self.lbl_ext = ctk.CTkLabel(self.criteria_frame)
        self.lbl_ext.grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.lbl_ext.text_key = "lbl_supported_ext"

        self.ent_ext = ctk.CTkEntry(self.criteria_frame, placeholder_text=".jpg, .png, .webp, .jpeg")
        self.ent_ext.grid(row=3, column=1, padx=10, pady=5, sticky="ew")
        self.ent_ext.insert(0, ".jpg, .png, .webp, .jpeg")

        # 4. Action Buttons Footer
        self.btn_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.btn_frame.grid(row=3, column=0, padx=20, pady=15, sticky="ew")

        self.btn_container = ctk.CTkFrame(self.btn_frame, fg_color="transparent")
        self.btn_container.pack(expand=True)

        # Standardized Buttons (150x40)
        btn_w, btn_h = 150, 40

        self.btn_start = ctk.CTkButton(self.btn_container, width=btn_w, height=btn_h, command=self.start_organizing, font=ctk.CTkFont(weight="bold"))
        self.btn_start.pack(side="left", padx=10)
        self.btn_start.text_key = "btn_match_images"

        self.btn_logs = ctk.CTkButton(self.btn_container, width=btn_w, height=btn_h, command=lambda: self.master.select_page("Log"))
        self.btn_logs.pack(side="left", padx=10)
        self.btn_logs.text_key = "main_sidebar_log"

        # 5. Status
        self.status_label = ctk.CTkLabel(self, font=ctk.CTkFont(size=14, weight="bold"))
        self.status_label.grid(row=4, column=0, padx=20, pady=(0, 20), sticky="w")
        self.status_label.text_key = "status_ready"

        self.grid_columnconfigure(0, weight=1)
        self.update_localization(self.master.config.get("language", "English"))

    def update_localization(self, lang):
        Localization.refresh_widgets(self, lang)

    def browse_db(self):
        file = filedialog.askopenfilename(filetypes=[("Database Files", "*.db")])
        if file:
            self.db_entry.delete(0, 'end')
            self.db_entry.insert(0, file)

    def browse_img(self):
        folder = filedialog.askdirectory()
        if folder:
            self.img_entry.delete(0, 'end')
            self.img_entry.insert(0, folder)

    def start_organizing(self):
        db_path = self.db_entry.get()
        img_folder = self.img_entry.get()
        
        if not db_path or not os.path.exists(db_path): return
        if not img_folder or not os.path.exists(img_folder): return

        self.btn_start.configure(state="disabled")
        self.status_label.configure(text=Localization.get_text("status_running", self.master.config.get("language", "English")), text_color="blue")

        self.worker = ImageLinkerThread(
            target_db=db_path,
            images_folder=img_folder,
            criteria=self.combo_criteria.get(),
            path_format=self.combo_path.get(),
            extensions=self.ent_ext.get(),
            language=self.combo_lang.get(),
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
        self.btn_start.configure(state="normal")
