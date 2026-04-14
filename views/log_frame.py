import customtkinter as ctk
from core.localization import Localization

class LogFrame(ctk.CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # 1. Header
        self.header_label = ctk.CTkLabel(self, font=ctk.CTkFont(size=20, weight="bold"))
        self.header_label.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="nw")
        self.header_label.text_key = "main_sidebar_log"

        # 2. Log Text Area
        self.log_text = ctk.CTkTextbox(self, font=ctk.CTkFont(family="Consolas", size=12))
        self.log_text.grid(row=1, column=0, padx=20, pady=10, sticky="nsew")
        self.log_text.configure(state="disabled")

        self.update_localization(self.master.config.get("language", "English"))

    def log_message(self, message):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{Localization.get_text('status_running', self.master.config.get('language', 'English'))}] {message}\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def update_localization(self, lang):
        Localization.refresh_widgets(self, lang)
