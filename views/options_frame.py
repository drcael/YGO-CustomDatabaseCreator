import os
import threading
import tkinter.filedialog as filedialog
import customtkinter as ctk
from core.data_manager import DataManager
from core.localization import Localization
from core.theme_manager import ThemeManager

class OptionsFrame(ctk.CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        
        self.config = DataManager.load_config()
        self.pack_urls = {}

        # Main Layout
        self.grid_columnconfigure(0, weight=1)
        
        # 1. Path Settings
        self.path_frame = ctk.CTkFrame(self)
        self.path_frame.grid(row=0, column=0, padx=20, pady=20, sticky="ew")
        self.path_frame.grid_columnconfigure(0, weight=1)

        self.lbl_save = ctk.CTkLabel(self.path_frame, font=ctk.CTkFont(weight="bold"))
        self.lbl_save.grid(row=0, column=0, padx=10, pady=(10, 0), sticky="w")
        self.lbl_save.text_key = "lbl_save_folder"

        self.save_folder_entry = ctk.CTkEntry(self.path_frame)
        self.save_folder_entry.grid(row=1, column=0, padx=10, pady=(0, 10), sticky="ew")
        
        self.btn_browse_save = ctk.CTkButton(self.path_frame, width=80, command=lambda: self.browse_folder("default_save_folder", self.save_folder_entry))
        self.btn_browse_save.grid(row=1, column=1, padx=10, pady=(0, 10))
        self.btn_browse_save.text_key = "btn_browse"

        self.lbl_pool = ctk.CTkLabel(self.path_frame, font=ctk.CTkFont(weight="bold"))
        self.lbl_pool.grid(row=2, column=0, padx=10, pady=(10, 0), sticky="w")
        self.lbl_pool.text_key = "lbl_links_pool"

        self.pool_entry = ctk.CTkEntry(self.path_frame)
        self.pool_entry.grid(row=3, column=0, padx=10, pady=(0, 10), sticky="ew")
        
        self.btn_browse_pool = ctk.CTkButton(self.path_frame, width=80, command=self.browse_pool_file)
        self.btn_browse_pool.grid(row=3, column=1, padx=10, pady=(0, 10))
        self.btn_browse_pool.text_key = "btn_browse"

        # 2. Appearance & Language
        self.settings_frame = ctk.CTkFrame(self)
        self.settings_frame.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        
        self.lbl_lang = ctk.CTkLabel(self.settings_frame)
        self.lbl_lang.text_key = "lbl_language"
        self.lbl_lang.grid(row=0, column=0, padx=10, pady=10)
        
        # Sync-Ready Language Menu
        langs = Localization.FULL_LANGUAGE_LIST
        self.lang_menu = ctk.CTkOptionMenu(self.settings_frame, values=langs, command=self.change_lang_event)
        self.lang_menu.grid(row=0, column=1, padx=10, pady=10)
        self.lang_menu.set(self.config.get("language", "English"))

        self.lbl_theme = ctk.CTkLabel(self.settings_frame)
        self.lbl_theme.grid(row=1, column=0, padx=10, pady=10)
        self.lbl_theme.text_key = "lbl_theme"
        
        # Sync-Ready Theme Menu
        themes = ThemeManager.get_all_themes()
        self.theme_menu = ctk.CTkOptionMenu(self.settings_frame, values=themes, command=self.change_theme_event)
        self.theme_menu.grid(row=1, column=1, padx=10, pady=10)
        self.theme_menu.set(self.config.get("theme_variant", "Standard"))

        # 3. Dynamic Downloader
        self.download_frame = ctk.CTkFrame(self)
        self.download_frame.grid(row=2, column=0, padx=20, pady=10, sticky="ew")
        self.download_frame.grid_columnconfigure(0, weight=1)

        self.lbl_down = ctk.CTkLabel(self.download_frame, font=ctk.CTkFont(weight="bold"))
        self.lbl_down.grid(row=0, column=0, padx=10, pady=10, sticky="w")
        self.lbl_down.text_key = "btn_download" # Reusing key or localized specifically
        
        self.pack_dropdown = ctk.CTkOptionMenu(self.download_frame, values=["No Data Found"])
        self.pack_dropdown.grid(row=1, column=0, padx=10, pady=10, sticky="ew")

        self.btn_download = ctk.CTkButton(self.download_frame, command=self.start_download)
        self.btn_download.grid(row=1, column=1, padx=10, pady=10)
        self.btn_download.text_key = "btn_download"

        self.progress_bar = ctk.CTkProgressBar(self.download_frame)
        self.progress_bar.grid(row=2, column=0, columnspan=2, padx=10, pady=(0, 10), sticky="ew")
        self.progress_bar.set(0)

        self.download_status_label = ctk.CTkLabel(self.download_frame, text="Ready", font=ctk.CTkFont(size=12))
        self.download_status_label.grid(row=3, column=0, columnspan=2, padx=10, pady=(0, 10), sticky="w")
        self.download_status_label.text_key = "status_ready"

        # Initial States
        self.save_folder_entry.insert(0, self.config.get("default_save_folder", ""))
        self.pool_entry.insert(0, self.config.get("links_pool_path", ""))
        self.refresh_downloader()
        self.update_localization(self.config.get("language", "English"))

    def update_localization(self, lang):
        Localization.refresh_widgets(self, lang)

    def browse_folder(self, key, entry):
        folder = filedialog.askdirectory()
        if folder:
            entry.delete(0, 'end')
            entry.insert(0, folder)
            self.config[key] = folder
            DataManager.save_config(self.config)

    def browse_pool_file(self):
        file = filedialog.askopenfilename(filetypes=[("Text Files", "*.txt")])
        if file:
            self.pool_entry.delete(0, 'end')
            self.pool_entry.insert(0, file)
            self.config["links_pool_path"] = file
            DataManager.save_config(self.config)
            self.refresh_downloader()

    def refresh_downloader(self):
        path = self.config.get("links_pool_path")
        self.pack_urls = DataManager.parse_links_pool(path)
        if self.pack_urls:
            self.pack_dropdown.configure(values=list(self.pack_urls.keys()))
            self.pack_dropdown.set(list(self.pack_urls.keys())[0])
        else:
            self.pack_dropdown.configure(values=["No Data Found"])
            self.pack_dropdown.set("No Data Found")

    def change_lang_event(self, new_lang):
        """Bridge call to Master for centralized synchronization."""
        if hasattr(self.master, "apply_language"):
            self.master.apply_language(new_lang)

    def change_theme_event(self, new_theme):
        """Bridge call to Master for centralized synchronization."""
        if hasattr(self.master, "apply_theme"):
            self.master.apply_theme(new_theme)

    def start_download(self):
        tag = self.pack_dropdown.get()
        url = self.pack_urls.get(tag)
        if url:
            self.btn_download.configure(state="disabled")
            threading.Thread(target=self.download_worker, args=(url, tag), daemon=True).start()

    def download_worker(self, url, tag):
        # 1. FIX CORRUPTED ZIP: Convert GitHub blob to raw URL
        if "github.com" in url and "/blob/" in url:
            url = url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")

        save_path = os.path.join(os.getcwd(), "links_pack.zip")
        def up(p): self.after(0, lambda: self.progress_bar.set(p))
        
        self.after(0, lambda: self.download_status_label.configure(text=f"Downloading {tag}...", text_color="blue"))

        success, msg = DataManager.download_prepared_data(url, save_path, up)
        
        if success:
            # 2. FIX EXTRACTION PATH: Extract to downloads/tag_name
            import re
            safe_tag = re.sub(r'[^a-zA-Z0-9_\- ]', '', tag).strip()
            extract_to = os.path.join(os.getcwd(), "downloads", safe_tag)
            os.makedirs(extract_to, exist_ok=True)
            
            self.after(0, lambda: self.download_status_label.configure(text="Extracting...", text_color="blue"))
            DataManager.extract_zip(save_path, extract_to)
            
            # Clean up the temp zip file
            try: os.remove(save_path)
            except: pass
            
            final_msg = f"Complete: Extracted to downloads/{safe_tag}"
            is_error = False
        else:
            final_msg = f"Error: {msg}"
            is_error = True

        self.after(0, lambda: self.finish_download(final_msg, is_error))

    def finish_download(self, msg, is_error):
        self.btn_download.configure(state="normal")
        self.progress_bar.set(0)
        
        color = "red" if is_error else "green"
        self.download_status_label.configure(text=msg, text_color=color)
        
        # Log to main log frame
        try:
            self.master.frames["Log"].log_message(msg)
        except:
            pass
