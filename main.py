import os
import customtkinter as ctk
from views.create_database import CreateDatabaseFrame
from views.update_database import UpdateDatabaseFrame
from views.translate_database import TranslateDatabaseFrame
from views.grabber_frame import LinkGrabberFrame
from views.extract_database import ExtractDatabaseFrame
from views.image_organizer import ImageOrganizerFrame
from views.options_frame import OptionsFrame
from views.about_frame import AboutFrame
from views.support_frame import SupportFrame
from views.placeholder import PlaceholderFrame
from views.log_frame import LogFrame
from core.data_manager import DataManager
from core.localization import Localization
from core.theme_manager import ThemeManager

ctk.set_appearance_mode("System")
ctk.set_default_color_theme("blue")

class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        # Load Global Config
        self.config = DataManager.load_config()

        # Initial Appearance & Theme
        ctk.set_appearance_mode(self.config.get("appearance_mode", "Dark"))
        
        self.title("YGO Card Database Creator v1.0.0.1")
        self.geometry("1100x750")

        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(1, weight=1)

        # -- Top Bar --
        self.topbar_frame = ctk.CTkFrame(self, height=50, corner_radius=0)
        self.topbar_frame.grid(row=0, column=0, columnspan=2, sticky="ew")
        
        # Appearance Toggle (Top-Left)
        self.appearance_mode_switch = ctk.CTkSwitch(
            self.topbar_frame, 
            command=self.change_appearance_mode_event,
            onvalue="Dark", offvalue="Light"
        )
        self.appearance_mode_switch.text_key = "lbl_dark_mode"
        self.appearance_mode_switch.pack(side="left", padx=20, pady=10)
        
        if self.config.get("appearance_mode", "Dark") == "Dark":
            self.appearance_mode_switch.select()
        else:
            self.appearance_mode_switch.deselect()

        # Language dropdown (Top-Right)
        self.lang_optionmenu = ctk.CTkOptionMenu(
            self.topbar_frame, 
            values=Localization.PRIMARY_LANGUAGES,
            command=self.apply_language
        )
        self.lang_optionmenu.pack(side="right", padx=20, pady=10)
        self.lang_optionmenu.set(self.config.get("language", "English"))
        
        self.lbl_lang_top = ctk.CTkLabel(self.topbar_frame)
        self.lbl_lang_top.text_key = "lbl_language"
        self.lbl_lang_top.pack(side="right", padx=(0, 10), pady=10)

        self.sidebar_frame = ctk.CTkFrame(self, width=200, corner_radius=0)
        self.sidebar_frame.grid(row=1, column=0, sticky="nsew")
        
        # Add a weight to the row above the exit button to push it to the bottom
        # We have 10 sidebar buttons (rows 1-10), so row 11 will be the spacer.
        self.sidebar_frame.grid_rowconfigure(11, weight=1) 

        self.btn_exit = ctk.CTkButton(
            self.sidebar_frame, 
            text="EXIT", 
            fg_color="#c0392b", 
            hover_color="#e74c3c",
            command=self.destroy
        )
        self.btn_exit.text_key = "btn_exit"
        self.btn_exit.grid(row=12, column=0, padx=20, pady=20, sticky="sew")

        self.logo_label = ctk.CTkLabel(self.sidebar_frame, font=ctk.CTkFont(size=20, weight="bold"))
        self.logo_label.grid(row=0, column=0, padx=20, pady=(20, 10))
        self.logo_label.text_key = "main_sidebar_logo"

        self.frames = {}
        self.current_frame = None

        # Init frames
        app_root = os.path.dirname(os.path.abspath(__file__))
        self.frames["Create Database"] = CreateDatabaseFrame(self, corner_radius=0, fg_color="transparent")
        self.frames["Update Database"] = UpdateDatabaseFrame(self, corner_radius=0, fg_color="transparent")
        self.frames["Translate Database"] = TranslateDatabaseFrame(self, corner_radius=0, fg_color="transparent")
        self.frames["Link Grabber"] = LinkGrabberFrame(self, app_root_path=app_root, corner_radius=0, fg_color="transparent")
        self.frames["Extractor and Organizer"] = ExtractDatabaseFrame(self, corner_radius=0, fg_color="transparent")
        self.frames["Image Add"] = ImageOrganizerFrame(self, corner_radius=0, fg_color="transparent")
        self.frames["Options"] = OptionsFrame(self, corner_radius=0, fg_color="transparent")
        self.frames["About"] = AboutFrame(self, corner_radius=0, fg_color="transparent")
        self.frames["Support"] = SupportFrame(self, corner_radius=0, fg_color="transparent")
        self.frames["Log"] = LogFrame(self, corner_radius=0, fg_color="transparent")
        
        self.sidebar_buttons = []
        
        # Initial synchronization and refresh
        self.update_ui_state()
        
        # Default Page
        self.select_page("Create Database")

    def apply_language(self, lang):
        """Centralized method with Sync Loop Guard."""
        if self.config.get("language") == lang:
            return
        
        self.config["language"] = lang
        DataManager.save_config(self.config)
        
        # Update both dropdowns
        self.lang_optionmenu.set(lang)
        if "Options" in self.frames:
            self.frames["Options"].lang_menu.set(lang)
            
        self.update_ui_state()

    def apply_theme(self, theme):
        """Centralized method with Sync Loop Guard."""
        if self.config.get("theme_variant") == theme:
            return
            
        self.config["theme_variant"] = theme
        DataManager.save_config(self.config)
        
        if "Options" in self.frames:
            self.frames["Options"].theme_menu.set(theme)
            
        self.update_ui_state()

    def apply_appearance_mode(self, mode):
        """Dark/Light Mode logic."""
        if self.config.get("appearance_mode") == mode:
            return
        
        self.config["appearance_mode"] = mode
        DataManager.save_config(self.config)
        ctk.set_appearance_mode(mode)

    def update_ui_state(self):
        """Global refresh trigger for language and theme."""
        lang = self.config.get("language", "English")
        theme = self.config.get("theme_variant", "Standard")
        
        # 1. Update Translations
        self.retranslate_ui()
        
        # 2. Update Theme (Accents Only)
        ThemeManager.apply_theme(self, theme)

    def retranslate_ui(self):
        lang = self.config.get("language", "English")
        
        # Refresh main app elements
        Localization.refresh_widgets(self, lang)
        
        # Rebuild Sidebar buttons
        sidebar_map = {
            "Create Database": "main_sidebar_create",
            "Update Database": "main_sidebar_update",
            "Translate Database": "main_sidebar_translate",
            "Link Grabber": "main_sidebar_grabber",
            "Log": "main_sidebar_log",
            "Extractor and Organizer": "main_sidebar_extractor",
            "Image Add": "main_sidebar_img_add",
            "Options": "main_sidebar_options",
            "About": "main_sidebar_about",
            "Support": "main_sidebar_support"
        }

        for _, btn in self.sidebar_buttons:
            btn.destroy()
        self.sidebar_buttons = []

        pages = [
            "Create Database", "Update Database", "Translate Database", 
            "Link Grabber", "Log", "Extractor and Organizer", "Image Add", 
            "Options", "About", "Support"
        ]

        for i, page_id in enumerate(pages):
            translated_text = Localization.get_text(sidebar_map.get(page_id, page_id), lang)
            btn = ctk.CTkButton(
                self.sidebar_frame, 
                text=translated_text, 
                fg_color="transparent", 
                text_color=("gray10", "gray90"), 
                hover_color=("gray70", "gray30"),
                anchor="w",
                command=lambda name=page_id: self.select_page(name)
            )
            btn.grid(row=i+1, column=0, padx=20, pady=5, sticky="ew")
            self.sidebar_buttons.append((page_id, btn))
        
        # Refresh root app UI elements (topbar, exit button, etc)
        Localization.refresh_widgets(self, lang)

        # Refresh all child frames
        for frame in self.frames.values():
            if hasattr(frame, "update_localization"):
                # Pass lang if method expects it, otherwise try without
                try:
                    frame.update_localization(lang)
                except TypeError:
                    frame.update_localization()
            else:
                Localization.refresh_widgets(frame, lang)

    def select_page(self, name):
        if self.current_frame is not None:
            self.current_frame.grid_forget()
        self.current_frame = self.frames[name]
        self.current_frame.grid(row=1, column=1, sticky="nsew")

        for btn_name, btn in self.sidebar_buttons:
            if btn_name == name:
                btn.configure(fg_color=("gray75", "gray25"))
            else:
                btn.configure(fg_color="transparent")

    def change_appearance_mode_event(self):
        new_mode = self.appearance_mode_switch.get()
        self.apply_appearance_mode(new_mode)

    def change_language_event(self, new_lang):
        self.apply_language(new_lang)

if __name__ == "__main__":
    app = App()
    app.mainloop()
