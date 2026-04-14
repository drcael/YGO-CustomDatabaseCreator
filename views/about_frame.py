import customtkinter as ctk
import webbrowser
from core.localization import Localization

class AboutFrame(ctk.CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # Main Scrollable Container
        self.scroll_frame = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.scroll_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        self.scroll_frame.grid_columnconfigure(0, weight=1)

        # Fonts
        self.title_font = ctk.CTkFont(size=24, weight="bold")
        self.header_font = ctk.CTkFont(size=18, weight="bold")
        self.body_font = ctk.CTkFont(size=13)
        self.italic_font = ctk.CTkFont(size=12, slant="italic")

        self.setup_ui()
        self.update_localization(self.master.config.get("language", "English"))

    def setup_ui(self):
        # 1. Title Section
        self.lbl_title = ctk.CTkLabel(self.scroll_frame, font=self.title_font, text_color="#3498db")
        self.lbl_title.pack(pady=(20, 5))
        self.lbl_title.text_key = "about_main_title"
        
        self.lbl_version = ctk.CTkLabel(self.scroll_frame, font=self.italic_font)
        self.lbl_version.pack(pady=(0, 20))
        self.lbl_version.text_key = "about_version"

        # 2. Main Description
        self.lbl_desc = ctk.CTkLabel(self.scroll_frame, font=self.body_font, justify="center", wraplength=600)
        self.lbl_desc.pack(padx=20, pady=10)
        self.lbl_desc.text_key = "about_desc"

        # 3. Features Section (Grouped)
        self.features_frame = ctk.CTkFrame(self.scroll_frame, fg_color=("gray90", "gray15"), corner_radius=10)
        self.features_frame.pack(fill="x", padx=30, pady=20)
        
        feat_title = ctk.CTkLabel(self.features_frame, font=self.header_font)
        feat_title.pack(pady=(10, 5))
        feat_title.text_key = "about_features"

        self.feat_link = ctk.CTkLabel(self.features_frame, font=self.body_font, justify="left", anchor="w")
        self.feat_link.pack(fill="x", padx=20, pady=2)
        self.feat_link.text_key = "about_feat_link_full"

        self.feat_create = ctk.CTkLabel(self.features_frame, font=self.body_font, justify="left", anchor="w")
        self.feat_create.pack(fill="x", padx=20, pady=2)
        self.feat_create.text_key = "about_feat_create_full"

        self.feat_update = ctk.CTkLabel(self.features_frame, font=self.body_font, justify="left", anchor="w")
        self.feat_update.pack(fill="x", padx=20, pady=2)
        self.feat_update.text_key = "about_feat_update_full"

        self.feat_trans = ctk.CTkLabel(self.features_frame, font=self.body_font, justify="left", anchor="w")
        self.feat_trans.pack(fill="x", padx=20, pady=2)
        self.feat_trans.text_key = "about_feat_translate_full"

        self.feat_export = ctk.CTkLabel(self.features_frame, font=self.body_font, justify="left", anchor="w")
        self.feat_export.pack(fill="x", padx=20, pady=2)
        self.feat_export.text_key = "about_feat_export_full"

        self.feat_img = ctk.CTkLabel(self.features_frame, font=self.body_font, justify="left", anchor="w")
        self.feat_img.pack(fill="x", padx=20, pady=2)
        self.feat_img.text_key = "about_feat_img_full"

        # 4. Project Policy Section
        self.policy_frame = ctk.CTkFrame(self.scroll_frame, fg_color="transparent")
        self.policy_frame.pack(fill="x", padx=30, pady=10)
        
        policy_title = ctk.CTkLabel(self.policy_frame, font=self.header_font)
        policy_title.pack(pady=(10, 5), anchor="w")
        policy_title.text_key = "about_policy"

        self.lbl_policy = ctk.CTkLabel(self.policy_frame, font=self.body_font, justify="left", wraplength=600)
        self.lbl_policy.pack(anchor="w", padx=10)
        self.lbl_policy.text_key = "about_policy_text"

        # 5. License & Disclaimer
        self.license_frame = ctk.CTkFrame(self.scroll_frame, fg_color="transparent")
        self.license_frame.pack(fill="x", padx=30, pady=10)
        
        self.lbl_cc = ctk.CTkLabel(self.license_frame, font=self.body_font, wraplength=600, justify="left")
        self.lbl_cc.pack(anchor="w", pady=5)
        self.lbl_cc.text_key = "about_license"

        self.lbl_disclaimer = ctk.CTkLabel(self.license_frame, font=ctk.CTkFont(size=12, slant="italic"), 
                                           text_color="#e74c3c", wraplength=600, justify="left")
        self.lbl_disclaimer.pack(anchor="w", pady=5)
        self.lbl_disclaimer.text_key = "about_disclaimer"

        # 6. Links Section
        self.links_frame = ctk.CTkFrame(self.scroll_frame, fg_color="transparent")
        self.links_frame.pack(pady=20)

        self.btn_visit = ctk.CTkButton(self.links_frame, command=lambda: webbrowser.open("https://yugipedia.com/"), 
                                       fg_color="#3498db", hover_color="#2980b9", width=150)
        self.btn_visit.pack(side="left", padx=10)
        self.btn_visit.text_key = "about_btn_visit"
        
        self.btn_github = ctk.CTkButton(self.links_frame, command=lambda: webbrowser.open("https://github.com/drcael/YGO-CustomDatabaseCreator"),
                                        fg_color="gray30", hover_color="gray20", width=150)
        self.btn_github.pack(side="left", padx=10)
        self.btn_github.text_key = "about_btn_github"

        # 7. Copyright & Resources
        self.lbl_res = ctk.CTkLabel(self.scroll_frame, font=self.italic_font)
        self.lbl_res.pack(pady=5)
        self.lbl_res.text_key = "about_resources"

        self.lbl_copy = ctk.CTkLabel(self.scroll_frame, font=self.body_font, justify="center")
        self.lbl_copy.pack(pady=(20, 10))
        self.lbl_copy.text_key = "about_copyright"

        self.lbl_ygo_copy = ctk.CTkLabel(self.scroll_frame, text="©1996 KAZUKI TAKAHASHI\n©2020 Studio Dice/SHUEISHA, TV TOKYO, KONAMI", font=ctk.CTkFont(size=11, weight="bold"), text_color="gray")
        self.lbl_ygo_copy.pack(pady=10)

    def update_localization(self, lang):
        Localization.refresh_widgets(self.scroll_frame, lang)
