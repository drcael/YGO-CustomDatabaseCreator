import customtkinter as ctk
import webbrowser
from core.localization import Localization

class SupportFrame(ctk.CTkFrame):
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
        self.small_italic = ctk.CTkFont(size=11, slant="italic")
        self.group_header_font = ctk.CTkFont(size=14, weight="bold")

        self.setup_ui()
        self.update_localization(self.master.config.get("language", "English"))

    def setup_ui(self):
        # 1. Title
        self.lbl_title = ctk.CTkLabel(self.scroll_frame, font=self.title_font, text_color="#3498db")
        self.lbl_title.pack(pady=(20, 10))
        self.lbl_title.text_key = "support_main_title"

        self.lbl_sub = ctk.CTkLabel(self.scroll_frame, font=self.body_font)
        self.lbl_sub.pack(pady=(0, 20))
        self.lbl_sub.text_key = "support_sub"

        # 2. Social Accounts (Grouped)
        social_header = ctk.CTkLabel(self.scroll_frame, font=self.header_font)
        social_header.pack(pady=(10, 10))
        social_header.text_key = "support_socials"

        social_groups = [
            ("support_group_code", [
                ("GitHub", "https://github.com/drcael"),
                ("GitHub Repo", "https://github.com/drcael/YGO-CustomDatabaseCreator"),
                ("1001 Tools Repo", "https://github.com/drcael/1001tools"),
                ("1001 Tools Web", "https://drcael.github.io/projects/1001tools/"),
                ("SourceForge", "https://sourceforge.net/u/drcael1/profile/"),
                ("HuggingFace", "https://huggingface.co/drcael"),
                ("Kaggle", "https://www.kaggle.com/drcael")
            ]),
            ("support_group_prof", [
                ("LinkedIn", "https://www.linkedin.com/in/burhanerdemir/"),
                ("StackOverflow", "https://stackoverflow.com/users/9747089/burhan-erdemir"),
                ("Medium", "https://medium.com/@burhanerdemir"),
                ("Blog", "https://drcael.github.io/blog"),
                ("WebSite", "https://drcael.github.io"),
                ("DEV.Community", "https://dev.to/drcael"),
                ("HashNode", "https://hashnode.com/@drcael")
            ]),
            ("support_group_social", [
                ("Reddit", "https://www.reddit.com/user/burhanerdemir/"),
                ("DeviantArt", "https://deviantart.com/drcael"),
                ("Instagram (B. Erdemir)", "https://www.instagram.com/burhanerdemir.drc"),
                ("Instagram (drcael)", "https://www.instagram.com/drcael"),
                ("X (Twitter)", "https://x.com/drcael")
            ])
        ]

        for group_key, links in social_groups:
            glbl = ctk.CTkLabel(self.scroll_frame, font=self.group_header_font, text_color="gray")
            glbl.pack(pady=(15, 5))
            glbl.text_key = group_key
            
            grid_frame = ctk.CTkFrame(self.scroll_frame, fg_color="transparent")
            grid_frame.pack(fill="x", padx=40)
            grid_frame.grid_columnconfigure((0, 1, 2, 3), weight=1)

            for i, (name, url) in enumerate(links):
                btn = ctk.CTkButton(grid_frame, text=name, command=lambda u=url: webbrowser.open(u),
                                    fg_color="transparent", border_width=1, border_color=("gray70", "gray30"),
                                    text_color=("gray10", "gray90"), hover_color=("gray80", "gray20"), height=30)
                btn.grid(row=i // 4, column=i % 4, padx=5, pady=5, sticky="ew")

        # 3. Platform Support
        plat_header = ctk.CTkLabel(self.scroll_frame, font=self.header_font)
        plat_header.pack(pady=(40, 10))
        plat_header.text_key = "support_platforms"

        self.plat_frame = ctk.CTkFrame(self.scroll_frame, fg_color="transparent")
        self.plat_frame.pack(pady=5)

        platforms = [
            ("Patreon", "https://patreon.com/burhanerdemir", "#f96854"),
            ("BuyMeACoffee", "https://buymeacoffee.com/drcael", "#FFDD00"),
            ("Ko-Fi", "https://ko-fi.com/drcael", "#29abe0")
        ]

        for name, url, color in platforms:
            text_c = "black" if name == "BuyMeACoffee" else "white"
            ctk.CTkButton(self.plat_frame, text=name, command=lambda u=url: webbrowser.open(u),
                          fg_color=color, hover_color=color, text_color=text_c, width=140).pack(side="left", padx=10)

        # 4. Crypto Donation Section
        crypto_header = ctk.CTkLabel(self.scroll_frame, font=self.header_font)
        crypto_header.pack(pady=(40, 10))
        crypto_header.text_key = "support_crypto"

        cwlbl = ctk.CTkLabel(self.scroll_frame, font=self.small_italic, text_color="#e74c3c")
        cwlbl.pack()
        cwlbl.text_key = "support_crypto_warn"

        self.crypto_list = ctk.CTkFrame(self.scroll_frame, fg_color=("gray90", "gray15"), corner_radius=10)
        self.crypto_list.pack(fill="x", padx=30, pady=20)
        self.crypto_list.grid_columnconfigure(1, weight=1)

        wallets = [
            ("BTC", "15QuhaXt8THsSSfxT7MEcXXWLm4x34xuUT"),
            ("BTC (BEP20)", "0xb697ac751ff642e8aea62fd7ac2dfe93852dedc4"),
            ("ETH", "0xb697ac751ff642e8aea62fd7ac2dfe93852dedc4"),
            ("TRX (TRC20)", "TVoghyRBr4QzCWXdkF2x3kp3ZPYMxGyxiA"),
            ("TRX (BEP20)", "0xb697ac751ff642e8aea62fd7ac2dfe93852dedc4"),
            ("TON / USDT", "UQDErOQrvEJkc1v_S3GajShKeAOcf-tpu_YwI_ys1PWk9isD"),
            ("BNB (BEP20)", "0xb697ac751ff642e8aea62fd7ac2dfe93852dedc4"),
            ("USDT (BEP20)", "0xb697ac751ff642e8aea62fd7ac2dfe93852dedc4"),
            ("USDT (TRC20)", "TVoghyRBr4QzCWXdkF2x3kp3ZPYMxGyxiA"),
            ("SOL", "H88zkkt5Newho9JzPToGCpfAfATEz4StmhYainY8Bxae"),
            ("DOGE", "DR3pRGKqTDSbvd9i9nDYKvFHE8FPhKAR7j")
        ]

        for i, (name, addr) in enumerate(wallets):
            lbl = ctk.CTkLabel(self.crypto_list, text=name, font=ctk.CTkFont(weight="bold"), width=120, anchor="w")
            lbl.grid(row=i, column=0, padx=(15, 5), pady=10)

            entry = ctk.CTkEntry(self.crypto_list, corner_radius=5)
            entry.insert(0, addr)
            entry.configure(state="readonly")
            entry.grid(row=i, column=1, padx=5, pady=10, sticky="ew")

            btn = ctk.CTkButton(self.crypto_list, width=60, 
                                command=lambda a=addr: self.copy_to_clipboard(a))
            btn.grid(row=i, column=2, padx=(5, 15), pady=10)
            btn.text_key = "support_copy_btn"

        # 5. Status Label (Confirmation)
        self.status_label = ctk.CTkLabel(self, text="", font=ctk.CTkFont(size=12, weight="bold"), text_color="#2ecc71")
        self.status_label.grid(row=1, column=0, pady=(0, 10))

    def copy_to_clipboard(self, text):
        self.master.clipboard_clear()
        self.master.clipboard_append(text)
        lang = self.master.config.get("language", "English")
        self.status_label.configure(text=Localization.get_text("support_copied_msg", lang))
        self.after(3000, lambda: self.status_label.configure(text=""))

    def update_localization(self, lang):
        Localization.refresh_widgets(self.scroll_frame, lang)
