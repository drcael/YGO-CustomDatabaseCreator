import customtkinter as ctk

class PlaceholderFrame(ctk.CTkFrame):
    def __init__(self, master, title, **kwargs):
        super().__init__(master, **kwargs)
        self.label = ctk.CTkLabel(self, text=title, font=ctk.CTkFont(size=20, weight="bold"))
        self.label.pack(expand=True)
