import os
import threading
import customtkinter as ctk
import tkinter.filedialog as filedialog
import zipfile
import io
import requests
from core.link_grabber import LinkGrabberThread
from core.localization import Localization
from core.parser import get_download_urls

class LinkGrabberFrame(ctk.CTkFrame):
    def __init__(self, master, app_root_path, **kwargs):
        super().__init__(master, **kwargs)
        self.app_root = app_root_path
        self.worker = None

        # 1. Header
        self.title_label = ctk.CTkLabel(self, font=ctk.CTkFont(size=20, weight="bold"))
        self.title_label.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="nw")
        self.title_label.text_key = "main_sidebar_grabber"

        # 2. Categories Container
        self.cat_frame = ctk.CTkFrame(self)
        self.cat_frame.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        
        self.cat_label = ctk.CTkLabel(self.cat_frame, font=ctk.CTkFont(weight="bold"))
        self.cat_label.grid(row=0, column=0, padx=10, pady=(10, 5), sticky="w")
        self.cat_label.text_key = "lbl_categories"

        # Stable Internal Keys (English) - Used to communicate with the Backend
        self.cat_keys = [
            "OCG+TCG", "OCG", "TCG", "TCG Speed Duel", 
            "Rush Duel", "Anime", "Other (Token+Skill)", "ALL"
        ]
        
        # Mapping Internal Keys to Localization Keys
        self.loc_map = {
            "OCG+TCG": "OCG+TCG",
            "OCG": "OCG",
            "TCG": "TCG",
            "TCG Speed Duel": "grabber_cat_speed",
            "Rush Duel": "grabber_cat_rush",
            "Anime": "grabber_cat_anime",
            "Other (Token+Skill)": "grabber_cat_other",
            "ALL": "grabber_cat_all"
        }

        self.cat_vars = {}
        self.cat_cbs = {}
        
        row_idx, col_idx = 1, 0
        for key in self.cat_keys:
            var = ctk.BooleanVar(value=(key == "OCG+TCG"))
            self.cat_vars[key] = var
            
            cb = ctk.CTkCheckBox(self.cat_frame, variable=var, 
                                 command=lambda k=key: self._on_category_changed(k))
            cb.grid(row=row_idx, column=col_idx, padx=10, pady=5, sticky="w")
            cb.text_key = self.loc_map[key]
            self.cat_cbs[key] = cb
            
            col_idx += 1
            if col_idx > 2:
                col_idx = 0
                row_idx += 1

        # Additional Option: Neuron
        self.neuron_var = ctk.BooleanVar()
        self.neuron_cb = ctk.CTkCheckBox(self.cat_frame, variable=self.neuron_var)
        self.neuron_cb.grid(row=row_idx+1, column=0, columnspan=3, padx=10, pady=10, sticky="w")
        self.neuron_cb.text_key = "cb_neuron"

        # 3. Settings (Delay)
        self.conf_frame = ctk.CTkFrame(self)
        self.conf_frame.grid(row=2, column=0, padx=20, pady=10, sticky="ew")
        
        self.lbl_delay = ctk.CTkLabel(self.conf_frame)
        self.lbl_delay.grid(row=0, column=0, padx=10, pady=10)
        self.lbl_delay.text_key = "lbl_delay"

        self.delay_slider = ctk.CTkSlider(self.conf_frame, from_=200, to=5000, number_of_steps=48, command=self._update_delay_label)
        self.delay_slider.set(1000)
        self.delay_slider.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        
        self.lbl_delay_val = ctk.CTkLabel(self.conf_frame, text="1000 ms", font=ctk.CTkFont(size=12))
        self.lbl_delay_val.grid(row=0, column=2, padx=10, pady=10)

        # Saving Folder Selection
        self.lbl_save = ctk.CTkLabel(self.conf_frame)
        self.lbl_save.grid(row=1, column=0, padx=10, pady=10)
        self.lbl_save.text_key = "lbl_save_folder"

        self.save_entry = ctk.CTkEntry(self.conf_frame, placeholder_text="Path to save scraped links...")
        self.save_entry.grid(row=1, column=1, padx=10, pady=10, sticky="ew")
        self.conf_frame.grid_columnconfigure(1, weight=1)

        self.btn_browse_save = ctk.CTkButton(self.conf_frame, width=80, command=self.browse_save)
        self.btn_browse_save.grid(row=1, column=2, padx=10, pady=10)
        self.btn_browse_save.text_key = "btn_browse"

        # 4. Github Prepared Data Section
        self.github_frame = ctk.CTkFrame(self)
        self.github_frame.grid(row=3, column=0, padx=20, pady=10, sticky="ew")

        self.lbl_github = ctk.CTkLabel(self.github_frame, font=ctk.CTkFont(weight="bold"))
        self.lbl_github.grid(row=0, column=0, padx=10, pady=10)
        self.lbl_github.text_key = "lbl_download_prepared"

        self.btn_download_zip = ctk.CTkButton(self.github_frame, command=self.download_prepared_data, fg_color="#3498db", hover_color="#2980b9")
        self.btn_download_zip.grid(row=0, column=1, padx=10, pady=10)
        self.btn_download_zip.text_key = "btn_download_prepared"

        # 5. Control Buttons
        self.btn_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.btn_frame.grid(row=4, column=0, padx=20, pady=15, sticky="ew")

        self.btn_container = ctk.CTkFrame(self.btn_frame, fg_color="transparent")
        self.btn_container.pack(expand=True)

        # Standardized Buttons (110x35)
        btn_w, btn_h = 110, 35

        self.btn_start = ctk.CTkButton(self.btn_container, width=btn_w, height=btn_h, command=self.execute_grab, fg_color="#2ecc71", hover_color="#27ae60")
        self.btn_start.pack(side="left", padx=5)
        self.btn_start.text_key = "btn_start"

        self.btn_pause = ctk.CTkButton(self.btn_container, width=btn_w, height=btn_h, command=self.pause_process, state="disabled")
        self.btn_pause.pack(side="left", padx=5)
        self.btn_pause.text_key = "btn_pause"

        self.btn_stop = ctk.CTkButton(self.btn_container, width=btn_w, height=btn_h, command=self.stop_process, fg_color="#e74c3c", hover_color="#c0392b", state="disabled")
        self.btn_stop.pack(side="left", padx=5)
        self.btn_stop.text_key = "btn_stop"

        self.btn_try_failed = ctk.CTkButton(self.btn_container, width=btn_w, height=btn_h, command=self.try_failed, fg_color="#f39c12", hover_color="#d35400")
        self.btn_try_failed.pack(side="left", padx=5)
        self.btn_try_failed.text_key = "btn_try_failed"

        self.btn_logs = ctk.CTkButton(self.btn_container, width=btn_w, height=btn_h, command=lambda: self.master.select_page("Log"))
        self.btn_logs.pack(side="left", padx=5)
        self.btn_logs.text_key = "main_sidebar_log"

        # 6. Status Footer
        self.status_label = ctk.CTkLabel(self, font=ctk.CTkFont(size=14, weight="bold"))
        self.status_label.grid(row=5, column=0, padx=20, pady=10, sticky="w")
        self.status_label.text_key = "status_ready"

        self.grid_columnconfigure(0, weight=1)
        self.update_localization(self.master.config.get("language", "English"))

    def _on_category_changed(self, changed_key):
        """Implement 'ALL' checkbox UX logic with categorical exclusion."""
        if changed_key == "ALL":
            is_all = self.cat_vars["ALL"].get()
            if is_all:
                # Uncheck and disable other categories
                for key in self.cat_keys:
                    if key != "ALL":
                        self.cat_vars[key].set(False)
                        self.cat_cbs[key].configure(state="disabled")
            else:
                # Re-enable other categories
                for key in self.cat_keys:
                    if key != "ALL":
                        self.cat_cbs[key].configure(state="normal")
        else:
            # If an individual category is checked, ensure 'ALL' is unchecked
            if self.cat_vars[changed_key].get():
                self.cat_vars["ALL"].set(False)

    def _update_delay_label(self, val):
        self.lbl_delay_val.configure(text=f"{int(val)} ms")

    def browse_save(self):
        folder = filedialog.askdirectory()
        if folder:
            self.save_entry.delete(0, 'end')
            self.save_entry.insert(0, folder)

    def download_prepared_data(self):
        cats = [key for key, var in self.cat_vars.items() if var.get()]
        if not cats:
            self.update_status("Error: Select a category first.", "red")
            return
            
        # Local mapping for the View to translate UI keys to Github file tags
        CAT_MAP = {
            "OCG+TCG": "-OCG+TCG-",
            "TCG": "-TCG-",
            "OCG": "-OCG-",
            "TCG Speed Duel": "-TCG Speed Duel-",
            "Rush Duel": "-Rush Duel-",
            "Other (Token+Skill)": "-Other (Token+Skill)-",
            "Anime": "-Anime-"
        }
        
        pool_path = self.master.config.get("links_pool_path", "")
        if not pool_path or not os.path.exists(pool_path):
            self.update_status("Error: links_pool.txt path not found in config.", "red")
            return

        # Map UI keys to tags safely
        tags = [CAT_MAP.get(c) for c in cats if c != "ALL"]
        if "ALL" in cats:
            tags = list(CAT_MAP.values())
            
        from core.parser import get_download_urls
        dl_dict = get_download_urls(pool_path, tags)
        urls = []
        for t_urls in dl_dict.values():
            urls.extend(t_urls)
            
        if not urls:
            self.update_status("Error: No Github download link found in pool.", "red")
            return
            
        target_url = urls[0]
        
        # Convert github blob to raw if necessary
        if "github.com" in target_url and "/blob/" in target_url:
            target_url = target_url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")

        self.btn_download_zip.configure(state="disabled")
        self.update_status("Downloading prepared data...", "blue")
        
        def download_worker():
            try:
                import requests, zipfile, io
                res = requests.get(target_url, stream=True, timeout=30)
                res.raise_for_status()
                
                self.update_status("Extracting ZIP...", "blue")
                save_dir = self.save_entry.get() or os.path.join(self.app_root, "links")
                os.makedirs(save_dir, exist_ok=True)
                
                with zipfile.ZipFile(io.BytesIO(res.content)) as thezip:
                    thezip.extractall(path=save_dir)
                    
                self.update_status("Download & Extraction Complete!", "green")
            except Exception as e:
                self.update_status(f"Download failed: {e}", "red")
            finally:
                self.after(0, lambda: self.btn_download_zip.configure(state="normal"))

        import threading
        threading.Thread(target=download_worker, daemon=True).start()

    def update_localization(self, lang):
        Localization.refresh_widgets(self, lang)

    def execute_grab(self):
        # 1. Collect checked stable English keys
        cats = [key for key, var in self.cat_vars.items() if var.get()]
        if not cats:
            return

        # 2. Extract settings
        delay_ms = int(self.delay_slider.get())
        
        # 3. UI State Management
        self.btn_start.configure(state="disabled")
        self.btn_pause.configure(state="normal", text=Localization.get_text("btn_pause", self.master.config.get("language", "English")))
        self.btn_pause.text_key = "btn_pause"
        self.btn_stop.configure(state="normal")
        
        self.status_label.configure(
            text=Localization.get_text("status_running", self.master.config.get("language", "English")),
            text_color="#3498db"
        )
        self.status_label.text_key = "status_running"

        def log_to_ui(msg):
            try:
                self.master.frames["Log"].log_message(msg)
            except:
                pass

        # 4. Background Execution
        custom_save = self.save_entry.get()
        
        self.worker = LinkGrabberThread(
            categories=cats,
            grab_neuron=self.neuron_var.get(),
            delay_ms=delay_ms,
            app_root=self.app_root,
            on_log=log_to_ui,
            on_progress=self.update_status,
            on_complete=self.on_task_complete,
            on_error=self.on_task_error
        )
        self.worker.custom_save_dir = custom_save if custom_save else None
        self.worker.start()

    def update_status(self, text, color="gray"):
        self.after(0, lambda: self.status_label.configure(text=text, text_color=color))

    def on_task_complete(self, result_msg):
        # Display the actual result message (contains error details if any)
        color = "#2ecc71" if "Completed" in result_msg else "#e74c3c"
        self.update_status(result_msg, color=color)
        self.after(0, self.reset_buttons)

    def on_task_error(self, err_msg):
        self.update_status(f"Error: {err_msg}", color="#e74c3c")
        self.after(0, self.reset_buttons)

    def pause_process(self):
        if not self.worker:
            return
        
        lang = self.master.config.get("language", "English")
        if self.worker.is_paused():
            self.worker.resume()
            self.btn_pause.configure(text=Localization.get_text("btn_pause", lang))
        else:
            self.worker.pause()
            self.btn_pause.configure(text=Localization.get_text("btn_resume", lang))

    def stop_process(self):
        if self.worker:
            self.worker.stop()

    def try_failed(self):
        # Implementation for re-trying failed categories or neuron scans
        pass

    def reset_buttons(self):
        self.btn_start.configure(state="normal")
        self.btn_pause.configure(state="disabled", text=Localization.get_text("btn_pause", self.master.config.get("language", "English")))
        self.btn_pause.text_key = "btn_pause"
        self.btn_stop.configure(state="disabled")
