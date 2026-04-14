import customtkinter as ctk

class ThemeManager:
    # 13 Total Options
    THEMES = {
        "Standard": {"primary": "#3a7ebf", "hover": "#325882"},
        "Midnight": {"primary": "#F2AA4C", "hover": "#bc843b"},
        "Ember": {"primary": "#e74c3c", "hover": "#c0392b"},
        "Forest": {"primary": "#2ecc71", "hover": "#27ae60"},
        "Cyber": {"primary": "#00f3ff", "hover": "#00b8c4"},
        "Ghost": {"primary": "#95a5a6", "hover": "#7f8c8d"},
        "Sweet": {"primary": "#e84393", "hover": "#d63031"},
        "Lavender": {"primary": "#6c5ce7", "hover": "#4834d4"},
        "Sepia": {"primary": "#7d5a5a", "hover": "#5a3e3e"},
        "Frost": {"primary": "#0984e3", "hover": "#74b9ff"},
        "Mint": {"primary": "#00b894", "hover": "#55efc4"}
    }

    @classmethod
    def apply_theme(cls, app, theme_name):
        """
        Entry point for theme application.
        Only updates accent colors (primary, hover).
        """
        data = cls.THEMES.get(theme_name, cls.THEMES["Standard"])
        
        # 1. Inject primary colors recursively
        # Native CTk backgrounds are handled by the Topbar switch
        cls.update_app_theme(app, data)

    @classmethod
    def update_app_theme(cls, root, theme_data):
        """
        Safe, unidirectional traversal of the widget tree for theme application.
        Avoids circular loops by strictly moving DOWN the hierarchy.
        """
        # 1. Apply primary colors to the current widget if applicable
        try:
            if isinstance(theme_data, dict):
                primary_color = theme_data.get("primary")
                hover_color = theme_data.get("hover", primary_color)
                
                # Check for buttons, switches, and progress bars safely
                # Using string names for robustness across CTK versions
                w_type = str(type(root)).lower()
                
                if hasattr(root, "configure"):
                    # Accent widgets
                    if "button" in w_type or "switch" in w_type:
                        root.configure(fg_color=primary_color, hover_color=hover_color)
                    elif "progress" in w_type:
                        root.configure(progress_color=primary_color)
                    elif "slider" in w_type:
                        root.configure(progress_color=primary_color, button_color=primary_color, button_hover_color=hover_color)
                    elif "checkbox" in w_type or "radiobutton" in w_type:
                        root.configure(fg_color=primary_color, hover_color=hover_color)
        except Exception:
            pass

        # 2. Recursively visit children safely (Traverse DOWN only)
        try:
            if hasattr(root, "winfo_children"):
                for child in root.winfo_children():
                    cls.update_app_theme(child, theme_data)
        except Exception:
            pass

        # 3. Special handling for CTK Tabviews (children are in tabs)
        if hasattr(root, "_tab_dict"):
            try:
                for tab_name in root._tab_dict:
                    cls.update_app_theme(root.tab(tab_name), theme_data)
            except Exception:
                pass

    @classmethod
    def get_all_themes(cls):
        return list(cls.THEMES.keys())
