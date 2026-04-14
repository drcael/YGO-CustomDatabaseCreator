import os

def parse_link_pool(filepath, target_categories, section_marker="[Link-Grab-Adress]"):
    """
    Parses link_pool.txt and returns a dictionary mapping category to a list of URLs.
    
    :param filepath: Path to link_pool.txt.
    :param target_categories: List of category string markers (e.g. ['-TCG-', '-OCG-']).
    :param section_marker: The block to parse (either '[Link-Grab-Adress]' or '[Link-Download-Adress]').
    :return: dict of {category_marker: [urls]}
    """
    if not os.path.exists(filepath):
        return {}

    parsed_data = {cat: [] for cat in target_categories}
    
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]

    # Find the section boundaries
    try:
        start_idx = lines.index(section_marker)
        # Find the next occurrence which marks the end
        end_idx = lines.index(section_marker, start_idx + 1)
    except ValueError:
        # If markers are missing, just return empty
        return {}

    section_lines = lines[start_idx+1:end_idx]

    # Iterate and parse between target categories
    for cat in target_categories:
        try:
            # Find the start block for this category
            cat_start = section_lines.index(cat)
            # Find the end block (next occurrence of the exact same category marker)
            cat_end = section_lines.index(cat, cat_start + 1)
            
            # The urls are between cat_start and cat_end
            for idx in range(cat_start + 1, cat_end):
                parsed_data[cat].append(section_lines[idx])
        except ValueError:
            # Category not found or malformed block
            pass

    return parsed_data

def get_grab_urls(filepath, categories):
    """
    Extracts scraping starting URLs.
    """
    return parse_link_pool(filepath, categories, "[Link-Grab-Adress]")

def get_download_urls(filepath, categories):
    """
    Extracts ZIP download URLs.
    """
    return parse_link_pool(filepath, categories, "[Link-Download-Adress]")
