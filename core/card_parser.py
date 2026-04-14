import cloudscraper
import json
import re
from bs4 import BeautifulSoup

class YugipediaParser:
    def __init__(self):
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    def fetch_html(self, url):
        response = self.scraper.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.text
        else:
            print(f"Error: Connection blocked or failed, Status {response.status_code}")
            return None

    def _get_table_row_value(self, soup, header_text):
        """Helper to find a <th> containing the given text, and return its sibling <td> text."""
        for th in soup.find_all('th'):
            if header_text.lower() in th.text.lower():
                td = th.find_next_sibling('td')
                if td:
                    # Clean up multiple whitespaces/newlines into a single space
                    return re.sub(r'\s+', ' ', td.text).strip()
        return None

    def _get_table_row_html(self, soup, header_text):
        """Helper to find a <th> and return its sibling <td> element directly."""
        for th in soup.find_all('th'):
            if header_text.lower() in th.text.lower():
                td = th.find_next_sibling('td')
                if td:
                    return td
        return None

    def _extract_text(self, element):
        """Extract text from a BeautifulSoup element, preserving structural line breaks
        but keeping inline tags (like <a>) joined as continuous text.
        
        - Replaces <br>, </p>, </li>, </dd> boundaries with explicit newlines
        - Does NOT use separator='\n' which would fragment inline <a> tags
        - Cleans up multiple consecutive newlines
        - Preserves bullet characters like ●
        """
        import copy
        elem = copy.copy(element)
        # Insert newlines after block-level / line-break tags
        for br in elem.find_all('br'):
            br.replace_with('\n')
        for tag_name in ['p', 'li']:
            for tag in elem.find_all(tag_name):
                tag.insert_before('\n')
                tag.insert_after('\n')
        # Get text without any separator — inline <a> tags merge naturally
        text = elem.get_text(separator='', strip=False)
        # Clean up: collapse multiple newlines, strip each line, remove empties
        lines = [line.strip() for line in text.split('\n')]
        lines = [line for line in lines if line]
        text = '\n'.join(lines)
        
        # Ensure bullet points always start on a new line
        text = re.sub(r'([^\n])(●)', r'\1\n\2', text)
        
        return text.strip()

    def parse_card(self, url):
        html = self.fetch_html(url)
        if not html:
            return {}
        html = html.replace('\r', '')

        soup = BeautifulSoup(html, 'html.parser')
        card_data = {}

        # Name
        heading = soup.find('div', class_='heading')
        if heading:
            card_data['Name'] = heading.text.strip()
            
        # General Data
        card_type = self._get_table_row_value(soup, 'Card type')
        if card_type:
            card_data['Card Type'] = card_type

        attribute = self._get_table_row_value(soup, 'Attribute')
        property_ = self._get_table_row_value(soup, 'Property')
        if attribute:
            card_data['Attribute'] = attribute
        elif property_:
            card_data['Property'] = property_

        types = self._get_table_row_value(soup, 'Types')
        if types:
            card_data['Types'] = types
            
        # Passcode / Password
        passcode = self._get_table_row_value(soup, 'Password')
        if not passcode:
            passcode = self._get_table_row_value(soup, 'Passcode')
        card_data['Passcode'] = passcode if passcode else ""

        # Status (OCG / TCG / Genesys)
        for th in soup.find_all('th'):
            if th.text.strip().lower() == 'status':
                td = th.find_next_sibling('td')
                if td:
                    badges = td.find_all('div', class_='status-badge')
                    for badge in badges:
                        badge_text = badge.text.strip()
                        if 'OCG' in badge_text:
                            card_data['Status_OCG'] = badge_text
                        elif 'TCG' in badge_text:
                            card_data['Status_TCG'] = badge_text
                        elif 'Genesys' in badge_text:
                            card_data['Genesys_Status'] = badge_text
                    # Combined status string
                    status_parts = [b.text.strip() for b in badges]
                    if status_parts:
                        card_data['Status'] = ' / '.join(status_parts)
                break

        # Effect Types
        for th in soup.find_all('th'):
            if 'effect type' in th.text.strip().lower():
                td = th.find_next_sibling('td')
                if td:
                    dls = td.find_all('dl')
                    if dls:
                        # Pendulum cards: DT = category, DD = types
                        effect_parts = []
                        for dl in dls:
                            for dt in dl.find_all('dt'):
                                dd = dt.find_next_sibling('dd')
                                if dd:
                                    # Get individual types from <li> or plain text
                                    lis = dd.find_all('li')
                                    types_list = [li.text.strip() for li in lis] if lis else [dd.text.strip()]
                                    effect_parts.append(f"{dt.text.strip()}: {', '.join(types_list)}")
                        card_data['Effect_Types'] = ' | '.join(effect_parts)
                    else:
                        # Simple list of effect types
                        lis = td.find_all('li')
                        if lis:
                            card_data['Effect_Types'] = ', '.join([li.text.strip() for li in lis])
                        else:
                            card_data['Effect_Types'] = re.sub(r'\s+', ' ', td.text).strip()
                    try: print(f"[{card_data.get('Name', 'Unknown')}] Effect Types: {card_data.get('Effect_Types', '')}")
                    except UnicodeEncodeError: pass
                break

        # Archetypes
        archetypes = []
        # Yugipedia uses <dl><dt>Archetypes and series</dt><dd><a>name</a></dd></dl>
        # under the 'Search categories' section
        search_cat_span = soup.find('span', id='Search_categories')
        if search_cat_span:
            search_section = search_cat_span.find_parent(['h2', 'h3'])
            if search_section:
                for sibling in search_section.find_next_siblings():
                    if sibling.name in ['h2', 'h3']:
                        break
                    # Find all <dl> elements in this section
                    dls = sibling.find_all('dl') if hasattr(sibling, 'find_all') else []
                    for dl in dls:
                        for dt in dl.find_all('dt'):
                            dt_text = dt.get_text().strip().lower()
                            if 'archetype' in dt_text:
                                # Get all <dd> siblings that follow this <dt>
                                for dd in dt.find_next_siblings('dd'):
                                    a = dd.find('a')
                                    if a:
                                        name = a.text.strip()
                                        if name and name not in archetypes:
                                            archetypes.append(name)
        if archetypes:
            card_data['Archetypes'] = archetypes
            try: print(f"[{card_data.get('Name', 'Unknown')}] Archetypes: {archetypes[:5]}")
            except UnicodeEncodeError: pass

        # Stats
        level = self._get_table_row_value(soup, 'Level')
        rank = self._get_table_row_value(soup, 'Rank')
        
        if level:
            card_data['Level'] = level
        if rank:
            card_data['Rank'] = rank
            
        # Pendulum Scale
        pendulum_scale = self._get_table_row_value(soup, 'Pendulum Scale')
        if pendulum_scale:
            card_data['Pendulum Scale'] = pendulum_scale
            
        # Link Arrows
        link_arrows_td = self._get_table_row_html(soup, 'Link Arrows')
        if link_arrows_td:
            arrows = []
            ul = link_arrows_td.find('ul')
            if ul:
                arrows = [li.text.strip() for li in ul.find_all('li')]
            if not arrows:
                arrows = [img.get('title') for img in link_arrows_td.find_all('img') if img.get('title')]
                
            if arrows:
                card_data['Link Arrows'] = arrows
                try: print(f"[{card_data.get('Name', 'Unknown')}] Extracted Link Arrows: {arrows}")
                except UnicodeEncodeError: pass

        # ATK / DEF or ATK / LINK robust extraction
        atk_def = ""
        atk_link = ""
        for th in soup.find_all('th'):
            text = th.text.upper()
            if 'ATK' in text and 'DEF' in text:
                td = th.find_next_sibling('td')
                if td: atk_def = re.sub(r'\s+', ' ', td.text).strip()
            elif 'ATK' in text and 'LINK' in text:
                td = th.find_next_sibling('td')
                if td: atk_link = re.sub(r'\s+', ' ', td.text).strip()
                
        if atk_def:
            parts = [p.strip() for p in atk_def.split('/')]
            if len(parts) >= 2:
                card_data['ATK'] = parts[0]
                card_data['DEF'] = parts[1]
            else:
                card_data['ATK'] = atk_def
                card_data['DEF'] = ""
        elif atk_link:
            parts = [p.strip() for p in atk_link.split('/')]
            if len(parts) >= 2:
                card_data['ATK'] = parts[0]
                match = re.search(r'\d+', parts[1])
                link_val = match.group() if match else parts[1]
                card_data['LINK'] = link_val
                card_data['Link Rating'] = link_val
                card_data['DEF'] = ""
                try: print(f"[{card_data.get('Name', 'Unknown')}] Extracted Link Stats -> ATK: {parts[0]} | LINK: {link_val}")
                except UnicodeEncodeError: pass
            else:
                card_data['ATK'] = atk_link
                card_data['DEF'] = ""
                
        # Link Rating fallback
        if 'Link Rating' not in card_data:
            lr = self._get_table_row_value(soup, 'Link Rating')
            if lr:
                match = re.search(r'\d+', lr)
                link_val = match.group() if match else lr
                card_data['Link Rating'] = link_val
                card_data['LINK'] = link_val

        # Lore / Effects
        lore_div = soup.find('div', class_='lore')
        if lore_div:
            # Check if there are definition lists (dt/dd) typically used for Pendulum cards or Rush format info
            dls = lore_div.find_all('dl', recursive=False)
            if not dls:
                dls = lore_div.find_all('dl')
                
            lore_dict = {}
            if dls:
                for dl in dls:
                    for dt in dl.find_all('dt', recursive=False):
                        dd = dt.find_next_sibling('dd')
                        if dd:
                            lore_dict[dt.text.strip()] = self._extract_text(dd)
            
            if lore_dict:
                card_data['Lore'] = lore_dict
            else:
                # Normal/Effect monster simple lore text
                card_data['Lore'] = self._extract_text(lore_div)
                
        # Card Set Information
        sets = []
        set_containers = soup.find_all('div', class_='switcher-container-sets')
        for container in set_containers:
            # Each 'switcher-container-sets' has child divs representing different languages/regions
            langs = container.find_all('div', recursive=False)
            for lang_div in langs:
                lang_name = "Unknown"
                # Find the language name typically wrapped in <p><b>Language</b></p>
                p_b = lang_div.find('p')
                if p_b and p_b.b:
                    lang_name = p_b.b.text.strip()
                
                table = lang_div.find('table', class_='card-list')
                if table:
                    trs = table.find_all('tr')
                    if trs:
                        headers = []
                        for tr in trs:
                            ths = tr.find_all('th')
                            if ths:
                                headers = [th.text.strip() for th in ths]
                                break
                        
                        if headers:
                            for tr in trs:
                                tds = tr.find_all('td')
                                if len(tds) == len(headers):
                                    set_info = {}
                                    for i in range(len(headers)):
                                        set_info[headers[i]] = ', '.join(tds[i].stripped_strings)
                                    set_info['Region/Language'] = lang_name
                                    sets.append(set_info)
        
        if sets:
            card_data['Sets'] = sets

        # Localized Data Parsing (Names + Card Texts)
        localized_data = {}
        
        # 1. Pre-populate Asian name sub-variations (Kana, Romaji, Base) from DL/DT/DD blocks
        #    These appear separately on the page outside the main language table
        for dl in soup.find_all('dl'):
            dts = dl.find_all('dt')
            dds = dl.find_all('dd')
            if len(dts) == len(dds) and len(dts) > 0:
                main_lang = dts[0].text.strip()
                if main_lang in ['Japanese', 'Korean', 'Simplified Chinese', 'Traditional Chinese']:
                    lang_dict = {}
                    for i in range(1, len(dts)):
                        lang_dict[dts[i].text.strip()] = dds[i].text.strip()
                    localized_data[main_lang] = lang_dict

        # 2. Parse the "In other languages" table rows for Name (TD[0]) and Card Text (TD[1])
        #    STRICT SCOPING: Only parse rows from the table under the "Other_languages" heading.
        #    Stop if we hit the "In_other_media" section.
        lang_whitelist = ['Japanese', 'French', 'German', 'Italian', 'Portuguese', 'Spanish', 'Korean', 'Simplified Chinese', 'Traditional Chinese']
        asian_languages = {'Japanese', 'Korean', 'Simplified Chinese', 'Traditional Chinese'}
        current_lang = None  # Track current language for rowspan sub-rows
        sub_row_idx = 0      # Track which sub-row we're on
        
        # Find the 'Other languages' heading to scope our search
        other_lang_heading = soup.find('span', id='Other_languages')
        if other_lang_heading:
            # Get the parent heading element, then find the next table
            lang_section = other_lang_heading.find_parent(['h2', 'h3'])
            if lang_section:
                # Collect all <tr> rows ONLY from tables between this heading and the next h2
                scoped_trs = []
                for sibling in lang_section.find_next_siblings():
                    # Stop at the next h2 heading (e.g., "In other media")
                    if sibling.name == 'h2':
                        break
                    if sibling.name == 'table' or (hasattr(sibling, 'find_all')):
                        scoped_trs.extend(sibling.find_all('tr'))
            else:
                scoped_trs = []
        else:
            scoped_trs = []
        
        for tr in scoped_trs:
            th = tr.find('th', scope='row')
            
            if th:
                lang_text = th.text.strip()
                if lang_text in lang_whitelist:
                    current_lang = lang_text
                    sub_row_idx = 0
                    rowspan = th.get('rowspan')
                    
                    tds = tr.find_all('td')
                    if not tds:
                        continue
                    
                    if current_lang not in localized_data:
                        localized_data[current_lang] = {}
                    
                    # TD[0] = Localized Name
                    name_val = re.sub(r'\s+', ' ', tds[0].text).strip()
                    if 'Name' not in localized_data[current_lang]:
                        localized_data[current_lang]['Name'] = name_val
                    
                    # TD[1] = Card Text (if exists)
                    card_text_td = tds[1] if len(tds) >= 2 else None
                else:
                    current_lang = None
                    continue
            elif current_lang:
                # This is a sub-row (rowspan continuation) for the current language
                # Only extract Name_2/Name_3 for Asian languages
                sub_row_idx += 1
                tds = tr.find_all('td')
                if tds:
                    if current_lang in asian_languages:
                        sub_name = re.sub(r'\s+', ' ', tds[0].text).strip()
                        if sub_name:
                            localized_data[current_lang][f'Name_{sub_row_idx + 1}'] = sub_name
                    # DO NOT capture card_text_td from sub-rows!
                    # Sub-row 3 typically contains the English translation of the
                    # Asian text, which would overwrite the native lore.
                    card_text_td = None
                else:
                    continue
            else:
                continue
            
            # Process card text TD (shared logic — but only from main row, not sub-rows)
            if card_text_td:
                # Skip if lore was already populated for this language (prevent overwrite)
                if 'Lore' in localized_data.get(current_lang, {}):
                    pass
                else:
                    dls = card_text_td.find_all('dl')
                    
                    if dls:
                        # Pendulum card: DL contains DT/DD pairs for Pendulum Effect / Monster Effect
                        for dl in dls:
                            dt_list = dl.find_all('dt')
                            for dt in dt_list:
                                dd = dt.find_next_sibling('dd')
                                if dd:
                                    dt_label = dt.text.strip().lower()
                                    dd_text = self._extract_text(dd)
                                    
                                    # Detect Pendulum vs Monster effect by keyword in any language
                                    pendulum_keywords = ['pendul', 'pendel', 'péndulo', 'pêndulo', 'ｐ効果', '펜듈럼']
                                    is_pendulum_text = any(kw in dt_label for kw in pendulum_keywords)
                                    
                                    if is_pendulum_text:
                                        if 'Pendulum_Lore' not in localized_data[current_lang]:
                                            localized_data[current_lang]['Pendulum_Lore'] = dd_text
                                    else:
                                        if 'Lore' not in localized_data[current_lang]:
                                            localized_data[current_lang]['Lore'] = dd_text
                    else:
                        # Standard card: plain text lore
                        lore_text = self._extract_text(card_text_td)
                        if lore_text:
                            localized_data[current_lang]['Lore'] = lore_text

        if localized_data:
            card_data['Localized_Data'] = localized_data
            # Log which languages have name vs full text
            langs_with_lore = [l for l, d in localized_data.items() if 'Lore' in d]
            try: print(f"[{card_data.get('Name', 'Unknown')}] Localized Data: {list(localized_data.keys())} | Lore found for: {langs_with_lore}")
            except UnicodeEncodeError: pass
        
        return card_data


if __name__ == "__main__":
    parser = YugipediaParser()
    test_url = "https://yugipedia.com/wiki/Abyss_Actor_-_Mellow_Madonna"
    print(f"Testing Yugipedia Parser on: {test_url}")
    card_data = parser.parse_card(test_url)
    with open('parser_test.json', 'w', encoding='utf-8') as f:
        json.dump(card_data, f, indent=4, ensure_ascii=False)
    print("Saved output to parser_test.json")
