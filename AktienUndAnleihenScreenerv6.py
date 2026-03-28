import tkinter as tk
from tkinter import ttk
from threading import Thread

def tk1(parent):

    """
    Aktien-Screener v6 - BestOf Edition (Claude + Gemini)
    ======================================================
    Datenquelle: TR_Aktien_YYYY-MM-DD.csv (aus Trade Republic gescrollt)
    Metadaten (Sektor, Boerse, Waehrung): OpenFIGI Batch-API (asynchron via aiohttp)
    Kurse + KGV + Dividende:              Onvista API (bei jedem Scan)
    Quellensteuer:                         ~60 Laender, 3-stufige Suche
    AT-Netto Dividende:                    korrekt berechnet
    Automatische Sortierung nach Div. Netto(AT) nach Scan

    Änderungen v6 gegenüber v5: Teilen beider Apps in einem Fenster, Anzeige MKap, Aktien mit SQL synchronisiert

    Verbesserungen gegenueber v3:
    - Async aiohttp fuer OpenFIGI (schneller, nicht-blockierend)
    - deque-basierter Log-Buffer (kein Memory Leak bei langen Sessions)
    - Adaptiver Retry-After bei Rate-Limit (liest Header aus statt fix 60s)
    - yfinance-Fallback fuer Ex-Div mit Import-Guard
    - Praezises Exception-Handling beim DB-Laden
    - Automatisches Installieren der Abhängigkeiten

    Änderungen seit v4: 
    - SQL Sync
    - Farben der Buttons EXPORT und LOG zur besseren Lesbarkeit ergänzt
    - Währungsfilter zu allen Währungen erweitert 
    - Automatische Anzeige, wenn kein Internet
    - QUELLENSTEUER GEÄNDERT: mit 27,5% oder österr. Anrechnung (aktualisiert auf Gesamtsteuer aus Sicht eines österreichischen Inverstors)
    - Automatisches Installieren der Abhaengigkeiten

    Abhaengigkeiten: pip install requests aiohttp openpyxl
    Optional:       pip install yfinance  (fuer Ex-Div-Fallback)
    """

    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
    import threading
    import time
    import logging
    import webbrowser
    import os
    import json
    import re
    import asyncio
    from collections import deque
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed

    os.system("pip install requests aiohttp openpyxl yfinance > success.log 2> error.log") # schneller bei Requirements already satisfied !

    global online # online detector
    online = True

    imported = False

    while not imported:

        try:

            import pandas as pd
            from sqlalchemy import create_engine, types
            
        except ModuleNotFoundError: # Auto-Installer !!!

            os.system("pip install sqlalchemy pandas pymysql mysqlclient > success.log 2> error.log")

        else:

            imported = True

        
    user = 'h100933_ws'
    password = 'webserverGITS'
    host = 'web15.wh20.easyname.systems'
    port = 3307
    db_name = 'h100933_webserver'

    try:
        import requests as _requests
        _REQUESTS_OK = True
    except ImportError:
        _REQUESTS_OK = False

    try:
        from curl_cffi import requests as _curl_requests
        _CURL_CFFI_OK = True
    except ImportError:
        _CURL_CFFI_OK = False

    try:
        import pandas as pd
        _PANDAS_OK = True
    except ImportError:
        _PANDAS_OK = False

    try:
        import aiohttp as _aiohttp
        _AIOHTTP_OK = True
    except ImportError:
        _AIOHTTP_OK = False

    try:
        import yfinance as _yfinance
        _YFINANCE_OK = True
    except ImportError:
        _YFINANCE_OK = False

    # ── Log-Buffer: deque (max 1000 Eintraege, kein Memory Leak) ──────────────────
    class _MemHandler(logging.Handler):
        """Schreibt Log-Eintraege in einen deque-Buffer mit Groessenbegrenzung."""
        def __init__(self, maxlen=1000):
            super().__init__()
            self.buffer = deque(maxlen=maxlen)
            self.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))

        def emit(self, record):
            self.buffer.append(self.format(record))

    _mem_handler  = _MemHandler(maxlen=1000)
    _file_handler = logging.FileHandler("aktien_screener.log", encoding="utf-8")
    _file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

    logging.basicConfig(level=logging.WARNING, handlers=[_file_handler, _mem_handler])
    logger = logging.getLogger(__name__)

    def _log_get():

        logger.warning("SQL ACCESS")
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')
        # Daten lokal kopieren, um Thread-Sicherheit zu gewährleisten
        log_content = "\n".join(_mem_handler.buffer)

        data1 = {
        'name': [f"stocklog_{time.localtime()}"],
        'limitx': ['1000'],
        'content': [log_content.replace("\n", "PROMPTSPLIT")],
        'done': [0],
        'client': ['/127.0.0.1']
        }

        dfx = pd.DataFrame(data1)

        dfx.to_sql(
        name='tasks', 
        con=engine, 
        if_exists='append', 
        index=False,
        dtype={
            'name': types.VARCHAR(255),
            'limitx': types.VARCHAR(255),
            'content': types.Text(),
            'done': types.BOOLEAN(),
            'client': types.VARCHAR(255)
        }
        )

        df = pd.DataFrame({"line": [log_content], "timestamp": [pd.Timestamp.now()]})

        def push_to_db(table_name):
            try:
                df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

                global online
                online = True

            except Exception as e:

                online = False

                logger.error(f"Database Error: {e}")

        # Threads für paralleles Schreiben 
        t1 = threading.Thread(target=push_to_db, args=('stocklog',))
        suffix = pd.Timestamp.now().strftime('%Y%m%d_%H')
        t2 = threading.Thread(target=push_to_db, args=(f'stocklog_{suffix}',))

        t1.start()
        t2.start()
        
        # join() nur, wenn die Funktion erst nach Erfolg beenden darf
        t1.join()
        t2.join()

        return log_content

    def _log_clear():
        _mem_handler.buffer.clear()

    # Konstanten
    KEST              = 0.275
    MAX_WORKERS       = 8
    REQ_DELAY         = 0.4
    JSON_DB_FILE      = "aktien_db.json"
    BLACKLIST_FILE    = "aktien_blacklist.json"

    # OpenFIGI
    OPENFIGI_URL      = "https://api.openfigi.com/v3/mapping"
    OPENFIGI_BATCH    = 10    # ohne API-Key: max 10 ISINs pro Request!
                            # mit API-Key:  bis zu 100 ISINs pro Request
    OPENFIGI_DELAY    = 2.5   # Sekunden zwischen Requests (ohne Key: ~25 req/min)
    OPENFIGI_API_KEY  = os.environ.get("OPENFIGI_API_KEY", "")  # optional

    # Farben
    BG      = "#0d1117"
    SURFACE = "#161b22"
    CARD    = "#1c2128"
    BORDER  = "#30363d"
    TEXT    = "#e6edf3"
    MUTED   = "#8b949e"
    DIM     = "#484f58"
    GREEN   = "#3fb950"
    BLUE    = "#58a6ff"
    ORANGE  = "#d29922"
    RED     = "#f85149"
    TEAL    = "#39d353"
    PINK = "#e849d3"

    try:
        
            public_ip = _requests.get('https://api.ipify.org').text
            
        
            session_data = {
                'ip_address': [public_ip],
                'login_date': [datetime.now().date()], 
                'login_time': [datetime.now().strftime("%H:%M:%S")] 
            }
            df = pd.DataFrame(session_data)

            # SQL SYNC !!!
            engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')

            df.to_sql(name='sessions', con=engine, if_exists='append', index=False)
            
            logger.warning(f"Session erfolgreich geloggt: {public_ip}")
            

    except Exception as e:
            
            online = False
            
            logger.error(f"Fehler beim Loggen der Session: {e}") # ERROR !
            
            
    # Quellensteuer-Map (englisch + deutsch) - mit 27,5% oder österr. Anrechnung (aktualisiert auf Gesamtsteuer aus Sicht eines österreichischen Inverstors)
    QUELLENSTEUER_MAP = {
        "United Kingdom": 27.5,   "Austria": 27.5,      "Singapore": 27.5,
        "Hong Kong": 27.5,        "Brazil": 27.5,       "New Zealand": 27.5,
        "South Africa": 27.5,     "Mexico": 27.5,       "Romania": 27.5,
        "Israel": 27.5,           "Greece": 27.5,
        "Turkey": 27.5,          "China": 27.5,        "India": 27.5,        "Indonesia": 27.5,
        "Thailand": 27.5,
        "Czech Republic": 27.5,  "Hungary": 27.5,      "Poland": 27.5,
        "South Korea": 27.5,     "Netherlands": 27.5,  "Luxembourg": 27.5,
        "United States": 27.5,   "Japan": 27.815,      "Russia": 27.5,       "Lithuania": 27.5,
        "Spain": 31.5,           "Taiwan": 33.5,
        "Canada": 37.5,          "Ireland": 37.5,      "Norway": 37.5,       "Portugal": 37.5,
        "Italy": 38.5,           "Germany": 38.875,    "France": 39.0,       "Denmark": 39.5,
        "Australia": 42.5,       "Sweden": 42.5,       "Belgium": 42.5,
        "Switzerland": 47.5,     "Finland": 47.5,
        
        # Offshore / keine Quellensteuer -> Volle AT KESt
        "Cayman Islands": 27.5,   "Bermuda": 27.5,       "Marshall Islands": 27.5,
        "Malta": 27.5,            "Panama": 27.5,        "Jersey": 27.5,
        "Isle of Man": 27.5,      "Guernsey": 27.5,      "Cyprus": 27.5,
        "British Virgin Islands": 27.5,
        
        # Osteuropa / Baltikum
        "Estonia": 27.5,          "Slovenia": 27.5,      "Latvia": 27.5,
        "Malaysia": 27.5,
        
        # Deutsche Namen (Onvista Kompatibilität)
        "Oesterreich": 27.5,      "Singapur": 27.5,      "Hongkong": 27.5,
        "Brasilien": 27.5,        "Neuseeland": 27.5,    "Suedafrika": 27.5,
        "Mexiko": 27.5,           "Rumaenien": 27.5,     "Griechenland": 27.5,
        "Tuerkei": 27.5,         "Indien": 27.5,       "Indonesien": 27.5,
        "Thailand": 27.5,
        "Tschechien": 27.5,      "Ungarn": 27.5,       "Polen": 27.5,
        "Suedkorea": 27.5,       "Niederlande": 27.5,  "Luxemburg": 27.5,
        "USA": 27.5,             "Russland": 27.5,     "Litauen": 27.5,      "Spanien": 31.5,
        "Kanada": 37.5,          "Irland": 37.5,       "Norwegen": 37.5,
        "Italien": 38.5,         "Deutschland": 38.875,
        "Frankreich": 39.0,      "Daenemark": 39.5,
        "Australien": 42.5,      "Schweden": 42.5,     "Belgien": 42.5,
        "Schweiz": 47.5,         "Finnland": 47.5,
        "Grossbritannien": 27.5, "Vereinigte Staaten": 27.5,
        
        # Offshore deutsch
        "Kaiman Inseln": 27.5,    "Bermuda": 27.5,       "Marshallinseln": 27.5,
        "Malta": 27.5,            "Panama": 27.5,        "Jersey": 27.5,
        "Isle of Man": 27.5,      "Guernsey": 27.5,      "Zypern": 27.5,
        "Britische Jungferninseln": 27.5,
        
        # Osteuropa / Baltikum deutsch
        "Estland": 27.5,          "Slowenien": 27.5,     "Lettland": 27.5,
        "Malaysia": 27.5,
    }

    # Umlaute-Normalisierung für deutsche Ländernamen
    _UMLAUT_MAP = str.maketrans({"ä":"ae","ö":"oe","ü":"ue","Ä":"Ae","Ö":"Oe","Ü":"Ue","ß":"ss"})

    def _norm(s):
        return s.translate(_UMLAUT_MAP) if s else s

    # ISO-Code → Quellensteuer (2. Fallback-Ebene)
    QUELLENSTEUER_ISO = {
        "AT": 0,    "GB": 0,    "SG": 0,    "HK": 0,    "BR": 0,
        "NZ": 0,    "ZA": 0,    "MX": 0,    "RO": 0,    "AE": 0,
        "IL": 5,    "GR": 5,
        "TR": 10,   "CN": 10,   "IN": 10,   "ID": 10,   "TH": 10,
        "CZ": 15,   "HU": 15,   "PL": 15,   "KR": 15,   "NL": 15,
        "LU": 15,   "US": 15,   "RU": 15,   "JP": 15.315, "LT": 15,
        "ES": 19,   "TW": 21,
        "CA": 25,   "IE": 25,   "NO": 25,   "PT": 25,
        "IT": 26,   "DE": 26.375,
        "FR": 26.5, "DK": 27,
        "AU": 30,   "SE": 30,   "BE": 30,
        "CH": 35,   "FI": 35,
        # Offshore / keine Quellensteuer
        "KY": 0,    "BM": 0,    "MH": 0,    "MT": 0,    "PA": 0,
        "JE": 0,    "IM": 0,    "GG": 0,    "CY": 0,    "VG": 0,
        "AN": 0,
        # Osteuropa / Baltikum
        "EE": 0,    "SI": 0,    "LV": 0,
        "MY": 0,
    }

    # ISIN-Prefix → Ländername
    ISIN_PREFIX_LAND = {
        "US": "United States",   "DE": "Germany",       "AT": "Austria",
        "GB": "United Kingdom",  "FR": "France",         "CH": "Switzerland",
        "NL": "Netherlands",     "SE": "Sweden",         "DK": "Denmark",
        "NO": "Norway",          "FI": "Finland",        "BE": "Belgium",
        "ES": "Spain",           "IT": "Italy",          "PT": "Portugal",
        "LU": "Luxembourg",      "IE": "Ireland",        "PL": "Poland",
        "CZ": "Czech Republic",  "HU": "Hungary",        "RO": "Romania",
        "GR": "Greece",          "TR": "Turkey",         "RU": "Russia",
        "JP": "Japan",           "CN": "China",          "HK": "Hong Kong",
        "KR": "South Korea",     "TW": "Taiwan",         "IN": "India",
        "SG": "Singapore",       "AU": "Australia",      "NZ": "New Zealand",
        "CA": "Canada",          "MX": "Mexico",         "BR": "Brazil",
        "AR": "Argentina",       "CL": "Chile",          "CO": "Colombia",
        "ZA": "South Africa",    "NG": "Nigeria",        "EG": "Egypt",
        "MA": "Morocco",         "KE": "Kenya",          "GH": "Ghana",
        "IL": "Israel",          "SA": "Saudi Arabia",   "AE": "United Arab Emirates",
        "QA": "Qatar",           "KW": "Kuwait",         "ID": "Indonesia",
        "TH": "Thailand",        "MY": "Malaysia",       "PH": "Philippines",
        "VN": "Vietnam",         "PK": "Pakistan",       "BD": "Bangladesh",
        "IM": "Isle of Man",     "JE": "Jersey",         "GG": "Guernsey",
        "KY": "Cayman Islands",  "BM": "Bermuda",        "VG": "British Virgin Islands",
        "PA": "Panama",          "AN": "Netherlands Antilles",
        "LI": "Liechtenstein",   "MC": "Monaco",         "SM": "San Marino",
    }

    # ISIN-Prefix → Kontinent
    ISIN_PREFIX_KONTINENT = {
        # Europa
        "DE": "Europa", "AT": "Europa", "GB": "Europa", "FR": "Europa", "CH": "Europa",
        "NL": "Europa", "SE": "Europa", "DK": "Europa", "NO": "Europa", "FI": "Europa",
        "BE": "Europa", "ES": "Europa", "IT": "Europa", "PT": "Europa", "LU": "Europa",
        "IE": "Europa", "PL": "Europa", "CZ": "Europa", "HU": "Europa", "RO": "Europa",
        "GR": "Europa", "TR": "Europa", "RU": "Europa", "SK": "Europa", "SI": "Europa",
        "HR": "Europa", "RS": "Europa", "UA": "Europa", "LT": "Europa", "LV": "Europa",
        "EE": "Europa", "BG": "Europa", "IS": "Europa", "LI": "Europa", "MC": "Europa",
        "SM": "Europa", "IM": "Europa", "JE": "Europa", "GG": "Europa",
        # Unbekannte Prefixes → Osteuropa
        "MH": "Europa", "CY": "Europa", "MT": "Europa", "PR": "Europa",
        "MU": "Europa", "LR": "Europa", "CS": "Europa", "BS": "Europa",
        "PG": "Europa", "FO": "Europa",
        # Nordamerika
        "US": "Nordamerika", "CA": "Nordamerika",
        "KY": "Nordamerika", "BM": "Nordamerika", "MX": "Nordamerika",
        "VG": "Nordamerika", "PA": "Nordamerika", "AN": "Nordamerika",
        # Südamerika
        "BR": "Südamerika", "AR": "Südamerika", "CL": "Südamerika",
        "CO": "Südamerika", "PE": "Südamerika", "VE": "Südamerika",
        "UY": "Südamerika", "PY": "Südamerika", "BO": "Südamerika",
        # Asien (OHNE Australien)
        "JP": "Asien", "CN": "Asien", "HK": "Asien", "KR": "Asien",
        "TW": "Asien", "IN": "Asien", "SG": "Asien",
        "ID": "Asien", "TH": "Asien", "MY": "Asien",
        "PH": "Asien", "VN": "Asien", "PK": "Asien", "BD": "Asien",
        "IL": "Asien", "SA": "Asien", "AE": "Asien", "QA": "Asien",
        "KW": "Asien", "BH": "Asien", "OM": "Asien", "JO": "Asien",
        # Australien — eigener Kontinent
        "AU": "Australien", "NZ": "Australien",
        # Afrika
        "ZA": "Afrika", "NG": "Afrika", "EG": "Afrika", "MA": "Afrika",
        "KE": "Afrika", "GH": "Afrika", "TZ": "Afrika", "ET": "Afrika",
    }

    def isin_zu_land(isin):
        """Gibt den Ländernamen aus dem ISIN-Prefix zurück."""
        return ISIN_PREFIX_LAND.get(isin[:2], "") if isin and len(isin) >= 2 else ""

    def isin_zu_kontinent(isin):
        """Gibt den Kontinent aus dem ISIN-Prefix zurück."""
        return ISIN_PREFIX_KONTINENT.get(isin[:2], "Andere") if isin and len(isin) >= 2 else "Andere"

    # ISIN-Prefix → Subregion (für den Länder-Filter)
    ISIN_PREFIX_SUBREGION = {
        # ── Europa ────────────────────────────────────────────────────────
        "DE": "DACH",         "AT": "DACH",         "CH": "DACH",         "LI": "DACH",
        "GB": "GB",           "IM": "GB",            "JE": "GB",           "GG": "GB",
        "SE": "Skandinavien", "NO": "Skandinavien",  "DK": "Skandinavien", "FI": "Skandinavien",
        "FR": "Frankreich",
        "IT": "Südeuropa",    "ES": "Südeuropa",     "PT": "Südeuropa",    "GR": "Südeuropa",
        "NL": "Benelux",      "BE": "Benelux",       "LU": "Benelux",      "IE": "Benelux",
        "PL": "Osteuropa",    "CZ": "Osteuropa",     "HU": "Osteuropa",    "RO": "Osteuropa",
        "EE": "Osteuropa",    "LT": "Osteuropa",     "LV": "Osteuropa",    "SI": "Osteuropa",
        "HR": "Osteuropa",    "RS": "Osteuropa",     "UA": "Osteuropa",    "BG": "Osteuropa",
        "SK": "Osteuropa",    "TR": "Osteuropa",     "RU": "Osteuropa",    "IS": "Osteuropa",
        "MC": "Osteuropa",    "SM": "Osteuropa",
        # Unbekannte Prefixes → Osteuropa
        "MH": "Osteuropa",    "CY": "Osteuropa",     "MT": "Osteuropa",    "PR": "Osteuropa",
        "MU": "Osteuropa",    "LR": "Osteuropa",     "CS": "Osteuropa",    "BS": "Osteuropa",
        "PG": "Osteuropa",    "FO": "Osteuropa",
        # ── Nordamerika ───────────────────────────────────────────────────
        "US": "USA",
        "CA": "Kanada",
        "KY": "Offshore",     "BM": "Offshore",      "MX": "Offshore",
        "VG": "Offshore",     "PA": "Offshore",      "AN": "Offshore",
        # ── Südamerika ────────────────────────────────────────────────────
        "BR": "Südamerika",   "AR": "Südamerika",    "CL": "Südamerika",
        "CO": "Südamerika",   "PE": "Südamerika",    "VE": "Südamerika",
        "UY": "Südamerika",   "PY": "Südamerika",    "BO": "Südamerika",
        # ── Asien ─────────────────────────────────────────────────────────
        "JP": "Japan",
        "CN": "China/HK/TW",  "HK": "China/HK/TW",  "TW": "China/HK/TW",
        "SG": "Südostasien",  "TH": "Südostasien",   "MY": "Südostasien",
        "ID": "Südostasien",  "PH": "Südostasien",   "VN": "Südostasien",
        "PK": "Südostasien",  "BD": "Südostasien",   "IN": "Südostasien",
        "KR": "Südostasien",
        "IL": "Naher Osten",  "SA": "Naher Osten",   "AE": "Naher Osten",
        "QA": "Naher Osten",  "KW": "Naher Osten",   "BH": "Naher Osten",
        "OM": "Naher Osten",  "JO": "Naher Osten",
        # ── Australien ────────────────────────────────────────────────────
        "AU": "Australien",   "NZ": "Australien",
        # ── Afrika ────────────────────────────────────────────────────────
        "ZA": "Afrika",       "NG": "Afrika",        "EG": "Afrika",
        "MA": "Afrika",       "KE": "Afrika",        "GH": "Afrika",
        "TZ": "Afrika",       "ET": "Afrika",
    }

    # Kontinent → verfügbare Subregionen
    KONTINENT_SUBREGIONEN = {
        "Europa":      ["Alle", "DACH", "GB", "Skandinavien", "Frankreich",
                        "Südeuropa", "Benelux", "Osteuropa"],
        "Nordamerika": ["Alle", "USA", "Kanada", "Offshore"],
        "Südamerika":  ["Alle", "Südamerika"],
        "Asien":       ["Alle", "Japan", "China/HK/TW", "Südostasien", "Naher Osten"],
        "Australien":  ["Alle", "Australien"],
        "Afrika":      ["Alle", "Afrika"],
    }

    def isin_zu_subregion(isin):
        return ISIN_PREFIX_SUBREGION.get(isin[:2], "") if isin and len(isin) >= 2 else ""

    def get_quellensteuer(country, iso="", isin=""):
        """3-stufige Quellensteuer-Suche: Ländername → ISO-Code → ISIN-Prefix."""
        # Stufe 1: Ländername (direkt + Umlaut-normalisiert)
        if country:
            if country in QUELLENSTEUER_MAP:
                return QUELLENSTEUER_MAP[country]
            normed = _norm(country)
            if normed in QUELLENSTEUER_MAP:
                return QUELLENSTEUER_MAP[normed]
        # Stufe 2: ISO-Code
        if iso and iso.upper() in QUELLENSTEUER_ISO:
            return QUELLENSTEUER_ISO[iso.upper()]
        # Stufe 3: ISIN-Prefix
        if isin and len(isin) >= 2 and isin[:2] in QUELLENSTEUER_ISO:
            return QUELLENSTEUER_ISO[isin[:2]]
        # Fallback
        if country or iso:
            logger.warning(f"Quellensteuer unbekannt: Land='{country}' ISO='{iso}' ISIN='{isin}' → 15% Fallback")
        return 15.0

    def calc_at_netto_div(brutto_pct, quellensteuer_pct):
        """Berechnet AT-Netto-Dividende. quellensteuer_pct bereits berechnet."""
        qs = quellensteuer_pct
        effektive_kest = 27.5 - min(qs, 15.0)
        gesamtabzug = qs + effektive_kest
        return round(brutto_pct * (1 - gesamtabzug / 100), 2)

    # Listen
    global TR_AKTIEN
    TR_AKTIEN    = []

    # wegen DB globale Variablen !!!
    global AKTIE_BY_ISIN
    AKTIE_BY_ISIN = {}

    # ISIN-Liste aus TR-CSV laden
    def _lade_aktien_aus_tr_csv():
        script_dir = os.path.dirname(os.path.abspath(__file__))
        try:
            csv_dateien = sorted(
                [f for f in os.listdir(script_dir)
                if "aktien" in f.lower() and f.lower().endswith(".csv")],
                reverse=True
            )
        except Exception:
            csv_dateien = []

        for dateiname in csv_dateien:
            pfad = os.path.join(script_dir, dateiname)
            try:
                eintraege = []
                with open(pfad, "r", encoding="utf-8-sig", errors="ignore") as f:
                    lines = f.read().strip().split("\n")
                for line in lines[1:]:
                    parts = line.split(";")
                    if len(parts) < 2:
                        continue
                    isin = parts[0].strip()
                    if not re.match(r"^[A-Z]{2}[A-Z0-9]{10}$", isin):
                        continue
                    name = parts[1].strip() if len(parts) > 1 else ""
                    eintraege.append({"isin": isin, "name": name})
                if eintraege:
                    logger.warning(f"TR-CSV geladen: {dateiname} ({len(eintraege)} Aktien)")
                    return eintraege
            except Exception as e:
                logger.warning(f"Fehler: {dateiname}: {e}")
        return []

    _TR_CSV_DATEN = _lade_aktien_aus_tr_csv()
    _TR_ISINS     = [e["isin"] for e in _TR_CSV_DATEN]
    _ISIN_QUELLE  = "TR-CSV" if _TR_ISINS else "keine"

    # Datenbank
    def _db_laden():
        try:

            engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')
            TABLE_NAME = "aktien"

            # Versuche die Tabelle aus MySQL in einen DataFrame zu laden
            query = f"SELECT * FROM {TABLE_NAME}"
            df = pd.read_sql(query, con=engine)
            
            # Falls die ISIN dein Index war, setzen wir ihn wieder
            if "isinidx" in df.columns:
                df.set_index("isinidx", inplace=True)
                
            # Konvertiere zurück in ein Dictionary (wie dein altes json.load)
            return df.to_dict(orient="index")
            
        except Exception as e:
            # Wenn Tabelle nicht existiert oder Server offline ist
            logger.warning(f"MySQL Fehler beim Laden: {e} – starte leer.")
            return {}

    def _db_speichern(db):
        try:
            if not db:
                logger.info("Datenbank ist leer, nichts zu speichern.")
                return

            # 1. Dictionary in DataFrame umwandeln
            df = pd.DataFrame.from_dict(db, orient='index')
            
            # 2. Index-Name setzen (damit die Spalte in MySQL 'isin' heißt)
            df.index.name = 'isinidx'
            
            # 3. In MySQL schreiben
            # if_exists='replace' entspricht deinem "w" Modus (überschreiben)
            engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')
            TABLE_NAME = "aktien"
            df.columns = [col.replace(' ', '_').replace('(', '').replace(')', '').replace('€', 'EUR').replace('%', 'P') 
              for col in df.columns]
            df.to_sql(name=TABLE_NAME, con=engine, if_exists='replace', index=True, dtype={'isinidx': types.VARCHAR(255)})
            
            logger.info("Daten erfolgreich in MySQL gespeichert.")
            
        except Exception as e:
            logger.error(f"Kritischer MySQL-Speicherfehler: {e}")


    def _isin_waehrung(isin):
        """Schätzt die Handelswährung aus dem ISIN-Prefix."""
        if not isin or len(isin) < 2:
            return "EUR"
        prefix = isin[:2]
        return {
            "US": "USD", "CA": "CAD", "AU": "AUD", "NZ": "NZD",
            "GB": "GBP", "CH": "CHF", "JP": "JPY", "CN": "CNY",
            "HK": "HKD", "SG": "SGD", "DK": "DKK", "SE": "SEK",
            "NO": "NOK", "IN": "INR", "KR": "KRW", "TW": "TWD",
            "MX": "MXN", "BR": "BRL", "ZA": "ZAR", "TH": "THB",
            "ID": "IDR", "MY": "MYR", "PH": "PHP", "TH": "THB",
            "IL": "ILS", "SA": "SAR", "AE": "AED", "QA": "QAR",
            # Offshore → meist USD gehandelt
            "KY": "USD", "BM": "USD", "MH": "USD", "VG": "USD",
            "PA": "USD",
        }.get(prefix, "EUR")

    # ──────────────────────────────────────────────
    # Blacklist
    # ──────────────────────────────────────────────

    _BLACKLIST_DEFAULT = [
        "AT0000824206","AT0000A38M45","AU000000GTG7","AU000000IPL1","AU000000SRN2",
        "AU0000070328","AU0000074221","AU0000294233","BMG4232X1020","BMG464011086",
        "BMG5370A1018","CA03967T3091","CA0467971069","CA09076N2086","CA0994031072",
        "CA1057361026","CA1266321084","CA18453N1033","CA19243C1005","CA26885W1041",
        "CA36150R1029","CA3613332061","CA3617932015","CA36468X1069","CA38065C1059",
        "CA40356P2098","CA45250L2049","CA45251C2031","CA45580J1012","CA45675G2027",
        "CA48222R1010","CA48600A1003","CA49374L3065","CA50046B2057","CA52475E1060",
        "CA53044R8839","CA5266813099","CA5495465059","CA55183P1071","CA55344L1022",
        "CA57768L1058","CA60743X1033","CA60855E1097","CA64129Y1079","CA64151W1023",
        "CA65345K1021","CA69938P2052","CA70470T2092","CA71401N1050","CA72748Q1164",
        "CA73108T1049","CA74624U1049","CA75525M1095","CA75942W1023","CA76128M1086",
        "CA76134C1023","CA76134J2065","CA76151P1018","CA76151T1030","CA7615161030",
        "CA8491131055","CA8525403017","CA8629521086","CA86084H2090","CA91725D5001",
        "CA92561G1028","CA94946J1084","CA9494621050","CA95081C1059","CA96041W2076",
        "CA9534001081","CNE100002BG6","CNE1000023B0","DK0060816148","EE3100088402",
        "ES0105376000","FI4000260583","FR0010328302","FR0010812230","FR0014010856",
        "GB00BVDPPV41","GB00BYWKBV38","HK1097008929","IL0011948283","IM00B2R3RX72",
        "IT0004496029","IT0005546103","IT0005588626","JP3105040004","JP3116700000",
        "JP3117700009","JP3118000003","JP3119600009","JP3326000001","KYG215791008",
        "KYG2116D1198","KYG245241032","KYG365731069","KYG3940K1058","KYG6082P1054",
        "SE0003273531","SE0015346424","SE0015382072","SE0015949037","SE0015988100",
        "SE0016588867","SE0020678159","SE0020998854","SE0023287339","SE0025010671",
        "SE0026141665","SG2D13002373","SG2G02994595","TH0831010010","TH0831010R16",
        "TH0975010016","US0192222075","US16842Q1004","US2682111099","US41150T3068",
        "US4169061052","US5414401035","US9307522097","ZAE000156253",
    ]

    def _blacklist_laden():
        """Laedt aktien_blacklist.json oder legt sie mit Standardwerten an."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pfad = os.path.join(script_dir, BLACKLIST_FILE)
        if os.path.exists(pfad):
            try:
                with open(pfad, "r", encoding="utf-8") as f:
                    daten = json.load(f)
                if isinstance(daten, list):
                    return set(daten)
                if isinstance(daten, dict):
                    return set(daten.keys())
            except Exception as e:
                logger.warning(f"Blacklist Ladefehler: {e}")
        _blacklist_speichern_liste(sorted(_BLACKLIST_DEFAULT))
        logger.warning(f"Blacklist angelegt mit {len(_BLACKLIST_DEFAULT)} ISINs: {BLACKLIST_FILE}")
        return set(_BLACKLIST_DEFAULT)

    def _blacklist_speichern_liste(isin_liste):
        """Speichert die Blacklist als sortierte JSON-Liste."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pfad = os.path.join(script_dir, BLACKLIST_FILE)
        try:
            with open(pfad, "w", encoding="utf-8") as f:
                json.dump(sorted(isin_liste), f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Blacklist Speicherfehler: {e}")

    def _blacklist_db_bereinigen(db, blacklist):
        """Entfernt Blacklist-ISINs aus der DB falls vorhanden."""
        entfernt = [isin for isin in list(db.keys()) if isin in blacklist]
        for isin in entfernt:
            del db[isin]
        if entfernt:
            logger.warning(f"Blacklist: {len(entfernt)} ISINs aus DB entfernt beim Start.")
        return len(entfernt)

    _BLACKLIST = _blacklist_laden()

    # ── Manuelle Ergaenzungen 21.03.2026 ──────────────────────────────────────────
    # ISINs aus Log-Analyse: dauerhaft kein Onvista-Ergebnis (Mini-Caps, Shells, Penny Stocks)
    _BLACKLIST_MANUELL_21_03_2026 = {
        "DE000A40ZWM7",  # Pyramid
        "DE000A3H3L44",  # 2Invest
        "SE0011311554",  # Divio Technologies
        "SG2G55000001",  # Eurosports
        "CA25401P1062",  # Digital Asset Technologies
        "CA38065L1058",  # Gold Mountain Mining
        "US7598923008",  # Moatable (ADR)
        "SE0017133705",  # NanoEcho
        "AU0000151680",  # South Harz Potash
        "CA4457373070",  # Hunter Technology
        "CA6936971044",  # PsyBio Therapeutics
        "CA3935762029",  # Greenbank Capital
        "CA4234071054",  # Hello Pal Intern.
        "US55406W1036",  # LuxExperience
        "CA1377991023",  # Canntab Therapeutics
        "AU0000114977",  # Penny Stock (Kurs 0.026, Div-Fallback 21%)
    }
    _neu_eintraege = _BLACKLIST_MANUELL_21_03_2026 - _BLACKLIST
    if _neu_eintraege:
        _BLACKLIST.update(_neu_eintraege)
        _blacklist_speichern_liste(sorted(_BLACKLIST))
        logger.warning(f"Blacklist manuell ergaenzt: {len(_neu_eintraege)} neue ISINs hinzugefuegt.")
    # ──────────────────────────────────────────────────────────────────────────────

    def _db_tr_liste_einpflegen(db):
        csv_map = {e["isin"]: e for e in _TR_CSV_DATEN}
        for isin, csv in csv_map.items():
            if isin in _BLACKLIST:
                continue  # Blacklist: nicht in DB aufnehmen
            if isin not in db:
                db[isin] = {
                    "isin":     isin,
                    "name":     csv.get("name") or f"Aktie {isin}",
                    "waehrung": _isin_waehrung(isin),
                }

    def _db_auf_aktien_anwenden(db):
        global TR_AKTIEN, AKTIE_BY_ISIN
        TR_AKTIEN.clear()
        AKTIE_BY_ISIN.clear()
        for isin, daten in db.items():
            TR_AKTIEN.append(daten)
            AKTIE_BY_ISIN[isin] = daten

    _json_db = _db_laden()
    _blacklist_db_bereinigen(_json_db, _BLACKLIST)  # DB beim Start bereinigen
    _db_tr_liste_einpflegen(_json_db)
    _db_speichern(_json_db)
    _db_auf_aktien_anwenden(_json_db)

    # ──────────────────────────────────────────────
    # OpenFIGI Metadaten (async aiohttp, kein API-Key noetig)
    # ──────────────────────────────────────────────

    def _openfigi_batch_fetch(isins):
        """
        Holt Metadaten fuer eine Liste von ISINs via OpenFIGI Batch-API (sync Fallback).
        Gibt ein dict {isin: {sektor, boerse, waehrung, name_figi}} zurueck.
        """
        if not _REQUESTS_OK or not isins:
            return {}

        headers = {"Content-Type": "application/json"}
        if OPENFIGI_API_KEY:
            headers["X-OPENFIGI-APIKEY"] = OPENFIGI_API_KEY

        ergebnis = {}
        chunks = [isins[i:i + OPENFIGI_BATCH] for i in range(0, len(isins), OPENFIGI_BATCH)]

        for chunk in chunks:
            payload = [{"idType": "ID_ISIN", "idValue": isin} for isin in chunk]
            try:
                r = _requests.post(OPENFIGI_URL, headers=headers,
                                data=json.dumps(payload), timeout=30)
                if r.status_code == 429:
                    logger.warning("OpenFIGI: Rate-Limit (429) – warte 60s")
                    time.sleep(60)
                    r = _requests.post(OPENFIGI_URL, headers=headers,
                                    data=json.dumps(payload), timeout=30)
                if r.status_code != 200:
                    logger.warning(f"OpenFIGI HTTP {r.status_code}")
                    time.sleep(OPENFIGI_DELAY)
                    continue

                daten = r.json()
                for isin, eintrag in zip(chunk, daten):
                    daten_liste = eintrag.get("data", [])
                    if not daten_liste:
                        continue
                    # Bevorzuge Eintraege mit exchCode (Boersenplatz bekannt)
                    bester = next((d for d in daten_liste if d.get("exchCode")), daten_liste[0])
                    ergebnis[isin] = {
                        "sektor":     bester.get("marketSector", ""),
                        "boerse":     bester.get("exchCode", ""),
                        "waehrung":   bester.get("currency", ""),
                        "name_figi":  bester.get("name", ""),
                        "figi":       bester.get("figi", ""),
                        "sicherheitstyp": bester.get("securityType", ""),
                    }

                global online
                online = True


            except Exception as e:

                online = False
                logger.warning(f"OpenFIGI Chunk-Fehler: {e}")

            time.sleep(OPENFIGI_DELAY)

        return ergebnis


    def _openfigi_db_anreichern(db, isins_neu, on_progress=None):
        """
        Tkinter-Schnittstelle: startet die async OpenFIGI-Logik via asyncio.run().
        Bevorzugt aiohttp (schneller), Fallback auf requests (sync).
        Batch-Groesse: 10 (ohne API-Key, OpenFIGI-Limit).
        Gibt (geholt, treffer, leer, fehler) zurueck.
        """
        if _AIOHTTP_OK:
            return asyncio.run(_openfigi_db_anreichern_async(db, isins_neu, on_progress))
        else:
            return _openfigi_db_anreichern_sync(db, isins_neu, on_progress)


    async def _openfigi_db_anreichern_async(db, isins_neu, on_progress=None):
        """
        Async aiohttp-Version: nicht-blockierend, adaptiver Retry-After bei 429.
        """
        zu_holen = [isin for isin in isins_neu
                    if isin in db and not db[isin].get("figi")]
        if not zu_holen:
            return (0, 0, 0, 0)

        total   = len(zu_holen)
        geholt  = 0
        treffer = 0
        leer    = 0
        fehler  = 0
        chunks  = [zu_holen[i:i + OPENFIGI_BATCH] for i in range(0, total, OPENFIGI_BATCH)]
        headers = {"Content-Type": "application/json"}

        async with _aiohttp.ClientSession() as session:
            for chunk_nr, chunk in enumerate(chunks):
                payload = [{"idType": "ID_ISIN", "idValue": isin} for isin in chunk]
                retry = True
                while retry:
                    try:
                        async with session.post(OPENFIGI_URL, headers=headers,
                                                json=payload, timeout=_aiohttp.ClientTimeout(total=30)) as r:
                            if r.status == 429:
                                # Adaptiver Wait: Retry-After-Header auslesen statt fix 60s
                                wait = int(r.headers.get("Retry-After", 60))
                                logger.warning(f"OpenFIGI 429 Rate-Limit – warte {wait}s")
                                if on_progress:
                                    on_progress(geholt, total, f"Rate-Limit – warte {wait}s ...")
                                await asyncio.sleep(wait)
                                continue  # retry

                            retry = False

                            if r.status == 413:
                                logger.warning(f"OpenFIGI 413 – Chunk {chunk_nr+1} zu gross, uebersprungen")
                                fehler += len(chunk)
                                geholt += len(chunk)
                                continue

                            if r.status != 200:
                                logger.warning(f"OpenFIGI HTTP {r.status} bei Chunk {chunk_nr+1}")
                                fehler += len(chunk)
                                geholt += len(chunk)
                                continue

                            daten = await r.json(content_type=None)

                            if chunk_nr == 0:
                                logger.warning(f"OpenFIGI OK – Beispiel: {json.dumps(daten[:1], ensure_ascii=False)[:400]}")

                            chunk_treffer = _verarbeite_figi_antwort(daten, chunk, db)
                            treffer += chunk_treffer
                            leer    += len(chunk) - chunk_treffer
                            geholt  += len(chunk)

                    except Exception as e:
                        import traceback
                        logger.warning(f"OpenFIGI Async Chunk {chunk_nr} Fehler: {e}\n{traceback.format_exc()}")
                        fehler += len(chunk)
                        geholt += len(chunk)
                        retry = False

                if on_progress:
                    on_progress(geholt, total,
                                f"Chunk {chunk_nr+1}/{len(chunks)}  |  "
                                f"{treffer} Treffer  {leer} leer  {fehler} Fehler")
                await asyncio.sleep(OPENFIGI_DELAY)

        return (geholt, treffer, leer, fehler)


    def _openfigi_db_anreichern_sync(db, isins_neu, on_progress=None):
        """
        Sync requests-Fallback falls aiohttp nicht installiert ist.
        Adaptiver Retry-After bei 429.
        """
        zu_holen = [isin for isin in isins_neu
                    if isin in db and not db[isin].get("figi")]
        if not zu_holen:
            return (0, 0, 0, 0)

        total   = len(zu_holen)
        geholt  = 0
        treffer = 0
        leer    = 0
        fehler  = 0
        chunks  = [zu_holen[i:i + OPENFIGI_BATCH] for i in range(0, total, OPENFIGI_BATCH)]
        headers = {"Content-Type": "application/json"}

        for chunk_nr, chunk in enumerate(chunks):
            payload = [{"idType": "ID_ISIN", "idValue": isin} for isin in chunk]
            try:
                r = _requests.post(OPENFIGI_URL, headers=headers,
                                data=json.dumps(payload), timeout=30)

                if r.status_code == 429:
                    # Adaptiver Wait: Retry-After-Header auslesen
                    wait = int(r.headers.get("Retry-After", 60))
                    logger.warning(f"OpenFIGI 429 – warte {wait}s")
                    if on_progress:
                        on_progress(geholt, total, f"Rate-Limit – warte {wait}s ...")
                    time.sleep(wait)
                    r = _requests.post(OPENFIGI_URL, headers=headers,
                                    data=json.dumps(payload), timeout=30)

                if r.status_code != 200:
                    logger.warning(f"OpenFIGI HTTP {r.status_code}: {r.text[:300]}")
                    fehler += len(chunk)
                    geholt += len(chunk)
                    if on_progress:
                        on_progress(geholt, total, f"HTTP {r.status_code} bei Chunk {chunk_nr+1}")
                    time.sleep(OPENFIGI_DELAY)
                    continue

                try:
                    daten = r.json()
                except Exception as e:
                    logger.warning(f"OpenFIGI JSON-Fehler: {e}")
                    fehler += len(chunk)
                    geholt += len(chunk)
                    time.sleep(OPENFIGI_DELAY)
                    continue

                if chunk_nr == 0:
                    logger.warning(f"OpenFIGI OK – Beispiel: {json.dumps(daten[:1], ensure_ascii=False)[:400]}")

                chunk_treffer = _verarbeite_figi_antwort(daten, chunk, db)
                treffer += chunk_treffer
                leer    += len(chunk) - chunk_treffer
                geholt  += len(chunk)

            except Exception as e:
                import traceback
                logger.warning(f"OpenFIGI Sync Chunk {chunk_nr} Fehler: {e}\n{traceback.format_exc()}")
                fehler += len(chunk)
                geholt += len(chunk)

            if on_progress:
                on_progress(geholt, total,
                            f"Chunk {chunk_nr+1}/{len(chunks)}  |  "
                            f"{treffer} Treffer  {leer} leer  {fehler} Fehler")
            time.sleep(OPENFIGI_DELAY)

        return (geholt, treffer, leer, fehler)


    def _verarbeite_figi_antwort(daten, chunk, db):
        """Parst OpenFIGI-Antwort und schreibt in DB. Gibt Anzahl Treffer zurueck."""
        treffer = 0
        for isin, eintrag in zip(chunk, daten):
            daten_liste = eintrag.get("data", [])
            if daten_liste:
                bester = (
                    next((d for d in daten_liste if d.get("exchCode") and d.get("figi")), None)
                    or next((d for d in daten_liste if d.get("figi")), None)
                    or daten_liste[0]
                )
                figi_val = bester.get("figi", "")
                if figi_val:
                    db[isin].update({
                        "sektor":         bester.get("marketSector", ""),
                        "boerse":         bester.get("exchCode", ""),
                        "waehrung":       bester.get("currency", "") or db[isin].get("waehrung", ""),
                        "name_figi":      bester.get("name", ""),
                        "figi":           figi_val,
                        "sicherheitstyp": bester.get("securityType2", "") or bester.get("securityType", ""),
                    })
                    treffer += 1
                else:
                    logger.warning(f"OpenFIGI: {isin} hat data aber kein figi-Feld")
            else:
                logger.warning(f"OpenFIGI: {isin} – leere data-Liste")
        return treffer


    # Onvista Fetch
    _cancel_event = threading.Event()
    _fetch_errors = []
    _errors_lock  = threading.Lock()

    def _log_err(isin, msg):
        with _errors_lock:
            _fetch_errors.append((datetime.now().strftime("%H:%M:%S"), isin, msg))
            if len(_fetch_errors) > 100:
                _fetch_errors.pop(0)

    def _onvista_fetch(isin, ath_jahre=1):
        """
        Holt Kursdaten von Onvista (neue API-Struktur ab 2025/26).

        Datenquellen im JSON:
        quote{}                   → Kurs (last), Vortag (previousLast), 52W-Hoch (highPrice1Year),
                                    Waehrung (isoCurrency), Boerse (market.nameExchange)
        cnPerformance{}           → highPriceW52 / highPriceY3 / highPriceY5 (ATH-Zeitraum)
        stocksCnFundamentalList   → KGV (cnPer), Div-Rendite (cnDivYield), aktuellstes Jahr
        company{}                 → Land (isoCountry / nameCountry), Sektor, Branche
        keywords{}                → isoCountry als Fallback
        stocksFigure{}            → Marktkapitalisierung
        """
        if not _REQUESTS_OK:
            return None
        HDR = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }

        def _plausibel(val):
            try:
                return 0.001 < float(val) < 1_000_000
            except Exception:
                return False

        try:
            url = f"https://api.onvista.de/api/v1/instruments/STOCK/ISIN:{isin}/snapshot"
            try:
                r = _requests.get(url, headers=HDR, timeout=10)

                global online
                online = True

            except Exception as e:

                # Timeout oder Netzwerkfehler — zaehlt NICHT als inhaltliches "kein Ergebnis" (in v5 automatisch "KEIN INTERNET" anzeigen) !

                online = False

                # Timeout oder Netzwerkfehler — zaehlt NICHT als inhaltliches "kein Ergebnis"
                logger.warning(f"Onvista Aktie {isin}: {e}")
                return {"_fehler_typ": "timeout"}
            if r.status_code != 200:
                return None
            d = r.json()

            # ── Kurs ─────────────────────────────────────────────────────────────
            # Hauptquelle: quote{} (erster und bester Handelsplatz)
            quote = d.get("quote") or {}
            # Fallback: quoteList.list[0]
            if not quote:
                ql = d.get("quoteList", {}).get("list", [])
                quote = ql[0] if ql else {}

            kurs = None
            prev = None
            waehrung = quote.get("isoCurrency", "")
            boerse_name = quote.get("market", {}).get("nameExchange", "")

            last_val = quote.get("last")
            if last_val and _plausibel(last_val):
                kurs = round(float(last_val), 4)

            prev_val = quote.get("previousLast")
            if prev_val and _plausibel(prev_val):
                prev = round(float(prev_val), 4)

            # ── Bid / Ask (Spread) ────────────────────────────────────────────
            bid = None
            ask = None
            bid_val = quote.get("bid")
            ask_val = quote.get("ask")
            if bid_val and _plausibel(bid_val):
                bid = round(float(bid_val), 4)
            if ask_val and _plausibel(ask_val):
                ask = round(float(ask_val), 4)
            # Fallback: quoteList durchsuchen wenn Hauptquote kein bid/ask hat
            if bid is None or ask is None:
                ql = d.get("quoteList", {}).get("list", [])
                for q in ql:
                    if bid is None:
                        bv = q.get("bid")
                        if bv and _plausibel(bv):
                            bid = round(float(bv), 4)
                    if ask is None:
                        av = q.get("ask")
                        if av and _plausibel(av):
                            ask = round(float(av), 4)
                    if bid and ask:
                        break

            if kurs is None:
                return None

            # ── Name & Land ───────────────────────────────────────────────────────
            inst    = d.get("instrument", {})
            name    = inst.get("name", "")

            # Land: company{} ist die zuverlässigste Quelle
            company = d.get("company", {})
            country_iso  = company.get("isoCountry", "")
            country_name = company.get("nameCountry", "")
            # Fallback: keywords.isoCountry
            if not country_iso:
                country_iso = d.get("keywords", {}).get("isoCountry", "")

            # Ländername aus ISO-Code ableiten wenn nötig
            if not country_name and country_iso:
                country_name = ISIN_PREFIX_LAND.get(country_iso, country_iso)

            # Sektor & Branche aus company.branch
            branch  = company.get("branch", {})
            branche = branch.get("name", "")
            sektor_ov = branch.get("sector", {}).get("name", "")

            # ── ATH je nach Zeitraum ─────────────────────────────────────────────
            # cnPerformance hat vorberechnete Hochs: highPriceW52, highPriceY3, highPriceY5
            cnperf = d.get("cnPerformance", {})
            ath = None
            if ath_jahre == 1:
                # 52W-Hoch: quote.highPrice1Year ist präziser
                v = quote.get("highPrice1Year") or cnperf.get("highPriceW52")
                if v and _plausibel(v):
                    ath = round(float(v), 4)
            elif ath_jahre == 3:
                v = cnperf.get("highPriceY3")
                if v and _plausibel(v):
                    ath = round(float(v), 4)
            elif ath_jahre == 5:
                v = cnperf.get("highPriceY5")
                if v and _plausibel(v):
                    ath = round(float(v), 4)
            # Fallback: 52W aus quote
            if ath is None:
                v = quote.get("highPrice1Year")
                if v and _plausibel(v):
                    ath = round(float(v), 4)

            # ── KGV & Dividende aus stocksCnFundamentalList ───────────────────────
            kgv        = None
            div_brutto = None
            fund_liste = d.get("stocksCnFundamentalList", {}).get("list", [])

            # Sortiere: neuestes reales Jahr zuerst (kein "e" im Label), dann Schätzjahre
            reale  = [f for f in fund_liste if "e" not in f.get("label", "")]
            schaetz = [f for f in fund_liste if "e" in f.get("label", "")]
            geordnet = list(reversed(reale)) + list(reversed(schaetz))

            for eintrag in geordnet:
                # KGV: cnPer — direkt als Zahl, kein Skalierungsproblem
                if kgv is None:
                    per_val = eintrag.get("cnPer")
                    if per_val:
                        try:
                            k = float(per_val)
                            if 0 < k < 2000:
                                kgv = round(k, 1)
                        except Exception:
                            pass

                # Dividende: cnDivYield kommt DIREKT in Prozent (z.B. 4.38 = 4.38%)
                # KEIN * 100 — das war der bisherige Fehler!
                if div_brutto is None:
                    div_val = eintrag.get("cnDivYield")
                    if div_val:
                        try:
                            dv = float(div_val)
                            if 0 < dv < 50:   # Plausibel: 0-50% Dividendenrendite
                                div_brutto = round(dv, 2)
                        except Exception:
                            pass

                if kgv is not None and div_brutto is not None:
                    break

            # Fallback: Dividende aus cnDps (Dividende je Aktie) / Kurs berechnen
            if div_brutto is None and kurs:
                for eintrag in geordnet:
                    dps_val = eintrag.get("cnDpsAdj") or eintrag.get("cnDps")
                    if dps_val:
                        try:
                            dps = float(dps_val)
                            if dps > 0:
                                dv_calc = round(dps / kurs * 100, 2)
                                if 0 < dv_calc < 50:
                                    div_brutto = dv_calc
                                    logger.warning(
                                        f"Div Fallback cnDps/kurs: {isin} "
                                        f"cnDps={dps} kurs={kurs} → {dv_calc}%"
                                    )
                                    break
                        except Exception:
                            pass

            # ── Ex-Dividenden-Tag ─────────────────────────────────────────────────
            from datetime import datetime as _dt
            _now = _dt.now()
            _heute_jahr = _now.year

            MONATE_DE = {1:"Jan",2:"Feb",3:"Mär",4:"Apr",5:"Mai",6:"Jun",
                        7:"Jul",8:"Aug",9:"Sep",10:"Okt",11:"Nov",12:"Dez"}

            # Alle historischen Ex-Datums sammeln (alle Jahre)
            alle_ex = []
            for eintrag in fund_liste:  # fund_liste = alle Jahre ungefiltert
                ex_raw = eintrag.get("dateExDividend")
                if ex_raw:
                    try:
                        ex_dt = _dt.strptime(str(ex_raw)[:10], "%Y-%m-%d")
                        alle_ex.append(ex_dt)
                    except Exception:
                        pass

            ex_tag        = None   # konkretes Datum (YYYY-MM-DD)
            ex_tag_status = "unbekannt"  # "zukunft" | "vergangen" | "schaetzung" | "unbekannt"
            ex_anzeige_raw = None  # für _process_aktie

            # Neuestes Datum suchen
            if alle_ex:
                alle_ex.sort(reverse=True)
                neuestes = alle_ex[0]

                if neuestes > _now:
                    # Bestätigtes Zukunftsdatum
                    ex_tag        = neuestes.strftime("%Y-%m-%d")
                    ex_tag_status = "zukunft"
                else:
                    # Vergangenes Datum — historisches Muster berechnen
                    # Monat aus allen verfügbaren Jahren extrahieren
                    monate = [d.month for d in alle_ex]
                    if monate:
                        # Häufigstes Monat (Mode)
                        from collections import Counter as _Counter
                        monat_haeufig = _Counter(monate).most_common(1)[0][0]
                        monat_name = MONATE_DE[monat_haeufig]

                        # Prüfen ob der geschätzte Termin 2026 noch in der Zukunft liegt
                        try:
                            geschaetzt = _dt(_heute_jahr, monat_haeufig, 15)
                            if geschaetzt < _now:
                                # Monat bereits vorbei in diesem Jahr → nächstes Jahr
                                geschaetzt = _dt(_heute_jahr + 1, monat_haeufig, 15)
                            ex_anzeige_raw = f"~{monat_name} {geschaetzt.year}"
                            ex_tag_status  = "schaetzung"
                        except Exception:
                            ex_tag_status = "vergangen"
                            ex_tag = neuestes.strftime("%Y-%m-%d")
                    else:
                        ex_tag        = neuestes.strftime("%Y-%m-%d")
                        ex_tag_status = "vergangen"

            # Fallback: yfinance NUR wenn Onvista ueberhaupt keinen Ex-Tag kennt
            # (nicht bei vergangen oder schaetzung — dort haben wir bereits historische Daten)
            if ex_tag_status == "unbekannt" and _YFINANCE_OK:
                try:
                    import logging as _logging
                    _logging.getLogger("yfinance").setLevel(_logging.CRITICAL)
                    ticker_sym = inst.get("homeSymbol") or inst.get("symbol", "")
                    if ticker_sym:
                        t = _yfinance.Ticker(ticker_sym)
                        yf_ex = t.info.get("exDividendDate")
                        if yf_ex:
                            ex_dt_yf = _dt.fromtimestamp(int(yf_ex))
                            if ex_dt_yf > _now:
                                ex_tag        = ex_dt_yf.strftime("%Y-%m-%d")
                                ex_tag_status = "zukunft"
                                ex_anzeige_raw = None
                except Exception:
                    pass  # yfinance-Fehler still ignorieren

            return {
                "kurs":           kurs,
                "prev":           prev or kurs,
                "bid":            bid,
                "ask":            ask,
                "kgv":            kgv,
                "div_brutto":     div_brutto,
                "country":        country_name,
                "country_iso":    country_iso,
                "name":           name,
                "ath_52w":        ath,
                "waehrung_ov":    waehrung,
                "boerse_name":    boerse_name,
                "sektor_ov":      sektor_ov,
                "branche":        branche,
                "ath_jahre":      ath_jahre,
                "ex_tag":         ex_tag,
                "ex_tag_status":  ex_tag_status,
                "ex_anzeige_raw": ex_anzeige_raw,
                "mktcap_eur":     round(d.get("stocksFigure", {}).get("marketCapInstrument", 0) / 1_000_000_000, 2) or None,
            }

        except Exception as e:
            logger.warning(f"Onvista Aktie {isin}: {e}")
            return {"_fehler_typ": "timeout"}

    # Scan Logik
    def _process_aktie(aktie, params):
        if _cancel_event.is_set():
            return {"status": "abbruch"}

        isin    = aktie["isin"]

        ath_jahre = params.get("ath_jahre", 1)
        time.sleep(REQ_DELAY)
        data = _onvista_fetch(isin, ath_jahre=ath_jahre)

        # Unterscheide: Timeout (Netzwerk) vs. kein_ergebnis (Onvista hat keine Daten)
        if data is None:
            return {"status": "fehler", "fehler_typ": "kein_ergebnis"}
        if isinstance(data, dict) and data.get("_fehler_typ") == "timeout":
            return {"status": "fehler", "fehler_typ": "timeout"}
        if not data:
            return {"status": "fehler", "fehler_typ": "kein_ergebnis"}

        kurs       = data["kurs"]
        prev       = data["prev"]
        kgv        = data["kgv"]
        div_brutto = data["div_brutto"]
        # Land: Onvista company-Block ist zuverlässig, sonst ISIN-Prefix
        country    = data["country"] or isin_zu_land(isin) or ""
        name       = data["name"] or aktie.get("name", isin)
        ath_52w    = data["ath_52w"]

        min_div = params.get("min_div", 0.0)
        if min_div > 0 and (div_brutto is None or div_brutto < min_div):
            return {"status": "filter"}

        max_kgv = params.get("max_kgv", 0.0)
        if max_kgv > 0 and kgv is not None and kgv > max_kgv:
            return {"status": "filter"}

        ath_abstand = None
        if ath_52w and ath_52w > 0:
            ath_abstand = round((ath_52w - kurs) / ath_52w * 100, 1)
        min_ath = params.get("min_ath_abstand", 0.0)
        if min_ath > 0 and ath_abstand is not None and ath_abstand < min_ath:
            return {"status": "filter"}

        steuer    = get_quellensteuer(country, iso=data.get("country_iso", ""), isin=isin)
        div_netto = calc_at_netto_div(div_brutto, steuer) if div_brutto else None

        delta_str = ""
        if prev and kurs != prev:
            delta     = round((kurs - prev) / prev * 100, 2)
            delta_str = f" ({'+' if delta > 0 else ''}{delta:.2f}%)"

        # Waehrung: Onvista-Kurswaehrung bevorzugen, dann DB, dann ISIN-Schätzung
        waehrung_anzeige = data.get("waehrung_ov") or aktie.get("waehrung", "")

        # Land-Anzeige
        land_anzeige = country or isin_zu_land(isin) or "-"

        # ATH-Zeitraum-Label für Anzeige
        ath_label = {1: "52W", 3: "3J", 5: "5J"}.get(ath_jahre, "52W")

        # Ex-Dividenden-Tag — Anzeige je nach Status
        ex_tag        = data.get("ex_tag")
        ex_tag_status = data.get("ex_tag_status", "unbekannt")
        ex_anzeige_raw = data.get("ex_anzeige_raw")  # z.B. "~Mai 2026"

        if ex_tag_status == "zukunft" and ex_tag:
            # Bestätigtes Datum: 2026-06-09 → 09.06.2026
            try:
                from datetime import datetime as _dt2
                ex_anzeige = _dt2.strptime(ex_tag, "%Y-%m-%d").strftime("%d.%m.%Y")
            except Exception:
                ex_anzeige = ex_tag
        elif ex_tag_status == "schaetzung" and ex_anzeige_raw:
            # Geschätzter Monat: "~Mai 2026"
            ex_anzeige = ex_anzeige_raw
        elif ex_tag_status == "vergangen" and ex_tag:
            # Vergangenes Datum anzeigen
            try:
                from datetime import datetime as _dt2
                ex_anzeige = _dt2.strptime(ex_tag, "%Y-%m-%d").strftime("%d.%m.%Y")
            except Exception:
                ex_anzeige = ex_tag
        else:
            ex_anzeige = "-"

        # ── Spread berechnen ─────────────────────────────────────────────────
        bid = data.get("bid")
        ask = data.get("ask")
        spread_pct = None
        if bid and ask and bid > 0:
            spread_pct = round((ask - bid) / bid * 100, 2)

        # Spread-Filter
        max_spread = params.get("max_spread")
        if max_spread is not None and spread_pct is not None:
            if spread_pct > max_spread:
                return {"status": "filter"}

        spread_str = f"{spread_pct:.2f}%" if spread_pct is not None else "-"

        row = {
            "isin":        isin,
            "name":        name,
            "land":        land_anzeige,
            "waehrung":    waehrung_anzeige or "-",
            "steuer":      f"{steuer:.1f}%",
            "kgv":         f"{kgv:.1f}" if kgv else "-",
            "div":         f"{div_brutto:.2f}%" if div_brutto else "-",
            "div_netto":   f"{div_netto:.2f}%" if div_netto else "-",
            "ex_tag":      ex_anzeige,
            "ex_status":   ex_tag_status,
            "ath_abstand": f"{ath_abstand:.1f}%" if ath_abstand is not None else "-",
            "kurs":        f"{kurs:.2f}" + delta_str,
            "spread":      spread_str,
            "spread_pct":  spread_pct,
            "ath_52w":     f"{ath_52w:.2f}" if ath_52w else "-",
            "ath_label":   ath_label,
            "mktcap":      f"{data['mktcap_eur']:.2f} Mrd" if data.get("mktcap_eur") else "-"
        }
        return {"status": "treffer", "row": row}

    def scan_aktien(aktien_list, params, on_row, on_progress, on_done):
        # Vorfilter: Kontinent/Subregion/Waehrung vor API-Aufruf prüfen
        kontinent_f  = params.get("kontinent", "Alle")
        subregion_f  = params.get("subregion", "Alle")
        waehr_f      = params.get("waehrung", "Alle")

        zu_pruefen   = []
        vorausgef    = 0
        for a in aktien_list:
            isin = a["isin"]
            # Waehrungs-Vorfilter
            if waehr_f != "Alle" and a.get("waehrung") != waehr_f:
                vorausgef += 1
                continue
            # Kontinent-Vorfilter
            if kontinent_f != "Alle" and isin_zu_kontinent(isin) != kontinent_f:
                vorausgef += 1
                continue
            # Subregion-Vorfilter
            if subregion_f not in ("Alle", "") and isin_zu_subregion(isin) != subregion_f:
                vorausgef += 1
                continue
            zu_pruefen.append(a)

        total    = len(zu_pruefen)
        gesamt   = len(aktien_list)
        treffer  = 0
        fehler   = 0
        gefiltert = 0  # KGV/Div/ATH-Filter nach API

        # Status-Meldung vor Scan-Start
        filter_info = []
        if kontinent_f != "Alle":
            filter_info.append(kontinent_f)
        if subregion_f not in ("Alle", ""):
            filter_info.append(subregion_f)
        if waehr_f != "Alle":
            filter_info.append(waehr_f)
        filter_str = "/".join(filter_info) if filter_info else "kein Vorfilter"
        on_progress(0, total or 1, f"Vorfilter [{filter_str}]: {total} von {gesamt} ISINs werden abgefragt ...")

        if total == 0:
            on_done(0, 0, vorausgef, gesamt, abgebrochen=False)
            return

        # Auto-Blacklist: Zaehler fuer neue Blacklist-Eintraege in diesem Scan
        _auto_blacklist_neu = []
        _auto_blacklist_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(_process_aktie, a, params): a for a in zu_pruefen}
            done = 0
            for fut in as_completed(futures):
                if _cancel_event.is_set():
                    ex.shutdown(wait=False, cancel_futures=True)
                    on_done(treffer, fehler, gefiltert + vorausgef, gesamt, abgebrochen=True)
                    return
                done  += 1
                aktie  = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    logger.warning(f"{aktie['isin']}: unerwarteter Fehler: {e}")
                    res = {"status": "fehler", "fehler_typ": "timeout"}

                s = res.get("status", "fehler")
                if s == "treffer":
                    treffer += 1
                    on_row(res["row"])
                    # Erfolg: Fehlerzaehler zuruecksetzen
                    isin_ok = aktie["isin"]
                    if isin_ok in _json_db:
                        if _json_db[isin_ok].get("onvista_fehler", 0) > 0:
                            _json_db[isin_ok]["onvista_fehler"] = 0
                elif s == "filter":
                    gefiltert += 1
                    # Erfolg (Daten vorhanden, nur gefiltert): Zaehler zuruecksetzen
                    isin_ok = aktie["isin"]
                    if isin_ok in _json_db:
                        if _json_db[isin_ok].get("onvista_fehler", 0) > 0:
                            _json_db[isin_ok]["onvista_fehler"] = 0
                elif s == "abbruch":
                    ex.shutdown(wait=False, cancel_futures=True)
                    on_done(treffer, fehler, gefiltert + vorausgef, gesamt, abgebrochen=True)
                    return
                elif s == "fehler":
                    fehler_typ = res.get("fehler_typ", "kein_ergebnis")
                    isin_f = aktie["isin"]
                    name_f = aktie.get("name", "")

                    if fehler_typ == "kein_ergebnis":
                        # Zaehler erhoehen (nur bei echtem "kein Ergebnis", nicht bei Timeout)
                        if isin_f in _json_db:
                            aktuell = _json_db[isin_f].get("onvista_fehler", 0) + 1
                            _json_db[isin_f]["onvista_fehler"] = aktuell
                            if aktuell >= 3:
                                # Auto-Blacklist ausloesen
                                with _auto_blacklist_lock:
                                    _BLACKLIST.add(isin_f)
                                    _auto_blacklist_neu.append((isin_f, name_f, aktuell))
                                del _json_db[isin_f]
                                logger.warning(
                                    f"Auto-Blacklist: {isin_f} ({name_f}) "
                                    f"nach {aktuell} Fehlversuchen hinzugefuegt"
                                )
                            else:
                                logger.warning(
                                    f"Onvista kein Ergebnis: {isin_f} ({name_f}) "
                                    f"[Fehlversuch {aktuell}/3]"
                                )
                        else:
                            logger.warning(f"Onvista kein Ergebnis: {isin_f} ({name_f})")
                    else:
                        # Timeout — nur loggen, kein Zaehler
                        logger.warning(f"Onvista Timeout: {isin_f} ({name_f}) – Zaehler unveraendert")

                    fehler += 1

                on_progress(done, total, aktie.get("name", aktie["isin"]))

        # Auto-Blacklist: speichern und zusammenfassen
        if _auto_blacklist_neu:
            _blacklist_speichern_liste(sorted(_BLACKLIST))
            _db_speichern(_json_db)
            namen = ", ".join(f"{n} ({i})" for i, n, _ in _auto_blacklist_neu)
            logger.warning(
                f"Auto-Blacklist: {len(_auto_blacklist_neu)} neue ISINs hinzugefuegt: {namen}"
            )

        on_done(treffer, fehler, gefiltert + vorausgef, gesamt, abgebrochen=False)

    def check_internet():

        while True:

            try:
            
                _requests.get('https://api.ipify.org').text # Internet probieren !!!

                global online
                online = True

            except:

                online = False

            global internet
            internet = ""

            if not online:

                internet = "(KEIN INTERNET)"  # Anzeige, wenn kein Internet !

            time.sleep(10) # 10-Sekunden-Takt !

    threading.Thread(target=check_internet, daemon=True).start()

    def update_label():    

        label.config(text = "TR AKTIEN-SCREENER v6 " + internet)
        main_frame.after(1000, update_label) # updaten !!!

    # GUI
    parent.pack_propagate(False) 
    parent.grid_propagate(False)

    main_frame = tk.Frame(parent, bg="#1c1c1c", padx=16, pady=12) # BG hier fest oder übergeben
    main_frame.pack(expand=True, fill="both")

    root = parent.winfo_toplevel() 
    root.title(f"TR Aktien-Screener v6 - {len(_TR_ISINS)} ISINs aus {_ISIN_QUELLE}")
    #root.geometry("1366x768") # um in NB zu passen ! -> NICHT !!
    parent.configure(bg=BG)
    parent.columnconfigure(0, weight=1)
    parent.rowconfigure(0, weight=1)

    style = ttk.Style()
    style.theme_use("clam")
    style.configure("TFrame",      background=BG)
    style.configure("TLabel",      background=BG, foreground=TEXT, font=("Consolas", 6))
    style.configure("TEntry",      fieldbackground=SURFACE, foreground=TEXT, insertcolor=TEXT, font=("Consolas", 9))
    style.configure("TCombobox",   fieldbackground=SURFACE, foreground=TEXT,
                    selectbackground=SURFACE, selectforeground=TEXT)
    style.map("TCombobox",
        fieldbackground=[("readonly", SURFACE)],
        selectbackground=[("readonly", SURFACE)],
        selectforeground=[("readonly", TEXT)],
        foreground=[("readonly", TEXT)])
    style.configure("Treeview",         background=CARD, fieldbackground=CARD, foreground=TEXT,
                    rowheight=15, font=("Consolas", 9))
    style.configure("Treeview.Heading", background=SURFACE, foreground=BLUE,
                    font=("Consolas", 9, "bold"), relief="flat")
    style.map("Treeview", background=[("selected", "#2d333b")])
    style.configure("TProgressbar", troughcolor=SURFACE, background=GREEN)

    main = tk.Frame(main_frame, bg=BG, padx=16, pady=12)
    main.grid(row=0, column=0, sticky="nsew")
    main.columnconfigure(0, weight=1)
    main.rowconfigure(5, weight=1)

    global internet
    internet = ""

    if not online:

        internet = "(KEIN INTERNET)"  # Anzeige, wenn kein Internet !

    # Titelzeile
    _mit_figi = sum(1 for d in _json_db.values() if d.get("figi"))
    title_frame = tk.Frame(main, bg=BG)
    title_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
    label = tk.Label(title_frame, text="TR AKTIEN-SCREENER v6 " + internet,
            bg=BG, fg=BLUE, font=("Consolas", 12, "bold"))
    label.pack(side="left")
    tk.Label(title_frame, text=f"[{len(_TR_ISINS)} ISINs | AT KESt-Fix | OpenFIGI]",
            bg=BG, fg=ORANGE, font=("Consolas", 8, "bold")).pack(side="left", padx=8)
    deps = ("curl_cffi OK" if _CURL_CFFI_OK else "curl_cffi fehlt") + \
        ("  requests OK" if _REQUESTS_OK else "  requests fehlt") + \
        (f"  FIGI-Key: {'ja' if OPENFIGI_API_KEY else 'nein (gratis)'}")
    tk.Label(title_frame, text=deps, bg=BG, fg=MUTED, font=("Consolas", 8)).pack(side="left", padx=20)
    lbl_count = tk.Label(title_frame,
                        text=f"{len(TR_AKTIEN)} Aktien in DB  |  {_mit_figi} mit FIGI-Daten",
                        bg=BG, fg=DIM, font=("Consolas", 8))
    lbl_count.pack(side="right")

    if not _TR_ISINS:
        tk.Label(main,
                text="  Keine TR_Aktien_*.csv gefunden! Lege die Datei in denselben Ordner.",
                bg=RED, fg=TEXT, font=("Consolas", 8, "bold")).grid(
                row=1, column=0, sticky="ew", pady=(0, 4))

    tk.Frame(main, bg=BORDER, height=1).grid(row=2, column=0, sticky="ew", pady=(0, 8))

    # Filterleiste
    fbar = tk.Frame(main, bg=BG)
    fbar.grid(row=3, column=0, sticky="ew", pady=(0, 8))

    def _lbl(parent, text):
        tk.Label(parent, text=text, bg=BG, fg=MUTED, font=("Consolas", 8)).pack(side="left", padx=(0, 4))

    def _entry(parent, default, width=7):
        e = tk.Entry(parent, bg=SURFACE, fg=TEXT, insertbackground=TEXT,
                    font=("Consolas", 9), width=width, relief="flat",
                    highlightbackground=BORDER, highlightcolor=BLUE, highlightthickness=1, bd=0)
        e.insert(0, default)
        e.pack(side="left", padx=(0, 14), ipady=3)
        return e

    _lbl(fbar, "Max. KGV (0=alle):")
    entry_kgv = _entry(fbar, "0")
    _lbl(fbar, "Min. Div. brutto %:")
    entry_div = _entry(fbar, "0.0")
    _lbl(fbar, "Min. ATH-Abstand %:")
    entry_ath = _entry(fbar, "0")

    _lbl(fbar, "ATH-Zeitraum:")
    ath_jahre_var = tk.StringVar(value="52W")
    ath_jahre_cb  = tk.OptionMenu(fbar, ath_jahre_var, "52W", "3 Jahre", "5 Jahre")
    ath_jahre_cb.config(bg=SURFACE, fg=TEXT, activebackground=BORDER, activeforeground=TEXT,
                        font=("Consolas", 9), relief="flat", bd=0, highlightthickness=1,
                        highlightbackground=BORDER)
    ath_jahre_cb["menu"].config(bg=SURFACE, fg=TEXT, activebackground=BLUE, activeforeground=BG,
                                font=("Consolas", 9))
    ath_jahre_cb.pack(side="left", padx=(0, 14))

    _lbl(fbar, "Max. Spread %:")
    spread_filter_var = tk.StringVar(value=">10% raus")
    spread_filter_cb  = tk.OptionMenu(fbar, spread_filter_var,
        ">10% raus", "1%", "2%", "3%", "4%", "5%", "6%", "7%", "8%", "9%", "10%")
    spread_filter_cb.config(bg=SURFACE, fg=TEXT, activebackground=BORDER, activeforeground=TEXT,
                            font=("Consolas", 9), relief="flat", bd=0, highlightthickness=1,
                            highlightbackground=BORDER)
    spread_filter_cb["menu"].config(bg=SURFACE, fg=TEXT, activebackground=BLUE, activeforeground=BG,
                                    font=("Consolas", 9))
    spread_filter_cb.pack(side="left", padx=(0, 14))

    tk.Frame(fbar, bg=BORDER, width=1, height=28).pack(side="left", padx=10)

    _lbl(fbar, "Waehrung:")
    waehr_var = tk.StringVar(value="Alle")

    currency_map = {
        "US": "USD", "CA": "CAD", "AU": "AUD", "NZ": "NZD",
        "GB": "GBP", "CH": "CHF", "JP": "JPY", "CN": "CNY",
        "HK": "HKD", "SG": "SGD", "DK": "DKK", "SE": "SEK",
        "NO": "NOK", "IN": "INR", "KR": "KRW", "TW": "TWD",
        "MX": "MXN", "BR": "BRL", "ZA": "ZAR", "TH": "THB",
        "ID": "IDR", "MY": "MYR", "PH": "PHP", "IL": "ILS", 
        "SA": "SAR", "AE": "AED", "QA": "QAR"
    }

    unique_currencies = sorted(list(set(currency_map.values())))
    options = ["Alle", "EUR"] + unique_currencies

    waehr_var = tk.StringVar(main_frame) 
    waehr_var.set("Alle")          

    waehr_cb = tk.OptionMenu(fbar, waehr_var, *options)
    waehr_cb.config(bg=SURFACE, fg=TEXT, activebackground=BORDER, activeforeground=TEXT,
                    font=("Consolas", 9), relief="flat", bd=0, highlightthickness=1,
                    highlightbackground=BORDER)
    waehr_cb["menu"].config(bg=SURFACE, fg=TEXT, activebackground=BLUE, activeforeground=BG,
                            font=("Consolas", 9))
    waehr_cb.pack(side="left", padx=(0, 14))

    _lbl(fbar, "Kontinent:")
    kontinent_var = tk.StringVar(value="Alle")
    _kontinent_opts = ["Alle", "Europa", "Nordamerika", "Südamerika", "Asien", "Australien", "Afrika"]
    kontinent_cb = tk.OptionMenu(fbar, kontinent_var, *_kontinent_opts)
    kontinent_cb.config(bg=SURFACE, fg=TEXT, activebackground=BORDER, activeforeground=TEXT,
                        font=("Consolas", 9), relief="flat", bd=0, highlightthickness=1,
                        highlightbackground=BORDER)
    kontinent_cb["menu"].config(bg=SURFACE, fg=TEXT, activebackground=BLUE, activeforeground=BG,
                                font=("Consolas", 9))
    kontinent_cb.pack(side="left", padx=(0, 4))

    _lbl(fbar, "Land:")
    subregion_var = tk.StringVar(value="Alle")
    subregion_cb  = tk.OptionMenu(fbar, subregion_var, "Alle")
    subregion_cb.config(bg=SURFACE, fg=DIM, activebackground=BORDER, activeforeground=TEXT,
                        font=("Consolas", 9), relief="flat", bd=0, highlightthickness=1,
                        highlightbackground=BORDER, width=14)
    subregion_cb["menu"].config(bg=SURFACE, fg=TEXT, activebackground=BLUE, activeforeground=BG,
                                font=("Consolas", 9))
    subregion_cb.pack(side="left", padx=(0, 14))

    def _on_kontinent_change(*_):
        """Aktualisiert das Länder-Dropdown wenn Kontinent geändert wird."""
        k = kontinent_var.get()
        menu = subregion_cb["menu"]
        menu.delete(0, "end")
        optionen = KONTINENT_SUBREGIONEN.get(k, ["Alle"])
        for opt in optionen:
            menu.add_command(label=opt, command=lambda v=opt: subregion_var.set(v))
        subregion_var.set("Alle")
        # Optisch aktiv/inaktiv
        if k == "Alle":
            subregion_cb.config(fg=DIM, state=tk.DISABLED)
        else:
            subregion_cb.config(fg=TEXT, state=tk.NORMAL)

    kontinent_var.trace_add("write", _on_kontinent_change)
    subregion_cb.config(state=tk.DISABLED)  # initial deaktiviert bis Kontinent gewählt

    # Aktionsleiste
    abar = tk.Frame(main, bg=BG)
    abar.grid(row=4, column=0, sticky="ew", pady=(0, 6))

    def _btn(parent, text, cmd, bg=SURFACE, fg=TEXT, bold=False):
        f = ("Consolas", 9, "bold") if bold else ("Consolas", 9)
        return tk.Button(parent, text=text, command=cmd, bg=bg, fg=fg, font=f,
                        relief="flat", bd=0, padx=14, pady=6, cursor="hand2",
                        activebackground=BORDER, activeforeground=TEXT)

    btn_scan   = _btn(abar, "ABFRAGEN",      lambda: start_scan(),          GREEN,  BG,     bold=True)
    btn_stop   = _btn(abar, "STOP",          lambda: _do_stop(),            RED,    BG,     bold=True)
    btn_import = _btn(abar, "CSV IMPORT",    lambda: csv_import_dialog(),   BLUE,   BG)
    btn_figi   = _btn(abar, "FIGI REFRESH",  lambda: figi_refresh_dialog(), TEAL,   BG)
    btn_diag   = _btn(abar, "FIGI DIAGNOSE", lambda: figi_diagnose(),       ORANGE, BG)
    btn_export = _btn(abar, "EXPORTIEREN",   lambda: export_data(),         PINK, BG)
    btn_log    = _btn(abar, "LOG",           lambda: open_log(),            BLUE, BG)

    btn_scan.pack(side="left",   padx=(0, 4))
    btn_stop.pack(side="left",   padx=(0, 6))
    btn_import.pack(side="left", padx=(0, 4))
    btn_figi.pack(side="left",   padx=(0, 4))
    btn_diag.pack(side="left",   padx=(0, 4))
    btn_export.pack(side="left", padx=(0, 4))
    btn_log.pack(side="left",    padx=(0, 4))

    def _do_stop():
        _cancel_event.set()
        status_var.set("Abbruch angefordert ...")
        btn_stop.config(state=tk.DISABLED)

    status_var = tk.StringVar(value=f"Bereit - {len(TR_AKTIEN)} Aktien geladen. Klicke ABFRAGEN.")
    tk.Label(abar, textvariable=status_var, bg=BG, fg=MUTED, font=("Consolas", 8)).pack(side="left", padx=16)
    last_update_var = tk.StringVar(value="")
    tk.Label(abar, textvariable=last_update_var, bg=BG, fg=GREEN, font=("Consolas", 8)).pack(side="right", padx=4)

    prog_var = tk.IntVar(value=0)
    ttk.Progressbar(main, variable=prog_var, maximum=100, style="TProgressbar").grid(
        row=4, column=0, sticky="ew", pady=(40, 4))

    # Tabelle
    tbl_frame = tk.Frame(main, bg=BG)
    tbl_frame.grid(row=5, column=0, sticky="nsew")
    tbl_frame.columnconfigure(0, weight=1)
    tbl_frame.rowconfigure(0, weight=1)

    COL_CFG = [
        ("name",        "Unternehmen",   120, "center"),
        ("land",        "Land",           90, "center"),
        ("waehrung",    "Waehr.",          55, "center"),
        ("steuer",      "Quellenst.",      72, "center"),
        ("kgv",         "KGV",             65, "center"),
        ("div",         "Div. brutto",     85, "center"),
        ("div_netto",   "Div. Netto(AT)",  95, "center"),
        ("ex_tag",      "Ex-Div Tag",      88, "center"),
        ("ath_abstand", "ATH-Abstand",     90, "center"),
        ("kurs",        "Kurs (Onvista)", 90, "center"),
        ("spread",      "Spread %",        72, "center"),
        ("ath_52w",     "ATH 52W",         90, "center"),
        ("isin",        "ISIN",           90, "center"),
        ("mktcap",      "MktCap (Mrd €)",   98, "center")
    ]

    tree = ttk.Treeview(tbl_frame, columns=[c[0] for c in COL_CFG], show="headings", height=18)
    for cid, heading, width, anchor in COL_CFG:
        tree.heading(cid, text=heading, command=lambda c=cid: _sort_col(c))
        tree.column(cid, width=width, anchor=anchor, minwidth=36)

    # Farb-Tags für Zeilen
    tree.tag_configure("div_hoch",      background="#2d1a1a", foreground="#f85149")  # rot: Div > 25%
    tree.tag_configure("ex_zukunft",    foreground="#3fb950")   # grün: bestätigter zukünftiger Ex-Tag
    tree.tag_configure("ex_schaetzung", foreground="#3fb950")   # grün: geschätzter Ex-Monat
    tree.tag_configure("ex_vergangen",  foreground="#484f58")   # grau: vergangener Ex-Tag
    tree.tag_configure("spread_ok",     foreground="#3fb950")   # grün: Spread <1%
    tree.tag_configure("spread_mittel", foreground="#d29922")   # orange: Spread 1-3%
    tree.tag_configure("spread_hoch",   foreground="#f85149")   # rot: Spread >3%
    # "ex_unbekannt" = keine besondere Farbe → weiß/normal

    sy = ttk.Scrollbar(tbl_frame, orient=tk.VERTICAL,   command=tree.yview)
    sx = ttk.Scrollbar(tbl_frame, orient=tk.HORIZONTAL, command=tree.xview)
    tree.configure(yscroll=sy.set, xscroll=sx.set)
    tree.grid(row=0, column=0, sticky="nsew")
    sy.grid(row=0, column=1, sticky="ns")
    sx.grid(row=1, column=0, sticky="ew")
    tree.bind("<Double-1>", lambda e: _open_onvista())

    tk.Label(main,
        text="Kurs = Onvista Live.  Div. Netto(AT) = nach Quellensteuer + KESt.  "
            "ATH-Abstand = Abstand vom Hoch (52W/3J/5J).  Ex-Div Tag: grün=bestätigt/geschätzt, grau=vergangen, weiß=unbekannt, rot=Div>25%.  "
            "Doppelklick öffnet Onvista.",
        bg=BG, fg=DIM, font=("Consolas", 7)).grid(row=6, column=0, pady=(4, 0), sticky="w")

    # Sortierung
    _sort_state = {}

    def _sort_col(col):
        asc = not _sort_state.get(col, False)
        _sort_state[col] = asc
        rows = [(tree.set(item, col), item) for item in tree.get_children("")]
        def key(v):
            s = v[0].replace("%", "").replace(",", ".").strip().split()[0]
            try:    return (0, float(s))
            except: return (1, s.lower())
        rows.sort(key=key, reverse=not asc)
        for idx, (_, item) in enumerate(rows):
            tree.move(item, "", idx)
        for cid, h, *_ in COL_CFG:
            tree.heading(cid, text=h)
        cur = next(h for cid, h, *_ in COL_CFG if cid == col)
        tree.heading(col, text=cur + (" A" if asc else " V"), command=lambda: _sort_col(col))

    def _sort_by_div_netto_desc():
        rows = [(tree.set(item, "div_netto"), item) for item in tree.get_children("")]
        def key(v):
            s = v[0].replace("%", "").strip()
            try:    return (0, -float(s))
            except: return (1, 0)
        rows.sort(key=key)
        for idx, (_, item) in enumerate(rows):
            tree.move(item, "", idx)
        for cid, h, *_ in COL_CFG:
            tree.heading(cid, text=h)
        tree.heading("div_netto", text="Div. Netto(AT) V",
                    command=lambda: _sort_col("div_netto"))

    # Scan Funktionen
    def start_scan():
        try:
            ath_raw = ath_jahre_var.get()
            ath_j   = 1 if ath_raw == "52W" else (3 if "3" in ath_raw else 5)
            spread_raw = spread_filter_var.get()
            if spread_raw == ">10% raus":
                max_spread = 10.0
            else:
                max_spread = float(spread_raw.replace("%", "").strip())

            params = {
                "max_kgv":         float(entry_kgv.get()),
                "min_div":         float(entry_div.get()),
                "min_ath_abstand": float(entry_ath.get()),
                "waehrung":        waehr_var.get(),
                "kontinent":       kontinent_var.get(),
                "subregion":       subregion_var.get(),
                "ath_jahre":       ath_j,
                "max_spread":      max_spread,
            }
        except ValueError:
            messagebox.showerror("Fehler", "Filter: Nur Zahlen eingeben!")
            return

        if not TR_AKTIEN:
            messagebox.showwarning("Keine Daten",
                                "Bitte TR_Aktien_*.csv in den Ordner legen und neu starten.")
            return

        # ATH-Spaltenheader dynamisch anpassen
        ath_label = {1: "ATH 52W", 3: "ATH 3J", 5: "ATH 5J"}.get(ath_j, "ATH 52W")
        tree.heading("ath_52w",     text=ath_label,
                    command=lambda: _sort_col("ath_52w"))
        tree.heading("ath_abstand", text=f"Abstand {ath_label}",
                    command=lambda: _sort_col("ath_abstand"))

        _cancel_event.clear()
        _fetch_errors.clear()
        for item in tree.get_children():
            tree.delete(item)

        prog_var.set(0)
        btn_scan.config(state=tk.DISABLED)
        btn_stop.config(state=tk.NORMAL)
        btn_export.config(state=tk.DISABLED)
        status_var.set(f"Onvista Abfrage - {len(TR_AKTIEN)} Aktien ...")
        last_update_var.set("")
        logger.info(f"Scan gestartet: {len(TR_AKTIEN)} Aktien | Filter: {params}")

        threading.Thread(target=scan_aktien,
                        args=(TR_AKTIEN, params, _on_row, _on_progress, _on_done),
                        daemon=True).start()

    def _on_row(row):
        values = (row["name"], row["land"],
                row["waehrung"], row["steuer"],
                row["kgv"], row["div"], row["div_netto"],
                row["ex_tag"],
                row["ath_abstand"], row["kurs"], row["spread"], row["ath_52w"], row["isin"], row["mktcap"])

        # Tag-Logik:
        # 1. Rot = Div > 25% (unplausibel)
        # 2. Gruen = Ex-Tag zukunft ODER Schaetzung
        # 3. Grau = Ex-Tag vergangen
        # 4. Kein Tag (weiss) = unbekannt
        tags = []
        try:
            div_val = float(str(row["div"]).replace("%", "").strip())
            if div_val > 25:
                tags.append("div_hoch")
        except Exception:
            pass

        # Spread-Tag (zusätzlich – überschreibt nicht andere Tags)
        spread_pct = row.get("spread_pct")
        if spread_pct is not None:
            if spread_pct < 1.0:
                tags.append("spread_ok")
            elif spread_pct <= 3.0:
                tags.append("spread_mittel")
            else:
                tags.append("spread_hoch")

        if not tags:
            status = row.get("ex_status", "unbekannt")
            if status == "zukunft":
                tags.append("ex_zukunft")
            elif status == "schaetzung":
                tags.append("ex_schaetzung")
            elif status == "vergangen":
                tags.append("ex_vergangen")

        main_frame.after(0, lambda v=values, t=tuple(tags): tree.insert("", tk.END, values=v, tags=t))

    def _on_progress(done, total, name):
        pct = int(done / total * 100) if total else 0
        main_frame.after(0, lambda: prog_var.set(pct))
        main_frame.after(0, lambda: status_var.set(f"{name[:35]:<35}  {done}/{total}  ({pct}%)"))

    def _on_done(treffer, fehler, gefiltert, total, abgebrochen=False):
        def _ui():
            prog_var.set(0 if abgebrochen else 100)
            btn_scan.config(state=tk.NORMAL)
            btn_stop.config(state=tk.DISABLED)
            if treffer > 0:
                btn_export.config(state=tk.NORMAL)
                _sort_by_div_netto_desc()
            icon = "Abgebrochen" if abgebrochen else "Fertig"
            status_var.set(
                f"{icon}  |  {treffer} Treffer  |  {fehler} Fehler  |  {gefiltert} übersprungen (von {total})"
            )
            logger.info(f"Scan beendet: {treffer} Treffer, {fehler} Fehler, {gefiltert} uebersprungen, abgebrochen={abgebrochen}")
            if not abgebrochen:
                last_update_var.set(f"Stand: {datetime.now().strftime('%d.%m.%Y  %H:%M')}")
        main_frame.after(0, _ui)

    def _open_onvista():
        item = tree.focus()
        if item:
            isin = tree.set(item, "isin")
            webbrowser.open(f"https://www.onvista.de/aktien/ISIN:{isin}")

    def csv_import_dialog():
        pfad = filedialog.askopenfilename(
            title="TR_Aktien_*.csv waehlen",
            filetypes=[("CSV", "*.csv"), ("Alle Dateien", "*.*")]
        )
        if not pfad:
            return
        try:
            eintraege = []
            with open(pfad, "r", encoding="utf-8-sig", errors="ignore") as f:
                lines = f.read().strip().split("\n")
            for line in lines[1:]:
                parts = line.split(";")
                if len(parts) < 2:
                    continue
                isin = parts[0].strip()
                if not re.match(r"^[A-Z]{2}[A-Z0-9]{10}$", isin):
                    continue
                name = parts[1].strip()
                eintraege.append({"isin": isin, "name": name})

            if not eintraege:
                messagebox.showwarning("Fehler", "Keine ISINs in der Datei gefunden.")
                return

            db  = _db_laden()
            neu = 0
            blockiert = 0
            neu_isins = []
            for e in eintraege:
                isin = e["isin"]
                if isin in _BLACKLIST:
                    blockiert += 1
                    continue  # Blacklist: ueberspringen
                if isin not in db:
                    db[isin] = {"isin": isin, "name": e["name"],
                                "waehrung": _isin_waehrung(isin)}
                    neu += 1
                    neu_isins.append(isin)

            _db_speichern(db)
            _db_auf_aktien_anwenden(db)
            lbl_count.config(text=f"{len(TR_AKTIEN)} Aktien in DB")
            status_var.set(f"{len(TR_AKTIEN)} Aktien geladen. ABFRAGEN klicken.")

            bl_info = f"\n({blockiert} ISINs per Blacklist blockiert)" if blockiert else ""
            if neu_isins and _REQUESTS_OK:
                antwort = messagebox.askyesno(
                    "OpenFIGI Metadaten",
                    f"{neu} neue ISINs importiert.{bl_info}\n\n"
                    f"Jetzt Metadaten (Sektor, Boerse, Waehrung) via OpenFIGI abrufen?\n"
                    f"Das dauert ca. {max(1, len(neu_isins) // OPENFIGI_BATCH + 1) * 2} Sekunden."
                )
                if antwort:
                    _starte_figi_enrichment(neu_isins, db)
                else:
                    messagebox.showinfo("Import", f"{len(eintraege)} ISINs geladen, {neu} neu.{bl_info}\nOpenFIGI uebersprungen.")
            else:
                msg = f"{len(eintraege)} ISINs geladen, {neu} neu.{bl_info}"
                if not _REQUESTS_OK:
                    msg += "\n(requests nicht installiert – OpenFIGI nicht verfuegbar)"
                messagebox.showinfo("Import", msg)

        except Exception as e:
            messagebox.showerror("Fehler", str(e))


    def _starte_figi_enrichment(isins, db, refresh_alle=False):
        """Startet OpenFIGI-Anreicherung im Hintergrund-Thread."""
        if refresh_alle:
            zu_holen = [isin for isin in db if not db[isin].get("figi")]
        else:
            zu_holen = isins

        if not zu_holen:
            messagebox.showinfo("OpenFIGI", "Alle Eintraege haben bereits FIGI-Metadaten.")
            return

        btn_figi.config(state=tk.DISABLED)
        btn_import.config(state=tk.DISABLED)
        status_var.set(f"OpenFIGI: {len(zu_holen)} ISINs werden abgefragt ...")
        prog_var.set(0)

        def _progress(done, total, msg):
            pct = int(done / total * 100) if total else 0
            main_frame.after(0, lambda: prog_var.set(pct))
            main_frame.after(0, lambda: status_var.set(f"OpenFIGI: {msg}  ({pct}%)"))

        def _run():
            try:
                ergebnis = _openfigi_db_anreichern(db, zu_holen, on_progress=_progress)
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                main_frame.after(0, lambda: messagebox.showerror(
                    "OpenFIGI – unerwarteter Fehler",
                    f"Fehler in _openfigi_db_anreichern:\n\n{e}\n\n{tb[:800]}"
                ))
                main_frame.after(0, lambda: btn_figi.config(state=tk.NORMAL))
                main_frame.after(0, lambda: btn_import.config(state=tk.NORMAL))
                main_frame.after(0, lambda: status_var.set("OpenFIGI Fehler – siehe Popup."))
                return

            # Unpack – kompatibel mit alter (int) und neuer (tuple) Rueckgabe
            if isinstance(ergebnis, tuple):
                geholt, treffer, leer, api_fehler = ergebnis
            else:
                geholt, treffer, leer, api_fehler = ergebnis, 0, 0, 0

            _db_speichern(db)
            _db_auf_aktien_anwenden(db)

            mit_figi_neu = sum(1 for d in db.values() if d.get("figi"))
            mit_sektor   = sum(1 for d in db.values() if d.get("sektor"))

            # Diagnose-Meldung je nach Ergebnis
            if treffer == 0 and api_fehler > 0:
                diag = (
                    f"Verarbeitet: {geholt}  |  Treffer: {treffer}  |  "
                    f"Leer: {leer}  |  Fehler: {api_fehler}\n\n"
                    f"Alle Anfragen sind fehlgeschlagen.\n"
                    f"Details im Log (LOG-Button) – dort steht der genaue Fehler."
                )
            elif treffer == 0 and leer > 0:
                diag = (
                    f"HINWEIS: OpenFIGI hat fuer alle {leer} ISINs leere Daten zurueckgegeben.\n\n"
                    f"Moegliche Ursachen:\n"
                    f"  • Rate-Limit ohne API-Key\n"
                    f"  • ISINs nicht in OpenFIGI-Datenbank\n\n"
                    f"Details im Log (LOG-Button)."
                )
            else:
                diag = (
                    f"Verarbeitet: {geholt}  |  Treffer: {treffer}  |  "
                    f"Leer: {leer}  |  Fehler: {api_fehler}\n"
                    f"{mit_sektor} Eintraege haben Sektordaten.\n\n"
                    f"Tipp: Sektor + Boerse erscheinen nach dem naechsten ABFRAGEN."
                )

            def _done():
                prog_var.set(0)
                btn_figi.config(state=tk.NORMAL)
                btn_import.config(state=tk.NORMAL)
                lbl_count.config(text=f"{len(TR_AKTIEN)} Aktien in DB  |  {mit_figi_neu} mit FIGI-Daten")
                status_var.set(
                    f"OpenFIGI: {treffer} Treffer, {leer} leer, {api_fehler} Fehler. "
                    f"ABFRAGEN klicken."
                )
                messagebox.showinfo("OpenFIGI abgeschlossen", diag)
            main_frame.after(0, _done)

        threading.Thread(target=_run, daemon=True).start()


    def figi_refresh_dialog():
        """Manuell OpenFIGI fuer alle ISINs ohne FIGI-Daten nachladen."""
        db = _db_laden()
        ohne_figi = sum(1 for d in db.values() if not d.get("figi"))
        if ohne_figi == 0:
            messagebox.showinfo("OpenFIGI", f"Alle {len(db)} Eintraege haben bereits FIGI-Metadaten.")
            return
        antwort = messagebox.askyesno(
            "OpenFIGI Refresh",
            f"{ohne_figi} von {len(db)} Eintraegen haben noch keine FIGI-Metadaten.\n\n"
            f"Jetzt abrufen? (ca. {max(1, ohne_figi // OPENFIGI_BATCH + 1) * 2}s)"
        )
        if antwort:
            _starte_figi_enrichment([], db, refresh_alle=True)


    def figi_diagnose():
        """Testet OpenFIGI mit 3 bekannten ISINs und zeigt die Rohantwort."""
        if not _REQUESTS_OK:
            messagebox.showerror("Fehler", "requests nicht installiert.")
            return

        test_isins = ["US0378331005", "DE0008404005", "AT0000743059"]  # Apple, Allianz, OMV
        payload = [{"idType": "ID_ISIN", "idValue": isin} for isin in test_isins]
        headers = {"Content-Type": "application/json"}
        if OPENFIGI_API_KEY:
            headers["X-OPENFIGI-APIKEY"] = OPENFIGI_API_KEY

        status_var.set("OpenFIGI Diagnose läuft ...")

        def _run():
            try:
                r = _requests.post(OPENFIGI_URL, headers=headers,
                                data=json.dumps(payload), timeout=15)
                status = r.status_code
                body   = r.text[:3000]
                try:
                    parsed = r.json()
                    treffer = sum(1 for e in parsed if e.get("data"))
                    msg = (
                        f"HTTP Status: {status}\n"
                        f"ISINs gesendet: {len(test_isins)}\n"
                        f"ISINs mit Daten: {treffer}\n"
                        f"API-Key aktiv: {'ja' if OPENFIGI_API_KEY else 'nein'}\n\n"
                        f"--- Rohantwort (gekuerzt) ---\n"
                        f"{json.dumps(parsed, indent=2, ensure_ascii=False)[:2000]}"
                    )
                except Exception:
                    msg = f"HTTP Status: {status}\nAntwort:\n{body}"

                def _show():
                    status_var.set("Bereit.")
                    # Eigenes Fenster fuer lesbare Ausgabe
                    win = tk.Toplevel(main_frame)
                    win.title("OpenFIGI Diagnose")
                    win.geometry("700x500")
                    win.configure(bg=BG)
                    txt = tk.Text(win, bg=SURFACE, fg=TEXT, font=("Consolas", 8),
                                wrap=tk.WORD, padx=10, pady=10)
                    txt.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
                    txt.insert("1.0", msg)
                    txt.config(state=tk.DISABLED)
                    tk.Button(win, text="Schliessen", command=win.destroy,
                            bg=SURFACE, fg=MUTED, font=("Consolas", 8),
                            relief="flat", padx=10, pady=4).pack(pady=(0, 8))
                main_frame.after(0, _show)

            except Exception as e:
                main_frame.after(0, lambda: messagebox.showerror(
                    "OpenFIGI Diagnose Fehler",
                    f"Verbindung fehlgeschlagen:\n{e}\n\n"
                    f"Bitte Internetverbindung pruefen."
                ))
                main_frame.after(0, lambda: status_var.set("Bereit."))

        threading.Thread(target=_run, daemon=True).start()

    def export_data():
        rows = tree.get_children()
        if not rows:
            return
        data    = [tree.item(r)["values"] for r in rows]
        headers = [h for _, h, *_ in COL_CFG]

        # Automatischer Dateiname: Region_Datum_HHmm
        region  = kontinent_var.get()
        sub     = subregion_var.get()
        if sub and sub != "Alle":
            region = sub
        datum   = datetime.now().strftime("%d_%m_%Y_um_%H_Uhr_%M")
        vorschlag = f"{region}_Scan_{datum}"

        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=vorschlag,
            filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")]
        )
        if not path:
            return
        try:
            if path.endswith(".csv"):
                import csv
                with open(path, "w", newline="", encoding="utf-8-sig") as f:  # PANDAS besser !!!

                    w = csv.writer(f, delimiter=";")
                    w.writerow(headers)
                    w.writerows(data)
                
            elif _PANDAS_OK:
        
                pd.DataFrame(data, columns=headers).to_excel(path, index=False)

                raw_rows = [tree.item(r)["values"] for r in rows]

                engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')

                logger.warning("SQL ACCESS")

                data1 = {
        'name': ['ausgabe' + str(datum)],
        'limitx': ['1000'],
        'content': ["".join([",".join(map(str, row)) for row in raw_rows]).replace("Mrd", "MrdPROMPTSPLIT")], # splitten !!!
        'done': [0],
        'client': ['/127.0.0.1']
        }

                dfx = pd.DataFrame(data1)

                dfx.to_sql(
                name='tasks', 
                con=engine, 
                if_exists='append', 
                index=False,
                dtype={
                    'name': types.VARCHAR(255),
                    'limitx': types.VARCHAR(255),
                    'content': types.Text(),
                    'done': types.BOOLEAN(),
                    'client': types.VARCHAR(255)
                }
                )

                t1 = threading.Thread(target= lambda: pd.DataFrame(data, columns=headers).to_sql(name='ausgabe', con=engine, if_exists='append', index=False)) # SQL Server Backup !!!
                t2 = threading.Thread(target= lambda: pd.DataFrame(data, columns=headers).to_sql(name='ausgabe' + str(datum), con=engine, if_exists='append', index=False))
            
            status_var.set(f"Gespeichert: {os.path.basename(path)}")

            t1.start() 
            t2.start()
            t1.join()
            t2.join()

            status_var.set(f"Gespeichert: {os.path.basename(path)}" + " & Mit Datenbankserver online synchronisiert: " + 'ausgabe' + str(datum)) # warten mit join, danach SYNC zeigem

        except Exception as e:
           
            messagebox.showerror("Export-Fehler", str(e))

    def open_log():
        win = tk.Toplevel(main_frame)
        win.title("Fehler & Log")
        win.geometry("1000x650")
        win.configure(bg=BG)
        win.columnconfigure(0, weight=1)
        win.rowconfigure(1, weight=1)

        # Titelzeile mit Buttons
        hdr = tk.Frame(win, bg=BG)
        hdr.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))
        tk.Label(hdr, text="FEHLER & LOG", bg=BG, fg=RED,
                font=("Consolas", 10, "bold")).pack(side="left")
        tk.Label(hdr, text="  (alle WARNING/ERROR/CRITICAL Meldungen aus dieser Session)",
                bg=BG, fg=MUTED, font=("Consolas", 8)).pack(side="left")

        def _refresh():
            inhalt = _log_get()
            
            # Nur WARNING und schlimmer anzeigen — DEBUG/INFO rausfiltern für Übersichtlichkeit
            zeilen = [z for z in inhalt.splitlines()
                    if any(lv in z for lv in ("WARNING", "ERROR", "CRITICAL", "EXCEPTION"))]
            if not zeilen:
                zeilen = ["(Noch keine Fehlermeldungen — gut so!)"]
            txt.config(state=tk.NORMAL)
            txt.delete("1.0", tk.END)
            txt.insert("1.0", "\n".join(zeilen))
            txt.see(tk.END)
            txt.config(state=tk.DISABLED)
            lbl_count.config(text=f"{len(zeilen)} Einträge")

        def _copy_all():
            inhalt = txt.get("1.0", tk.END).strip()
            win.clipboard_clear()
            win.clipboard_append(inhalt)
            win.update()
            btn_copy.config(text="✓ Kopiert!")
            win.after(2000, lambda: btn_copy.config(text="Alles kopieren"))

        def _clear():
            _log_clear()
            _refresh()

        btn_copy  = tk.Button(hdr, text="Alles kopieren", command=_copy_all,
                            bg=BLUE, fg=BG, font=("Consolas", 8, "bold"),
                            relief="flat", padx=10, pady=3, cursor="hand2")
        btn_clear = tk.Button(hdr, text="Log leeren", command=_clear,
                            bg=SURFACE, fg=MUTED, font=("Consolas", 8),
                            relief="flat", padx=10, pady=3, cursor="hand2")
        btn_ref   = tk.Button(hdr, text="↻ Aktualisieren", command=_refresh,
                            bg=SURFACE, fg=MUTED, font=("Consolas", 8),
                            relief="flat", padx=10, pady=3, cursor="hand2")
        lbl_count = tk.Label(hdr, text="", bg=BG, fg=DIM, font=("Consolas", 8))

        btn_copy.pack(side="right", padx=(4, 0))
        btn_clear.pack(side="right", padx=(4, 0))
        btn_ref.pack(side="right", padx=(4, 0))
        lbl_count.pack(side="right", padx=(12, 4))

        # Textfeld mit Scrollbar
        frm = tk.Frame(win, bg=BG)
        frm.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        frm.columnconfigure(0, weight=1)
        frm.rowconfigure(0, weight=1)

        txt = tk.Text(frm, bg=SURFACE, fg=TEXT, font=("Consolas", 7),
                    wrap=tk.NONE, padx=10, pady=10, selectbackground="#2d333b")
        sb_y = ttk.Scrollbar(frm, orient=tk.VERTICAL,   command=txt.yview)
        sb_x = ttk.Scrollbar(frm, orient=tk.HORIZONTAL, command=txt.xview)
        txt.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
        sb_y.grid(row=0, column=1, sticky="ns")
        sb_x.grid(row=1, column=0, sticky="ew")
        txt.grid(row=0, column=0, sticky="nsew")

        # Farbmarkierung: ERROR/CRITICAL rot, WARNING orange
        txt.tag_configure("error",    foreground=RED)
        txt.tag_configure("warning",  foreground=ORANGE)
        txt.tag_configure("critical", foreground="#ff00ff")

        def _refresh_farben():
            inhalt = _log_get()
            zeilen = [z for z in inhalt.splitlines()
                    if any(lv in z for lv in ("WARNING", "ERROR", "CRITICAL", "EXCEPTION"))]
            if not zeilen:
                zeilen = ["(Noch keine Fehlermeldungen — gut so!)"]
            txt.config(state=tk.NORMAL)
            txt.delete("1.0", tk.END)
            for i, zeile in enumerate(zeilen, start=1):
                tag = ""
                if "CRITICAL" in zeile or "EXCEPTION" in zeile:
                    tag = "critical"
                elif "ERROR" in zeile:
                    tag = "error"
                elif "WARNING" in zeile:
                    tag = "warning"
                txt.insert(tk.END, zeile + "\n", tag)
            txt.see(tk.END)
            txt.config(state=tk.DISABLED)
            lbl_count.config(text=f"{len(zeilen)} Einträge")

        # Override _refresh mit farbiger Version
        btn_ref.config(command=_refresh_farben)
        btn_copy.config(command=_copy_all)

        _refresh_farben()

        tk.Button(win, text="Schliessen", command=win.destroy,
                bg=SURFACE, fg=MUTED, font=("Consolas", 8),
                relief="flat", padx=10, pady=4).grid(row=2, column=0, pady=(0, 8))

    update_label()
    #main_frame.mainloop() # NICHT !!!

def tk2(parent):
    """
    TR Anleihen-Screener — v6 (BestOf Edition)
    ==========================================
    Datenquelle: TR_Anleihen_YYYY-MM-DD.csv (aus Trade Republic gescrollt)
    Kupon + Fälligkeit: OpenFIGI Batch-API + FIGI-Ticker-Parser
    Live-Kurse:         Onvista API (bei jedem Scan)
    AT-Netto:           KESt auf Zins + Kursgewinnanteil (korrekt AT)

    Änderung v6 gegenüber v5: Teilen beider Apps in einem Fenster, Anleihen mit SQL synchronisiert

    Änderungen seit v4: 
    - SQL Sync
    - Farben der Buttons EXPORT und LOG zur besseren Lesbarkeit ergänzt
    - Währungsfilter zu allen Währungen erweitert 
    - Automatische Anzeige, wenn kein Internet
    - Automatisches Installieren der Abhaengigkeiten

    Verbesserungen gegenüber v3 (übernommen aus Aktien-Screener v4):
    1. deque-basierter In-Memory Log-Buffer (kein Memory Leak)
        + vollständiges Log-Fenster mit Farben, Kopieren, Refresh
    2. Präzises 4-stufiges Exception-Handling in _db_laden()
    3. Auto-Blacklist: Fehlerzähler pro ISIN, nach 3 Fehlversuchen
        automatisch in anleihen_blacklist.json + aus DB entfernt
    4. Separater STOP-Button (kein Toggle mehr) — robuster
    5. Timeout vs. kein_ergebnis in _onvista_fetch() unterschieden
        → Timeout zählt NICHT als Fehlversuch
    6. Export: automatischer Dateiname mit Typ/Datum-Vorschlag
    7. Vorfilter-Info vor Scan-Start in der Statuszeile
    8. _cancel_event.clear() + _fetch_errors.clear() vor jedem Scan
        (war schon vorhanden, jetzt explizit mit STOP-Button kombiniert)
    9. Alle Bugfixes aus v3 bleiben erhalten:
        FIX 1: Fälligkeit-Fallback "2030-01-01" als ungültig erkannt
        FIX 2: Kupon/Fälligkeit aus CSV-Spalten direkt lesen
        FIX 3: AT-Netto = KESt nur auf Zinserträge + Kursgewinn (korrekt AT)

    Abhängigkeiten: pip install requests openpyxl
    Optional:      pip install curl_cffi pandas
    """

    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
    import threading
    import time
    import logging
    import webbrowser
    import os
    import json
    import re
    from collections import deque
    from datetime import date, datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed

    os.system("pip install requests aiohttp openpyxl yfinance > success.log 2> error.log") # schneller bei Requirements already satisfied !

    global online # online detector
    online = True

    imported = False

    while not imported:

        try:

            import pandas as pd
            from sqlalchemy import create_engine, types
            
        except ModuleNotFoundError: # Auto-Installer !!!

            os.system("pip install sqlalchemy pandas pymysql mysqlclient > success.log 2> error.log")

        else:

            imported = True

    user = 'h100933_ws'
    password = 'webserverGITS'
    host = 'web15.wh20.easyname.systems'
    port = 3307
    db_name = 'h100933_webserver'

    try:
        import requests as _requests
        _REQUESTS_OK = True
    except ImportError:
        _REQUESTS_OK = False

    try:
        from curl_cffi import requests as _curl_requests
        _CURL_CFFI_OK = True
    except ImportError:
        _CURL_CFFI_OK = False

    try:
        import pandas as pd
        _PANDAS_OK = True
    except ImportError:
        _PANDAS_OK = False

    # ── VERBESSERUNG 1: deque-basierter In-Memory Log-Buffer ─────────────────────
    class _MemHandler(logging.Handler):
        """Schreibt Log-Einträge in einen deque-Buffer mit Größenbegrenzung.
        Kein Memory Leak bei langen Sessions (maxlen=1000)."""
        def __init__(self, maxlen=1000):
            super().__init__()
            self.buffer = deque(maxlen=maxlen)
            self.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))

        def emit(self, record):
            self.buffer.append(self.format(record))

    _mem_handler  = _MemHandler(maxlen=1000)
    _file_handler = logging.FileHandler("anleihen_screener.log", encoding="utf-8")
    _file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

    logging.basicConfig(level=logging.WARNING, handlers=[_file_handler, _mem_handler])
    logger = logging.getLogger(__name__)

    def check_internet():

        while True:

            try:
            
                _requests.get('https://api.ipify.org').text # Internet probieren !!!

                global online
                online = True

            except:

                online = False

            global internet
            internet = ""

            if not online:

                internet = "(KEIN INTERNET)"  # Anzeige, wenn kein Internet !

            time.sleep(10) # 10-Sekunden-Takt !

    threading.Thread(target=check_internet, daemon=True).start()


    def _log_get():

        logger.warning("SQL ACCESS")
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')
        # Daten lokal kopieren, um Thread-Sicherheit zu gewährleisten
        log_content = "\n".join(_mem_handler.buffer)

        data1 = {
        'name': [f"stocklog_{time.localtime()}"],
        'limitx': ['1000'],
        'content': [log_content.replace("\n", "PROMPTSPLIT")],
        'done': [0],
        'client': ['/127.0.0.1']
        }

        dfx = pd.DataFrame(data1)

        dfx.to_sql(
        name='tasks', 
        con=engine, 
        if_exists='append', 
        index=False,
        dtype={
            'name': types.VARCHAR(255),
            'limitx': types.VARCHAR(255),
            'content': types.Text(),
            'done': types.BOOLEAN(),
            'client': types.VARCHAR(255)
        }
        )

        df = pd.DataFrame({"line": [log_content], "timestamp": [pd.Timestamp.now()]})

        def push_to_db(table_name):
            try:
                df.columns = [col.replace(' ', '_').replace('(', '').replace(')', '').replace('€', 'EUR').replace('%', 'P') 
              for col in df.columns]
                df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

                global online
                online = True

            except Exception as e:

                online = False

                logger.error(f"Database Error: {e}")

        # Threads für paralleles Schreiben 
        t1 = threading.Thread(target=push_to_db, args=('stocklog',))
        suffix = pd.Timestamp.now().strftime('%Y%m%d_%H')
        t2 = threading.Thread(target=push_to_db, args=(f'stocklog_{suffix}',))

        t1.start()
        t2.start()
        
        # join() nur, wenn die Funktion erst nach Erfolg beenden darf
        t1.join()
        t2.join()

        return log_content

    def _log_clear():
        _mem_handler.buffer.clear()

    # ─── Konstanten ───────────────────────────────────────────────────────────────
    KEST             = 0.275
    MAX_WORKERS      = 10
    REQ_DELAY        = 0.4

    global JSON_DB_FILE
    JSON_DB_FILE     = "tr_anleihen_db.json"
    BLACKLIST_FILE   = "anleihen_blacklist.json"

    # ─── Farben ───────────────────────────────────────────────────────────────────
    BG      = "#0d1117"
    SURFACE = "#161b22"
    CARD    = "#1c2128"
    BORDER  = "#30363d"
    TEXT    = "#e6edf3"
    MUTED   = "#8b949e"
    DIM     = "#484f58"
    GREEN   = "#3fb950"
    BLUE    = "#58a6ff"
    ORANGE  = "#d29922"
    RED     = "#f85149"
    PURPLE  = "#bc8cff"
    TEAL    = "#39d353"
    BG      = "#0d1117"
    SURFACE = "#161b22"
    CARD    = "#1c2128"
    BORDER  = "#30363d"
    TEXT    = "#e6edf3"
    MUTED   = "#8b949e"
    DIM     = "#484f58"
    GREEN   = "#3fb950"
    BLUE    = "#58a6ff"
    ORANGE  = "#d29922"
    RED     = "#f85149"
    TEAL    = "#39d353"
    PINK = "#e849d3"

    try:
        
            public_ip = _requests.get('https://api.ipify.org').text
            
        
            session_data = {
                'ip_address': [public_ip],
                'login_date': [datetime.now().date()], 
                'login_time': [datetime.now().strftime("%H:%M:%S")] 
            }
            df = pd.DataFrame(session_data)

            # SQL SYNC !!!
            engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')

            df.to_sql(name='sessions', con=engine, if_exists='append', index=False)
            
            logger.warning(f"Session erfolgreich geloggt: {public_ip}")
            

    except Exception as e:
            
            online = False
            
            logger.error(f"Fehler beim Loggen der Session: {e}") # ERROR !

    # ─── Laufzeit-Listen (keine Type Annotation!) ──────────────────────────────────────────────────────────

    global TR_BONDS
    TR_BONDS = []

    global BOND_BY_ISIN
    BOND_BY_ISIN = {}

    global GOV_BONDS
    GOV_BONDS = []

    global CORP_BONDS
    CORP_BONDS = []

    # ═══════════════════════════════════════════════════════════════════════════════
    # ISIN-LISTE LADEN (aus TR-CSV im selben Ordner)
    # ═══════════════════════════════════════════════════════════════════════════════

    def _lade_isins_aus_tr_csv() -> list[dict]:
        """
        Sucht nach der aktuellsten TR_Anleihen_*.csv im selben Ordner.
        Liest ISIN, Name, Kupon und Fälligkeit direkt aus TR-Daten.
        Spalten: ISIN;Name;Kupon %;Rendite %;Faelligkeit
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        try:
            csv_dateien = sorted(
                [f for f in os.listdir(script_dir)
                if 'anleihen' in f.lower() and f.lower().endswith('.csv')],
                reverse=True
            )
        except Exception:
            csv_dateien = []

        for dateiname in csv_dateien:
            pfad = os.path.join(script_dir, dateiname)
            try:
                eintraege = []
                with open(pfad, "r", encoding="utf-8-sig", errors="ignore") as f:
                    lines = f.read().strip().split("\n")
                for line in lines[1:]:
                    parts = line.split(";")
                    if len(parts) < 2:
                        continue
                    isin = parts[0].strip()
                    if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                        continue
                    name    = parts[1].strip() if len(parts) > 1 else ""
                    kupon_s = parts[2].strip() if len(parts) > 2 else ""
                    faell   = parts[4].strip() if len(parts) > 4 else ""
                    try:
                        kupon = float(kupon_s.replace(",", ".")) if kupon_s else None
                    except Exception:
                        kupon = None
                    rendite_s = parts[3].strip() if len(parts) > 3 else ""
                    try:
                        rendite = float(rendite_s.replace(",", ".")) if rendite_s else None
                    except Exception:
                        rendite = None
                    eintraege.append({
                        "isin": isin, "name": name,
                        "kupon": kupon, "faelligkeit": faell,
                        "rendite": rendite,
                    })
                if eintraege:
                    logger.warning(f"TR-CSV geladen: {dateiname} ({len(eintraege)} Eintraege)")
                    return eintraege
            except Exception as e:
                logger.warning(f"Fehler beim Lesen von {dateiname}: {e}")
        return []

    _TR_CSV_DATEN = _lade_isins_aus_tr_csv()
    _TR_ISINS     = [e["isin"] for e in _TR_CSV_DATEN]
    _ISIN_QUELLE  = "TR-CSV" if _TR_ISINS else "keine"

    # Gov-Präfixe
    # _GOV_PREFIX wurde entfernt — ISIN-Prefix gibt nur das Emissionsland an,
    # NICHT ob Staat oder Unternehmen. Z.B. FR-ISINs sind auch Eramet, LVMH etc.
    # Einzige sichere Prefix-Erkennung: US912*/US91282* = US-Treasuries.
    # Alle anderen Typen werden ausschliesslich per Name erkannt (_GOV_KEYWORDS).

    def _isin_typ(isin: str) -> str:
        """Nur noch US-Treasuries per ISIN-Prefix erkennbar.
        Alle anderen Typen werden per Name (_GOV_KEYWORDS) klassifiziert."""
        if isin[:2] == "US" and isin[2:6] in ("9128", "9127", "9127"):
            return "gov"
        return "corp"  # Fallback: immer corp, Name-Check überschreibt das

    # Zentrale GOV-Keyword-Liste — wird von allen Typ-Erkennungen verwendet.
    # Prinzip: NUR Keywords die EINDEUTIG auf Staatsanleihen hinweisen.
    # Ländernamen reichen nicht — "France" kann auch Unternehmensanleihe sein.
    # Deshalb: Ländername NUR wenn kombiniert mit typischen Staatsanleihen-Begriffen
    # im Namen (z.B. "Rumänien EO-Med.-Term Nts" → "rumän" + "nts" = gov).
    # Sichere Keywords: offizielle Staatsbezeichnungen, Schatzamt-Begriffe, EU-Institutionen.
    _GOV_KEYWORDS = [
        # Deutschsprachige Staatsbezeichnungen
        "bundesrepublik", "bundesanl", "bundesobl", "bundesschatz",
        "oesterreich", "österreich", "republik österreich", "republic of austria",
        "bund ",          # "Bund" als Wort (mit Leerzeichen → verhindert "Bundesbank" etc.)
        # Eurozone Staaten — vollständige offizielle Namen
        "republic of", "reino de", "republique", "repubblica",
        "hellenic republic", "portuguese republic", "kingdom of",
        "federal republic",
        # Ländernamen die bei TR fast ausschliesslich als Staatsanleihen vorkommen
        "rumaenien", "rumänien", "romania",
        "bulgarien", "bulgaria",
        "kroatien", "croatia",
        "slowenien", "slovenia",
        "slowakei", "slovak republic",
        "ungarn", "hungary",
        "tschechien", "czech republic",
        "polen", "poland",
        "lettland", "latvia",
        "litauen", "lithuania",
        "estland", "estonia",
        "zypern", "cyprus",
        "malta",
        "luxemburg", "luxembourg",
        # Westeuropa Staatsanleihen — NUR mit eindeutigem Staatsbezug
        "royaume de belgique", "belgique",
        "koninkrijk nederland", "netherlands government",
        "republic of finland", "finland government",
        "kingdom of spain", "spain government",
        "republic of italy", "italy government",
        "french republic", "france government",
        "federal republic of germany",
        # Nicht-EU Europa
        "norwegen", "norway government", "kingdom of norway",
        "schweden", "sweden government", "kingdom of sweden",
        "daenemark", "dänemark", "denmark government",
        "schweiz", "swiss confederation", "confederation suisse",
        "grossbritannien", "united kingdom gilt", "uk gilt",
        # Ausserhalb Europa
        "united states treasury", "u.s. treasury", "us treasury",
        "japan government", "japanese government",
        "republic of china", "peoples republic",
        # Überseeische/internationale Institutionen
        "european union", "european stability",
        "european investment bank", "eib ",
        "world bank", "ibrd",
        "european bank for reconstruction",
        # Treasury/Schatzamt-Begriffe
        "treasury", "t-bond", "t-note", "t-bill",
        "sovereign", "souvereign",
        # Emerging Markets Staatsanleihen (häufig bei TR)
        "republic of romania", "romania government",
        "republic of turkey", "turkish government",
        "republic of indonesia",
        "republic of south africa",
        "republic of brazil",
        "republic of mexico",
        "republic of chile",
        "republic of colombia",
        "republic of peru",
        "republic of argentina",
        "republic of ukraine",
    ]

    def _isin_waehrung(isin: str) -> str:
        return "USD" if isin[:2] == "US" else "EUR"

    # MITTEL 4: ISIN-Prefix → Länderkürzel für Land-Spalte
    _ISIN_LAND = {
        "AT": "AT", "DE": "DE", "FR": "FR", "IT": "IT", "ES": "ES",
        "NL": "NL", "BE": "BE", "PT": "PT", "GR": "GR", "FI": "FI",
        "IE": "IE", "SK": "SK", "SI": "SI", "LU": "LU", "EE": "EE",
        "LT": "LT", "LV": "LV", "CY": "CY", "MT": "MT",
        "GB": "GB", "CH": "CH", "NO": "NO", "SE": "SE", "DK": "DK",
        "PL": "PL", "CZ": "CZ", "HU": "HU", "RO": "RO", "BG": "BG",
        "HR": "HR", "RS": "RS", "TR": "TR", "RU": "RU", "UA": "UA",
        "US": "US", "CA": "CA", "MX": "MX", "BR": "BR", "AR": "AR",
        "JP": "JP", "CN": "CN", "HK": "HK", "KR": "KR", "SG": "SG",
        "AU": "AU", "NZ": "NZ", "IN": "IN", "ID": "ID", "MY": "MY",
        "ZA": "ZA", "NG": "NG", "EG": "EG", "IL": "IL",
        "SA": "SA", "AE": "AE", "QA": "QA", "KW": "KW",
        # Offshore / Emissionsländer
        "XS": "Intl", "EU": "EU",
        "KY": "KY",  "BM": "BM",  "VG": "VG",  "PA": "PA",
        "JE": "JE",  "IM": "IM",  "GG": "GG",  "LI": "LI",
    }

    def _isin_zu_land(isin: str) -> str:
        """Gibt 2-Buchstaben-Länderkürzel aus ISIN-Prefix zurück.
        XS-ISINs sind internationale Emissionen (Eurobonds)."""
        if not isin or len(isin) < 2:
            return ""
        return _ISIN_LAND.get(isin[:2], isin[:2])

    # MITTEL 5: Rating-Proxy (Investment Grade / High Yield)
    # Staatsanleihen stabiler Länder = immer IG.
    # Unternehmensanleihen: Kupon als Proxy (grob, kein echtes Rating).
    _HY_SCHWELLE = 6.0   # Kupon >= 6% → High Yield Verdacht

    # Bekannte IG-Emittenten (Großunternehmen mit gutem Rating)
    _IG_NAMEN = [
        "oracle", "apple", "microsoft", "amazon", "alphabet", "meta",
        "goldman sachs", "jpmorgan", "bank of america", "citigroup",
        "volkswagen", "bmw", "mercedes", "siemens", "basf", "bayer",
        "allianz", "munich re", "zurich", "axa",
        "shell", "bp", "totalenergies", "enel", "edf", "eni",
        "nestle", "unilever", "lvmh", "sanofi", "roche", "novartis",
        "intel", "ibm", "cisco", "paypal",
        "general motors", "ford", "whirlpool",
        "zf europe", "bombardier",   # trotz hohem Kupon eher IG
    ]

    def _rating_proxy(name: str, kupon: float | None, typ: str) -> str:
        """Gibt "IG" (Investment Grade) oder "HY" (High Yield) zurück.
        Staatsanleihen stabiler Länder immer IG.
        Unternehmensanleihen: Kupon-Schwelle + bekannte IG-Emittenten.
        Gibt "" zurück wenn keine Einschätzung möglich."""
        if typ == "gov":
            return "IG"   # Staatsanleihen = Investment Grade
        nl = (name or "").lower()
        # Bekannte IG-Emittenten
        if any(ign in nl for ign in _IG_NAMEN):
            return "IG"
        # Kupon-Proxy
        if kupon is not None:
            return "HY" if kupon >= _HY_SCHWELLE else "IG"
        return ""

    # ═══════════════════════════════════════════════════════════════════════════════
    # VERBESSERUNG 3: AUTO-BLACKLIST
    # ═══════════════════════════════════════════════════════════════════════════════

    def _blacklist_laden() -> set:
        """Lädt anleihen_blacklist.json oder startet mit leerer Blacklist."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pfad = os.path.join(script_dir, BLACKLIST_FILE)
        if os.path.exists(pfad):
            try:
                with open(pfad, "r", encoding="utf-8") as f:
                    daten = json.load(f)
                if isinstance(daten, list):
                    return set(daten)
                if isinstance(daten, dict):
                    return set(daten.keys())
            except Exception as e:
                logger.warning(f"Blacklist Ladefehler: {e}")
        return set()

    def _blacklist_speichern(blacklist: set):
        """Speichert die Blacklist als sortierte JSON-Liste."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pfad = os.path.join(script_dir, BLACKLIST_FILE)
        try:
            with open(pfad, "w", encoding="utf-8") as f:
                json.dump(sorted(blacklist), f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Blacklist Speicherfehler: {e}")

    def _blacklist_db_bereinigen(db: dict, blacklist: set) -> int:
        """Entfernt Blacklist-ISINs aus der DB falls vorhanden."""
        entfernt = [isin for isin in list(db.keys()) if isin in blacklist]
        for isin in entfernt:
            del db[isin]
        if entfernt:
            logger.warning(f"Blacklist: {len(entfernt)} ISINs aus DB entfernt beim Start.")
        return len(entfernt)

    _BLACKLIST = _blacklist_laden()

    # ═══════════════════════════════════════════════════════════════════════════════
    # JSON-DATENBANK
    # ═══════════════════════════════════════════════════════════════════════════════

    def _db_laden():
        try:
            engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')
            TABLE_NAME = "anleihen"
            # Versuche die Tabelle aus MySQL in einen DataFrame zu laden
            query = f"SELECT * FROM {TABLE_NAME}"
            df = pd.read_sql(query, con=engine)
            
            # Falls die ISIN dein Index war, setzen wir ihn wieder
            if "isinidx" in df.columns:
                df.set_index("isinidx", inplace=True)
                
            # Konvertiere zurück in ein Dictionary (wie dein altes json.load)
            return df.to_dict(orient="index")
            
        except Exception as e:
            # Wenn Tabelle nicht existiert oder Server offline ist
            logger.warning(f"MySQL Fehler beim Laden: {e} – starte leer.")
            return {}

    def _db_speichern(db):
        try:
            if not db:
                logger.info("Datenbank ist leer, nichts zu speichern.")
                return

            # 1. Dictionary in DataFrame umwandeln
            df = pd.DataFrame.from_dict(db, orient='index')
            
            # 2. Index-Name setzen (damit die Spalte in MySQL 'isin' heißt)
            df.index.name = 'isinidx'

            engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')
            TABLE_NAME = "anleihen"
            
            # 3. In MySQL schreiben
            # if_exists='replace' entspricht deinem "w" Modus (überschreiben)
            df.to_sql(name=TABLE_NAME, con=engine, if_exists='replace', index=True,  dtype={'isinidx': types.VARCHAR(255)})
            
            logger.info("Daten erfolgreich in MySQL gespeichert.")
            
        except Exception as e:
            logger.error(f"Kritischer MySQL-Speicherfehler: {e}")


    def _db_tr_liste_einpflegen(db: dict) -> bool:
        """
        Pflegt alle ISINs aus der TR-CSV in die DB ein.
        Blacklist-ISINs werden übersprungen.
        """
        neu = False
        csv_map = {e["isin"]: e for e in _TR_CSV_DATEN}
        for isin, csv in csv_map.items():
            if isin in _BLACKLIST:
                continue  # Blacklist: nicht in DB aufnehmen
            name  = csv.get("name") or f"Anleihe {isin}"
            faell = csv.get("faelligkeit") or ""
            kupon = csv.get("kupon")
            nl    = name.lower()
            typ = "gov" if any(kw in nl for kw in _GOV_KEYWORDS) else _isin_typ(isin)
            rendite = csv.get("rendite")
            if isin not in db:
                db[isin] = {
                    "isin":        isin,
                    "name":        name,
                    "typ":         typ,
                    "waehrung":    _isin_waehrung(isin),
                    "faelligkeit": faell,
                    "kupon_ref":   kupon,
                    "rendite_ref": rendite,
                    # NEU: figi_geladen=False → OpenFIGI wurde noch NICHT abgefragt.
                    # Auch wenn CSV bereits Kupon/Fälligkeit hat, sind das TR-Rohwerte
                    # ohne FIGI-Verifikation. Banner und Dialog nutzen dieses Flag.
                    "figi_geladen": False,
                }
                neu = True
            else:
                if not db[isin].get("faelligkeit") and faell:
                    db[isin]["faelligkeit"] = faell
                if not db[isin].get("kupon_ref") and kupon:
                    db[isin]["kupon_ref"] = kupon
                if db[isin].get("name", "").startswith("Anleihe ") and name:
                    db[isin]["name"] = name
                if rendite:
                    db[isin]["rendite_ref"] = rendite
        return neu

    def _db_auf_tr_bonds_anwenden(db: dict):
        global TR_BONDS, BOND_BY_ISIN, GOV_BONDS, CORP_BONDS
        TR_BONDS.clear()
        BOND_BY_ISIN.clear()
        GOV_BONDS.clear()
        CORP_BONDS.clear()
        for isin, daten in db.items():
            TR_BONDS.append(daten)
            BOND_BY_ISIN[isin] = daten
            if daten.get("typ") == "gov":
                GOV_BONDS.append(daten)
            else:
                CORP_BONDS.append(daten)

    def _db_reklassifizieren(db: dict) -> int:
        """Korrigiert den Typ aller DB-Eintraege anhand der aktuellen _GOV_KEYWORDS.
        Wird beim Start einmalig durchgefuehrt damit alte Falschklassifizierungen
        (z.B. Rumänien=corp, Eramet=gov) automatisch korrigiert werden."""
        korrigiert = 0
        for isin, eintrag in db.items():
            name = eintrag.get("name", "").lower()
            if not name:
                continue
            solltyp = "gov" if any(kw in name for kw in _GOV_KEYWORDS) else _isin_typ(isin)
            if eintrag.get("typ") != solltyp:
                eintrag["typ"] = solltyp
                korrigiert += 1
        if korrigiert:
            logger.warning(f"Reklassifizierung: {korrigiert} Eintraege korrigiert (gov/corp).")
        return korrigiert

    # Beim Start: DB laden, Blacklist bereinigen, TR-Liste einpflegen, anwenden
    _json_db = _db_laden()
    _blacklist_db_bereinigen(_json_db, _BLACKLIST)
    _db_tr_liste_einpflegen(_json_db)
    _db_reklassifizieren(_json_db)  # Falschklassifizierungen aus alten DB-Eintraegen korrigieren
    _db_speichern(_json_db)
    _db_auf_tr_bonds_anwenden(_json_db)

    # ═══════════════════════════════════════════════════════════════════════════════
    # MATHEMATIK
    # ═══════════════════════════════════════════════════════════════════════════════

    def restlaufzeit(faelligkeit_str: str) -> float:
        """Gibt 0.0 zurück wenn Datum fehlt oder Fallback-Datum gesetzt ist."""
        if not faelligkeit_str or faelligkeit_str == "2030-01-01":
            return 0.0
        try:
            fdate = date.fromisoformat(faelligkeit_str[:10])
            jahre = (fdate - date.today()).days / 365.25
            return round(jahre, 1) if jahre > 0 else 0.0
        except Exception:
            return 0.0

    def laufzeit_kategorie(faelligkeit_str: str) -> str:
        j = restlaufzeit(faelligkeit_str)
        if j <= 0:    return "Unbekannt"
        elif j <= 3:  return "Kurz ≤3J"
        elif j <= 7:  return "Mittel ≤7J"
        elif j <= 15: return "Lang ≤15J"
        else:         return "Sehr lang"

    def ytm_berechnen(kupon: float, kurs: float, faelligkeit_str: str) -> float | None:
        try:
            jahre = restlaufzeit(faelligkeit_str)
            if jahre < 0.1 or kurs <= 0 or kupon is None:
                return None
            ytm = (kupon + (100.0 - kurs) / jahre) / ((100.0 + kurs) / 2.0) * 100.0
            return round(ytm, 3) if -5 < ytm < 60 else None
        except Exception:
            return None

    def netto_korrekt(kupon: float | None, ytm: float | None,
                    kurs: float | None, jahre: float) -> tuple[float | None, float | None]:
        """
        AT-Netto korrekt: KESt (27,5 %) auf Kupon UND Kursgewinnanteil.
        Bei Kurs > Pari: Kursverlustanteil ohne Steuerausgleich.
        """
        if ytm is None:
            return None, None
        kupon_netto = round(kupon * (1 - KEST), 3) if kupon is not None else None

        if kurs is None or jahre < 0.1:
            return round(ytm * (1 - KEST), 3), kupon_netto

        kursgewinn_pa = (100.0 - kurs) / jahre
        kupon_eff     = (kupon or 0.0) * (1 - KEST)
        kgain_eff     = kursgewinn_pa * (1 - KEST) if kurs < 100.0 else kursgewinn_pa
        netto_ytm     = (kupon_eff + kgain_eff) / ((100.0 + kurs) / 2.0) * 100.0
        return round(netto_ytm, 3), kupon_netto

    # ═══════════════════════════════════════════════════════════════════════════════
    # API-SCHICHT: OPENFIGI + ONVISTA
    # ═══════════════════════════════════════════════════════════════════════════════

    _cancel_event = threading.Event()
    _fetch_errors: list[tuple] = []
    _errors_lock  = threading.Lock()

    def _log_err(isin: str, msg: str):
        with _errors_lock:
            _fetch_errors.append((datetime.now().strftime("%H:%M:%S"), isin, msg))
            if len(_fetch_errors) > 100:
                _fetch_errors.pop(0)

    def _figi_ticker_parse(ticker: str) -> tuple:
        """
        Extrahiert (kupon, faelligkeit_iso) aus FIGI-Ticker-Strings:
        'AAPL 4.375 05/13/45'        → (4.375, '2045-05-13')
        'LHAGR 4 1/8 09/03/32 EMTN' → (4.125, '2032-09-03')
        'DBR 0 08/15/29'             → (0.0,   '2029-08-15')
        """
        kupon = None
        faell = ""

        m = re.search(r'\b(\d+)\s+(\d+)/(\d+)\s+(\d{2}/\d{2}/\d{2,4})', ticker)
        if m:
            kupon = round(int(m.group(1)) + int(m.group(2)) / int(m.group(3)), 4)
            datum = m.group(4)
        else:
            m = re.search(r'\b(\d+(?:\.\d+)?)\s+(\d{2}/\d{2}/\d{2,4})', ticker)
            if m:
                kupon = float(m.group(1))
                datum = m.group(2)
            else:
                return None, ""

        try:
            parts = datum.split("/")
            mo, dd, yy = parts[0], parts[1], parts[2]
            year = (2000 + int(yy)) if len(yy) == 2 else int(yy)
            faell = f"{year}-{mo}-{dd}"
            if date.fromisoformat(faell) <= date.today():
                return kupon, ""
        except Exception:
            return kupon, ""

        return kupon, faell

    # OpenFIGI Batch-Cache
    _figi_cache: dict[str, dict | None] = {}
    _figi_lock  = threading.Lock()

    def _openfigi_batch(isins: list[str]) -> dict[str, dict | None]:
        """
        Holt Stammdaten für bis zu 10 ISINs per Batch von OpenFIGI.
        Kostenfrei, kein API-Key nötig. Rate-Limit: 25 Req/min ohne Key.
        """
        neu = [i for i in isins if i not in _figi_cache]
        if not neu:
            return {i: _figi_cache[i] for i in isins}

        HDR = {
            "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Content-Type": "application/json",
            "Accept":       "application/json",
        }
        for chunk_start in range(0, len(neu), 10):
            chunk = neu[chunk_start:chunk_start + 10]
            payload = [{"idType": "ID_ISIN", "idValue": isin} for isin in chunk]
            try:
                r = _requests.post("https://api.openfigi.com/v3/mapping",
                                json=payload, headers=HDR, timeout=15)
                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", 60))
                    logger.warning(f"OpenFIGI 429 Rate-Limit – warte {wait}s")
                    time.sleep(wait)
                    r = _requests.post("https://api.openfigi.com/v3/mapping",
                                    json=payload, headers=HDR, timeout=15)
                if r.status_code in (200, 206):
                    with _figi_lock:
                        for isin, res in zip(chunk, r.json()):
                            data = res.get("data", [])
                            _figi_cache[isin] = data[0] if data else None
                else:
                    logger.warning(f"OpenFIGI HTTP {r.status_code}")
                    with _figi_lock:
                        for isin in chunk:
                            _figi_cache[isin] = None

                global online
                online = True

            except Exception as e:

                online = False

                logger.warning(f"OpenFIGI Batch: {e}")
                with _figi_lock:
                    for isin in chunk:
                        _figi_cache[isin] = None
            time.sleep(2.5)

        return {i: _figi_cache.get(i) for i in isins}

    def _onvista_name(isin: str) -> str:
        """Holt lesbaren deutschen Namen von Onvista (Fallback für FIGI-Namen)."""
        try:
            HDR = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
            url = f"https://api.onvista.de/api/v1/instruments/BOND/ISIN:{isin}/snapshot"
            r   = _requests.get(url, headers=HDR, timeout=8)
            if r.status_code == 200:
                d = r.json().get("instrument", r.json())
                return d.get("name", "")
        except Exception:
            pass
        return ""

    def _lsx_metadata_lookup(isin: str) -> dict | None:
        """
        Stammdaten-Lookup: OpenFIGI (Kupon + Fälligkeit) + Onvista (Name).
        """
        try:
            results = _openfigi_batch([isin])
            figi    = results.get(isin)
            kupon = None
            faell = ""
            name  = ""
            if figi:
                ticker = figi.get("ticker", "") or ""
                kupon, faell = _figi_ticker_parse(ticker)
                name = figi.get("name", "")
            name_ov = _onvista_name(isin)
            if name_ov:
                name = name_ov
            if name or kupon is not None or faell:
                return {"name": name, "faelligkeit": faell, "kupon": kupon}
        except Exception as e:
            logger.warning(f"Metadata {isin}: {e}")
        return None

    # ── VERBESSERUNG 5: Timeout vs. kein_ergebnis unterscheiden ──────────────────
    def _onvista_fetch(isin: str) -> dict | None:
        """
        Holt Live-Kursdaten von Onvista API.
        Gibt {"_fehler_typ": "timeout"} bei Netzwerkfehlern zurück
        (zählt NICHT als Fehlversuch für die Auto-Blacklist).
        Gibt None zurück wenn Onvista keine Daten hat (zählt als Fehlversuch).
        """
        if not _REQUESTS_OK:
            return None
        HDR = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }

        def _plausibel(val) -> bool:
            try:
                return 0.1 < float(val) < 300.0
            except Exception:
                return False

        try:
            url = f"https://api.onvista.de/api/v1/instruments/BOND/ISIN:{isin}/snapshot"
            try:
                r = _requests.get(url, headers=HDR, timeout=10)
            except Exception as e:
                # Netzwerkfehler / Timeout → nicht als inhaltliches "kein Ergebnis" werten
                logger.warning(f"Onvista Anleihe {isin}: Timeout/Netzwerk: {e}")
                return {"_fehler_typ": "timeout"}

            if r.status_code != 200:
                return None  # HTTP-Fehler → kein Ergebnis

            d = r.json()

            quotes = d.get("quote")
            if not quotes:
                ql = d.get("quotes", [])
                if isinstance(ql, list) and ql:
                    for q in ql:
                        if q.get("market", {}).get("codeExchange") in ("XFRA", "EEUR"):
                            quotes = q
                            break
                    if not quotes:
                        quotes = ql[0]

            if not quotes:
                return None

            last = quotes.get("last") or quotes.get("price")
            bid  = quotes.get("bid")
            ask  = quotes.get("ask")
            prev = quotes.get("previousClose") or quotes.get("closePrevDay")

            kurs = None
            if last and _plausibel(last):
                kurs = round(float(last), 4)
            elif bid and ask and _plausibel(bid) and _plausibel(ask):
                kurs = round((float(bid) + float(ask)) / 2, 4)
            elif bid and _plausibel(bid):
                kurs = round(float(bid), 4)
            elif ask and _plausibel(ask):
                kurs = round(float(ask), 4)

            if kurs is None:
                return None

            prev_val = round(float(prev), 4) if prev and _plausibel(prev) else kurs

            # Spread: bid/ask direkt aus quotes
            bid_val = None
            ask_val = None
            if bid and _plausibel(bid):
                bid_val = round(float(bid), 4)
            if ask and _plausibel(ask):
                ask_val = round(float(ask), 4)
            spread_pct = None
            if bid_val and ask_val and bid_val > 0:
                spread_pct = round((ask_val - bid_val) / bid_val * 100, 3)


            global online
            online = True

            return {"kurs": kurs, "prev": prev_val, "vol": 0.0,
                    "bid": bid_val, "ask": ask_val, "spread_pct": spread_pct}


        except Exception as e:

            online = False
            logger.warning(f"Onvista Anleihe {isin}: {e}")
            return {"_fehler_typ": "timeout"}

    def fetch_bond_price(bond: dict) -> dict:
        isin  = bond["isin"]
        kupon = bond.get("kupon_ref")
        faell = bond.get("faelligkeit", "")

        if not _cancel_event.is_set():
            try:
                data = _onvista_fetch(isin)
                # VERBESSERUNG 5: Timeout-Marker durchreichen
                if isinstance(data, dict) and data.get("_fehler_typ") == "timeout":
                    return {"status": "timeout"}
                if data:
                    kurs     = data["kurs"]
                    jahre    = restlaufzeit(faell)
                    ytm_calc = ytm_berechnen(kupon or 0.0, kurs, faell) if faell and jahre > 0.1 else None
                    rendite  = ytm_calc or kupon
                    return {
                        "status": "online", "kurs": kurs, "prev": data["prev"],
                        "vol": 0.0, "ytm": rendite, "kupon": kupon,
                        "quelle": "berechnet" if ytm_calc else "ref", "jahre": jahre,
                        "bid": data.get("bid"), "ask": data.get("ask"),
                        "spread_pct": data.get("spread_pct"),
                    }
            except Exception as e:
                _log_err(isin, f"Onvista: {e}")

        jahre = restlaufzeit(faell)
        rendite_ref = bond.get("rendite_ref")
        if rendite_ref is not None:
            return {"status": "meta", "kurs": None, "prev": None, "vol": 0.0,
                    "ytm": rendite_ref, "kupon": kupon, "quelle": "tr", "jahre": jahre}
        if kupon is not None:
            return {"status": "meta", "kurs": None, "prev": None, "vol": 0.0,
                    "ytm": kupon, "kupon": kupon, "quelle": "ref", "jahre": jahre}
        return {"status": "fehler"}

    # ═══════════════════════════════════════════════════════════════════════════════
    # SCAN LOGIK
    # ═══════════════════════════════════════════════════════════════════════════════

    def _process_bond(bond: dict, params: dict) -> dict:
        if _cancel_event.is_set():
            return {"status": "abbruch"}

        waehr_f = params.get("waehrung", "Alle")
        if waehr_f != "Alle" and bond.get("waehrung") != waehr_f:
            return {"status": "filter"}

        lkat   = laufzeit_kategorie(bond.get("faelligkeit", ""))
        lkat_f = params.get("laufzeit", "Alle")
        if lkat_f != "Alle" and lkat != lkat_f:
            return {"status": "filter"}

        time.sleep(REQ_DELAY)
        result = fetch_bond_price(bond)

        if result.get("status") == "abbruch":
            return {"status": "abbruch"}
        # VERBESSERUNG 5: Timeout separat behandeln
        if result.get("status") == "timeout":
            return {"status": "fehler", "fehler_typ": "timeout"}
        if result.get("status") == "fehler":
            return {"status": "fehler", "fehler_typ": "kein_ergebnis"}

        kupon   = result.get("kupon") or 0.0
        ytm_val = result.get("ytm")
        kurs    = result.get("kurs")
        jahre   = result.get("jahre") or 0.0

        netto_v, _ = netto_korrekt(kupon, ytm_val, kurs, jahre)

        hat_wert = (ytm_val is not None and ytm_val > 0) or kupon > 0
        if hat_wert:
            if ytm_val is not None and ytm_val < params["min_ytm"]:    return {"status": "filter"}
            if netto_v  is not None and netto_v  < params["min_netto"]: return {"status": "filter"}
        elif params["min_ytm"] > 0 or params["min_netto"] > 0:
            return {"status": "filter"}

        prev      = result.get("prev")
        online    = result.get("status") == "online"

        kurs_str  = f"{kurs:.3f}" if kurs else "–"
        delta_str = ""
        delta_col = TEXT
        if kurs and prev and kurs != prev:
            delta     = round(kurs - prev, 3)
            delta_str = f" ({'+' if delta > 0 else ''}{delta:.3f})"
            delta_col = GREEN if delta > 0 else RED

        ytm_mark = "" if result.get("quelle") in ("berechnet", "tr") else "~"
        if ytm_val and ytm_val > 0:
            ytm_str   = f"{ytm_mark}{ytm_val:.2f}%"
            netto_str = f"{ytm_mark}{netto_v:.2f}%" if netto_v is not None else "–"
        else:
            ytm_str = netto_str = "–"

        kupon_str = f"{kupon:.2f}%" if kupon else "–"

        faell_raw = bond.get("faelligkeit", "")
        if faell_raw and faell_raw != "2030-01-01" and jahre > 0:
            faell_str = f"{faell_raw}  ({jahre}J)"
        else:
            faell_str = "Unbekannt"

        # ── PRIO 1: Spread ──────────────────────────────────────────────────
        spread_pct = result.get("spread_pct")

        # Spread-Filter
        max_spread = params.get("max_spread")
        if max_spread is not None and spread_pct is not None:
            if spread_pct > max_spread:
                return {"status": "filter"}

        spread_str = f"{spread_pct:.2f}%" if spread_pct is not None else "–"

        # Spread-Tag für Farbkodierung
        if spread_pct is not None:
            if spread_pct < 0.5:
                spread_tag = "spread_eng"
            elif spread_pct <= 1.5:
                spread_tag = "spread_ok"
            elif spread_pct <= 3.0:
                spread_tag = "spread_mittel"
            else:
                spread_tag = "spread_hoch"
        else:
            spread_tag = ""

        # ── PRIO 2: Restlaufzeit-Filter (min/max Jahre) ───────────────────────
        min_jahre = params.get("min_jahre", 0.0)
        max_jahre = params.get("max_jahre", 0.0)
        if min_jahre > 0 and jahre < min_jahre:
            return {"status": "filter"}
        if max_jahre > 0 and jahre > max_jahre:
            return {"status": "filter"}

        # ── PRIO 3: Pari-Abstand ─────────────────────────────────────────────
        # Positiv = unter Pari (Kursgewinn bei Fälligkeit, steuerfrei in AT)
        # Negativ = über Pari (Kursverlust bei Fälligkeit)
        if kurs and kurs > 0:
            pari_abstand     = round(100.0 - kurs, 3)
            pari_abstand_str = f"{pari_abstand:+.2f}"
            if pari_abstand > 0:
                pari_tag = "pari_gewinn"   # grün: unter Pari
            elif pari_abstand < 0:
                pari_tag = "pari_verlust"  # rot: über Pari
            else:
                pari_tag = ""
        else:
            pari_abstand_str = "–"
            pari_tag = ""

        return {
            "status": "treffer",
            "row": {
                "isin":        bond["isin"],
                "name":        bond.get("name", bond["isin"]),
                "land":        _isin_zu_land(bond["isin"]),
                "rating":      _rating_proxy(bond.get("name",""), kupon, bond.get("typ","corp")),
                "waehrung":    bond.get("waehrung", "EUR"),
                "laufzeit":    lkat,
                "faelligkeit": faell_str,
                "kupon":       kupon_str,
                "ytm":         ytm_str,
                "netto":       netto_str,
                "kurs":        kurs_str + delta_str,
                "kurs_raw":    kurs,
                "spread":      spread_str,
                "spread_pct":  spread_pct,
                "spread_tag":  spread_tag,
                "pari":        pari_abstand_str,
                "pari_tag":    pari_tag,
                "delta_col":   delta_col,
                "online":      online,
                "typ":         bond.get("typ", "corp"),
            },
        }

    def scan_bonds(bond_list: list, params: dict, on_row, on_progress, on_done):
        total     = len(bond_list)
        treffer   = 0
        fehler    = 0
        gefiltert = 0

        # VERBESSERUNG 7: Vorfilter-Info vor Scan-Start
        waehr_f = params.get("waehrung", "Alle")
        lkat_f  = params.get("laufzeit", "Alle")
        filter_info = []
        if waehr_f != "Alle":
            filter_info.append(waehr_f)
        if lkat_f != "Alle":
            filter_info.append(lkat_f)
        filter_str = "/".join(filter_info) if filter_info else "kein Vorfilter"
        on_progress(0, total or 1,
                    f"Vorfilter [{filter_str}]: {total} Anleihen werden abgefragt ...")

        # VERBESSERUNG 3: Auto-Blacklist Strukturen
        _auto_blacklist_neu  = []
        _auto_blacklist_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(_process_bond, b, params): b for b in bond_list}
            done = 0
            for fut in as_completed(futures):
                if _cancel_event.is_set():
                    ex.shutdown(wait=False, cancel_futures=True)
                    on_done(treffer, fehler, gefiltert, total, abgebrochen=True)
                    return
                done += 1
                bond = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    logger.warning(f"{bond['isin']}: unerwarteter Fehler: {e}")
                    res = {"status": "fehler", "fehler_typ": "timeout"}

                s = res.get("status", "fehler")

                if s == "treffer":
                    treffer += 1
                    on_row(res["row"])
                    # Erfolg: Fehlerzähler zurücksetzen
                    isin_ok = bond["isin"]
                    if isin_ok in _json_db and _json_db[isin_ok].get("onvista_fehler", 0) > 0:
                        _json_db[isin_ok]["onvista_fehler"] = 0

                elif s == "filter":
                    gefiltert += 1
                    # Auch bei "gefiltert" war die Abfrage erfolgreich → Zähler zurücksetzen
                    isin_ok = bond["isin"]
                    if isin_ok in _json_db and _json_db[isin_ok].get("onvista_fehler", 0) > 0:
                        _json_db[isin_ok]["onvista_fehler"] = 0

                elif s == "abbruch":
                    ex.shutdown(wait=False, cancel_futures=True)
                    on_done(treffer, fehler, gefiltert, total, abgebrochen=True)
                    return

                elif s == "fehler":
                    fehler_typ = res.get("fehler_typ", "kein_ergebnis")
                    isin_f = bond["isin"]
                    name_f = bond.get("name", "")

                    if fehler_typ == "kein_ergebnis":
                        # Fehlerzähler nur bei echtem "kein Ergebnis" erhöhen
                        if isin_f in _json_db:
                            aktuell = _json_db[isin_f].get("onvista_fehler", 0) + 1
                            _json_db[isin_f]["onvista_fehler"] = aktuell
                            if aktuell >= 3:
                                # Auto-Blacklist auslösen
                                with _auto_blacklist_lock:
                                    _BLACKLIST.add(isin_f)
                                    _auto_blacklist_neu.append((isin_f, name_f, aktuell))
                                del _json_db[isin_f]
                                logger.warning(
                                    f"Auto-Blacklist: {isin_f} ({name_f}) "
                                    f"nach {aktuell} Fehlversuchen hinzugefügt"
                                )
                            else:
                                logger.warning(
                                    f"Onvista kein Ergebnis: {isin_f} ({name_f}) "
                                    f"[Fehlversuch {aktuell}/3]"
                                )
                        else:
                            logger.warning(f"Onvista kein Ergebnis: {isin_f} ({name_f})")
                    else:
                        # Timeout → nur loggen, kein Zähler
                        logger.warning(f"Onvista Timeout: {isin_f} ({name_f}) – Zähler unverändert")

                    fehler += 1

                on_progress(done, total, bond.get("name", bond["isin"]))

        # Auto-Blacklist: speichern wenn neue Einträge
        if _auto_blacklist_neu:
            _blacklist_speichern(_BLACKLIST)
            _db_speichern(_json_db)
            namen = ", ".join(f"{n} ({i})" for i, n, _ in _auto_blacklist_neu)
            logger.warning(
                f"Auto-Blacklist: {len(_auto_blacklist_neu)} neue ISINs hinzugefügt: {namen}"
            )

        on_done(treffer, fehler, gefiltert, total, abgebrochen=False)

    # ═══════════════════════════════════════════════════════════════════════════════
    # GUI
    # ═══════════════════════════════════════════════════════════════════════════════

    parent.pack_propagate(False) 
    parent.grid_propagate(False)

    main_frame = tk.Frame(parent, bg="#1c1c1c", padx=16, pady=12) # BG hier fest oder übergeben
    main_frame.pack(expand=True, fill="both")

    root = parent.winfo_toplevel() 
    root.title(f"TR Anleihen-Screener v6 — {len(_TR_ISINS)} ISINs aus TR-CSV")
    #root.geometry("1366x768") # um in NB zu passen ! -> NICHT !
    parent.configure(bg=BG)
    parent.columnconfigure(0, weight=1)
    parent.rowconfigure(0, weight=1)

    style = ttk.Style()
    style.theme_use("clam")
    style.configure("TFrame",      background=BG)
    style.configure("TLabel",      background=BG, foreground=TEXT, font=("Consolas", 7))
    style.configure("TEntry",      fieldbackground=SURFACE, foreground=TEXT, insertcolor=TEXT, font=("Consolas", 7))
    style.configure("TCombobox",   fieldbackground=SURFACE, foreground=TEXT,
                    selectbackground=SURFACE, selectforeground=TEXT)
    style.map("TCombobox",
        fieldbackground=[("readonly", SURFACE)],
        selectbackground=[("readonly", SURFACE)],
        selectforeground=[("readonly", TEXT)],
        foreground=[("readonly", TEXT)])
    style.configure("Treeview",         background=CARD, fieldbackground=CARD, foreground=TEXT,
                    rowheight=27, font=("Consolas", 6))
    style.configure("Treeview.Heading", background=SURFACE, foreground=BLUE,
                    font=("Consolas", 6, "bold"), relief="flat")
    style.map("Treeview", background=[("selected", "#2d333b")])
    style.configure("TProgressbar", troughcolor=SURFACE, background=GREEN)

    main = tk.Frame(main_frame, bg=BG, padx=16, pady=12)
    main.grid(row=0, column=0, sticky="nsew")
    main.columnconfigure(0, weight=1)
    main.rowconfigure(5, weight=1)

    global internet
    internet = ""

    if not online:

        internet = "(KEIN INTERNET)"  # Anzeige, wenn kein Internet !

    # ── Titelzeile ────────────────────────────────────────────────────────────────
    title_frame = tk.Frame(main, bg=BG)
    title_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
    label = tk.Label(title_frame, text="TR ANLEIHEN-SCREENER v6 " + internet,
            bg=BG, fg=BLUE, font=("Consolas", 12, "bold"))
    label.pack(side="left")
    tk.Label(title_frame, text=f"[{len(_TR_ISINS)} ISINs  |  KESt-Fix  |  Auto-Blacklist  |  deque-Log]",
            bg=BG, fg=ORANGE, font=("Consolas", 6, "bold")).pack(side="left", padx=8)

    deps_ok = ("curl_cffi OK" if _CURL_CFFI_OK else "curl_cffi fehlt") + \
            ("  requests OK" if _REQUESTS_OK else "  requests fehlt")
    tk.Label(title_frame, text=deps_ok, bg=BG, fg=MUTED, font=("Consolas", 6)).pack(side="left", padx=20)
    lbl_count = tk.Label(title_frame, text=f"{len(TR_BONDS)} Anleihen in DB",
                        bg=BG, fg=DIM, font=("Consolas", 6))
    lbl_count.pack(side="right")

    tk.Frame(main, bg=BORDER, height=1).grid(row=1, column=0, sticky="ew", pady=(0, 10))

    # ── Stammdaten-Banner ─────────────────────────────────────────────────────────
    # Kriterium: figi_geladen=False → OpenFIGI wurde für diese ISIN noch nie abgefragt.
    # CSV-Werte allein reichen nicht (TR-Rohwerte ohne FIGI-Verifikation).
    _braucht_stammdaten = sum(
        1 for b in TR_BONDS if not b.get("figi_geladen", False)
    )

    banner_frame = tk.Frame(main, bg=CARD)
    if not _TR_ISINS:
        banner_frame.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        tk.Label(banner_frame,
                text="  Keine TR_Anleihen_*.csv gefunden! "
                    "Lege 'TR_Anleihen_YYYY-MM-DD.csv' in denselben Ordner wie dieses Script.",
                bg=RED, fg=TEXT, font=("Consolas", 6, "bold")).pack(side="left", padx=12, pady=4)
    elif _braucht_stammdaten > 0:
        banner_frame.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        tk.Label(banner_frame,
                text=f"  {_braucht_stammdaten} Anleihen ohne Stammdaten — "
                    f"klicke 'STAMMDATEN LADEN' um Kupon & Fälligkeit von OpenFIGI zu holen.",
                bg=ORANGE, fg=BG, font=("Consolas", 6, "bold")).pack(side="left", padx=12, pady=4)

    # ── Filter-Leiste ─────────────────────────────────────────────────────────────
    fbar = tk.Frame(main, bg=BG)
    fbar.grid(row=2, column=0, sticky="ew", pady=(0, 8))

    def _lbl(parent, text, fg=MUTED):
        tk.Label(parent, text=text, bg=BG, fg=fg, font=("Consolas", 6)).pack(side="left", padx=(0, 4))

    def _entry(parent, default, width=7):
        e = tk.Entry(parent, bg=SURFACE, fg=TEXT, insertbackground=TEXT,
                    font=("Consolas", 7), width=width, relief="flat",
                    highlightbackground=BORDER, highlightcolor=BLUE, highlightthickness=1, bd=0)
        e.insert(0, default)
        e.pack(side="left", padx=(0, 14), ipady=3)
        return e

    # Typ-Buttons (Alle / Staatsanleihen / Unternehmen)
    typ_frame = tk.Frame(fbar, bg=BG)
    typ_frame.pack(side="left", padx=(0, 20))
    typ_var = tk.StringVar(value="alle")

    def _typ_btn(parent, text, value, color):
        return tk.Button(parent, text=text, font=("Consolas", 7, "bold"),
                        bg=color, fg=BG, relief="flat", bd=0, padx=14, pady=5,
                        cursor="hand2",
                        command=lambda: [typ_var.set(value), _update_typ_btns()])

    btn_alle = _typ_btn(typ_frame, "ALLE",          "alle", GREEN)
    btn_gov  = _typ_btn(typ_frame, "STAATSANLEIHEN","gov",  SURFACE)
    btn_corp = _typ_btn(typ_frame, "UNTERNEHMEN",   "corp", SURFACE)
    btn_alle.pack(side="left", padx=2)
    btn_gov.pack(side="left",  padx=2)
    btn_corp.pack(side="left", padx=2)

    def _update_typ_btns():
        v = typ_var.get()
        btn_alle.config(bg=GREEN  if v == "alle" else SURFACE, fg=BG if v == "alle" else TEXT)
        btn_gov.config( bg=GREEN  if v == "gov"  else SURFACE, fg=BG if v == "gov"  else TEXT)
        btn_corp.config(bg=ORANGE if v == "corp" else SURFACE, fg=BG if v == "corp" else TEXT)

    tk.Frame(fbar, bg=BORDER, width=1, height=28).pack(side="left", padx=10)
    _lbl(fbar, "Min. YTM brutto (%):")
    entry_min_ytm   = _entry(fbar, "0.0")
    _lbl(fbar, "Min. AT-Netto (%):")
    entry_min_netto = _entry(fbar, "0.0")
    tk.Frame(fbar, bg=BORDER, width=1, height=28).pack(side="left", padx=10)

    _lbl(fbar, "Währung:")
    waehr_var = tk.StringVar(value="Alle")

    currency_map = {
        "US": "USD", "CA": "CAD", "AU": "AUD", "NZ": "NZD",
        "GB": "GBP", "CH": "CHF", "JP": "JPY", "CN": "CNY",
        "HK": "HKD", "SG": "SGD", "DK": "DKK", "SE": "SEK",
        "NO": "NOK", "IN": "INR", "KR": "KRW", "TW": "TWD",
        "MX": "MXN", "BR": "BRL", "ZA": "ZAR", "TH": "THB",
        "ID": "IDR", "MY": "MYR", "PH": "PHP", "IL": "ILS", 
        "SA": "SAR", "AE": "AED", "QA": "QAR"
    }

    unique_currencies = sorted(list(set(currency_map.values())))
    options = ["Alle", "EUR"] + unique_currencies

    waehr_var = tk.StringVar(main_frame) 
    waehr_var.set("Alle") 
    waehr_cb = tk.OptionMenu(fbar, waehr_var, *options)
    waehr_cb.config(bg=SURFACE, fg=TEXT, activebackground=BORDER, activeforeground=TEXT,
                    font=("Consolas", 7), relief="flat", bd=0, highlightthickness=1,
                    highlightbackground=BORDER)
    waehr_cb["menu"].config(bg=SURFACE, fg=TEXT, activebackground=BLUE, activeforeground=BG,
                            font=("Consolas", 7))
    waehr_cb.pack(side="left", padx=(0, 14))

    _lbl(fbar, "Laufzeit Kat.:")
    laufz_var = tk.StringVar(value="Alle")
    laufz_cb  = tk.OptionMenu(fbar, laufz_var, "Alle", "Kurz ≤3J", "Mittel ≤7J",
                            "Lang ≤15J", "Sehr lang", "Unbekannt")
    laufz_cb.config(bg=SURFACE, fg=TEXT, activebackground=BORDER, activeforeground=TEXT,
                    font=("Consolas", 7), relief="flat", bd=0, highlightthickness=1,
                    highlightbackground=BORDER)
    laufz_cb["menu"].config(bg=SURFACE, fg=TEXT, activebackground=BLUE, activeforeground=BG,
                            font=("Consolas", 7))
    laufz_cb.pack(side="left", padx=(0, 14))

    tk.Frame(fbar, bg=BORDER, width=1, height=28).pack(side="left", padx=8)
    _lbl(fbar, "Min. Jahre:")
    entry_min_jahre = _entry(fbar, "0", width=5)
    _lbl(fbar, "Max. Jahre:")
    entry_max_jahre = _entry(fbar, "0", width=5)

    tk.Frame(fbar, bg=BORDER, width=1, height=28).pack(side="left", padx=8)
    _lbl(fbar, "Max. Spread %:")
    spread_filter_var = tk.StringVar(value=">5% raus")
    spread_filter_cb  = tk.OptionMenu(fbar, spread_filter_var,
        ">5% raus", "0.5%", "1%", "1.5%", "2%", "3%", "4%", "5%")
    spread_filter_cb.config(bg=SURFACE, fg=TEXT, activebackground=BORDER,
                            activeforeground=TEXT, font=("Consolas", 7),
                            relief="flat", bd=0, highlightthickness=1,
                            highlightbackground=BORDER)
    spread_filter_cb["menu"].config(bg=SURFACE, fg=TEXT, activebackground=BLUE,
                                    activeforeground=BG, font=("Consolas", 7))
    spread_filter_cb.pack(side="left", padx=(0, 4))

    # ── Aktionsleiste ─────────────────────────────────────────────────────────────
    abar = tk.Frame(main, bg=BG)
    abar.grid(row=3, column=0, sticky="ew", pady=(0, 6))

    def _btn(parent, text, cmd, bg=SURFACE, fg=TEXT, bold=False):
        f = ("Consolas", 7, "bold") if bold else ("Consolas", 7)
        return tk.Button(parent, text=text, command=cmd, bg=bg, fg=fg, font=f,
                        relief="flat", bd=0, padx=14, pady=6, cursor="hand2",
                        activebackground=BORDER, activeforeground=TEXT)

    # VERBESSERUNG 4: Separater STOP-Button (kein Toggle)
    btn_scan   = _btn(abar, "ABFRAGEN",         lambda: start_scan(),              GREEN,  BG,    bold=True)
    btn_stop   = _btn(abar, "STOP",             lambda: _do_stop(),                RED,    BG,    bold=True)
    btn_stamm  = _btn(abar, "STAMMDATEN LADEN", lambda: stammdaten_laden_dialog(), ORANGE, BG)
    btn_add    = _btn(abar, "+ ANLEIHE",        lambda: neue_anleihe_dialog(),     PURPLE, BG)
    btn_import = _btn(abar, "EXCEL IMPORT",     lambda: excel_import_dialog(),     BLUE,   BG)
    btn_export = _btn(abar, "EXPORTIEREN",      lambda: export_data(),             PINK, BG)
    btn_log    = _btn(abar, "LOG",              lambda: open_log(),                BLUE, BG)

    btn_scan.pack(side="left",   padx=(0, 4))
    btn_stop.pack(side="left",   padx=(0, 6))
    btn_stamm.pack(side="left",  padx=(0, 4))
    btn_add.pack(side="left",    padx=(0, 4))
    btn_import.pack(side="left", padx=(0, 4))
    btn_export.pack(side="left", padx=(0, 4))
    btn_log.pack(side="left",    padx=(0, 4))

    def _do_stop():
        """VERBESSERUNG 4: Dedizierte Stop-Funktion."""
        _cancel_event.set()
        status_var.set("Abbruch angefordert ...")
        btn_stop.config(state=tk.DISABLED)

    status_var = tk.StringVar(value=(
        f"Bereit — {len(TR_BONDS)} Anleihen aus TR-CSV geladen. Klicke ABFRAGEN."
        if _TR_ISINS else
        "Keine TR_Anleihen_*.csv gefunden! Lege die Datei in denselben Ordner."
    ))
    tk.Label(abar, textvariable=status_var, bg=BG, fg=MUTED, font=("Consolas", 6)).pack(side="left", padx=16)
    last_update_var = tk.StringVar(value="")
    tk.Label(abar, textvariable=last_update_var, bg=BG, fg=GREEN, font=("Consolas", 6)).pack(side="right", padx=4)

    prog_var = tk.IntVar(value=0)
    ttk.Progressbar(main, variable=prog_var, maximum=100, style="TProgressbar").grid(
        row=4, column=0, sticky="ew", pady=(0, 4))

    # ── Tabelle ───────────────────────────────────────────────────────────────────
    tbl_frame = tk.Frame(main, bg=BG)
    tbl_frame.grid(row=5, column=0, sticky="nsew")
    tbl_frame.columnconfigure(0, weight=1)
    tbl_frame.rowconfigure(0, weight=1)

    COL_CFG = [
        ("typ",         "Typ",            40,  "center"),
        ("name",        "Emittent",      120,  "center"),
        ("land",        "Land",           40,  "center"),
        ("rating",      "Rating",         42,  "center"),
        ("waehrung",    "Währ.",          52,  "center"),
        ("laufzeit",    "Laufzeit",       80,  "center"),
        ("faelligkeit", "Fälligkeit",    90,  "center"),
        ("kupon",       "Kupon",          72,  "center"),
        ("ytm",         "YTM brutto",     90,  "center"),
        ("netto",       "AT-Netto",       90,  "center"),
        ("kurs",        "Kurs (Onvista)",90,  "center"),
        ("spread",      "Spread %",       75,  "center"),
        ("pari",        "Pari ±",         72,  "center"),
        ("isin",        "ISIN",          50,  "center"),
        ("mktcap",      "MktCap (Mrd €)",   40, "center"),
    ]

    tree = ttk.Treeview(tbl_frame, columns=[c[0] for c in COL_CFG], show="headings", height=18)
    for cid, heading, width, anchor in COL_CFG:
        tree.heading(cid, text=heading, command=lambda c=cid: _sort_col(c))
        tree.column(cid, width=width, anchor=anchor, minwidth=36)

    tree.tag_configure("gov",          background=CARD,      foreground=TEXT)
    tree.tag_configure("corp",          background="#161b22", foreground=TEXT)
    tree.tag_configure("online",        foreground=TEAL)
    tree.tag_configure("meta",          foreground=MUTED)
    tree.tag_configure("nodate",        foreground=ORANGE)
    # Spread-Tags
    tree.tag_configure("spread_eng",    foreground="#3fb950")  # grün: <0.5%
    tree.tag_configure("spread_ok",     foreground="#3fb950")  # grün: 0.5–1.5%
    tree.tag_configure("spread_mittel", foreground="#d29922")  # orange: 1.5–3%
    tree.tag_configure("spread_hoch",   foreground="#f85149")  # rot: >3%
    # Pari-Tags
    tree.tag_configure("pari_gewinn",   foreground="#3fb950")  # grün: unter Pari
    tree.tag_configure("pari_verlust",  foreground="#f85149")  # rot: über Pari
    # Rating-Tags
    tree.tag_configure("rating_hy",     foreground="#f85149")  # rot: High Yield

    sy = ttk.Scrollbar(tbl_frame, orient=tk.VERTICAL,   command=tree.yview)
    sx = ttk.Scrollbar(tbl_frame, orient=tk.HORIZONTAL, command=tree.xview)
    tree.configure(yscroll=sy.set, xscroll=sx.set)
    tree.grid(row=0, column=0, sticky="nsew")
    sy.grid(row=0, column=1, sticky="ns")
    sx.grid(row=1, column=0, sticky="ew")
    tree.bind("<Double-1>", lambda e: _open_onvista())

    tk.Label(main,
        text="Kurs = Onvista Live.  AT-Netto = KESt auf Zins+Kursgewinn (korrekt AT).  "
            "Spread: grün=eng, orange=mittel, rot=weit.  "
            "Pari±: grün=unter Pari (Kursgewinn bei Fälligkeit), rot=über Pari.  "
            "Rating: IG=Investment Grade, HY=High Yield (Proxy via Kupon/Name).  "
            "~ = Kupon-Referenz.  Orange = Fälligkeit fehlt.  Export: immer nach AT-Netto sortiert.  "
            "Doppelklick öffnet Onvista.",
        bg=BG, fg=DIM, font=("Consolas", 5)).grid(row=6, column=0, pady=(6, 0), sticky="w")

    # ── Sortierung ────────────────────────────────────────────────────────────────
    _sort_state: dict[str, bool] = {}

    def _sort_col(col: str):
        asc = not _sort_state.get(col, False)
        _sort_state[col] = asc
        rows = [(tree.set(item, col), item) for item in tree.get_children("")]
        def key(v):
            s = v[0].replace("%", "").replace("~", "").replace(",", ".").strip().split()[0]
            try:    return (0, float(s))
            except: return (1, s.lower())
        rows.sort(key=key, reverse=not asc)
        for idx, (_, item) in enumerate(rows):
            tree.move(item, "", idx)
        for cid, h, *_ in COL_CFG:
            tree.heading(cid, text=h)
        cur = next(h for cid, h, *_ in COL_CFG if cid == col)
        tree.heading(col, text=cur + (" A" if asc else " V"), command=lambda: _sort_col(col))

    def _sort_by_netto_desc():
        """Sortiert nach AT-Netto absteigend — wird nach Scan automatisch aufgerufen."""
        rows = [(tree.set(item, "netto"), item) for item in tree.get_children("")]
        def key(v):
            s = v[0].replace("%", "").replace("~", "").strip()
            try:    return (0, -float(s))
            except: return (1, 0)
        rows.sort(key=key)
        for idx, (_, item) in enumerate(rows):
            tree.move(item, "", idx)
        for cid, h, *_ in COL_CFG:
            tree.heading(cid, text=h)
        tree.heading("netto", text="AT-Netto V", command=lambda: _sort_col("netto"))

    # ═══════════════════════════════════════════════════════════════════════════════
    # SCAN & CALLBACKS
    # ═══════════════════════════════════════════════════════════════════════════════

    def start_scan():
        try:
            spread_raw = spread_filter_var.get()
            max_spread = 5.0 if spread_raw == ">5% raus" else float(spread_raw.replace("%","").strip())
            params = {
                "min_ytm":    float(entry_min_ytm.get()),
                "min_netto":  float(entry_min_netto.get()),
                "waehrung":   waehr_var.get(),
                "laufzeit":   laufz_var.get(),
                "min_jahre":  float(entry_min_jahre.get()),
                "max_jahre":  float(entry_max_jahre.get()),
                "max_spread": max_spread,
            }
        except ValueError:
            messagebox.showerror("Fehler", "Filter: Nur Zahlen eingeben!")
            return

        typ = typ_var.get()
        bond_list = GOV_BONDS if typ == "gov" else CORP_BONDS if typ == "corp" else TR_BONDS
        if not bond_list:
            messagebox.showwarning("Keine Daten", "Keine Anleihen in der DB.")
            return

        # VERBESSERUNG 8: Explizites Zurücksetzen vor jedem Scan
        _cancel_event.clear()
        _fetch_errors.clear()
        for item in tree.get_children():
            tree.delete(item)
        prog_var.set(0)

        btn_scan.config(state=tk.DISABLED)
        btn_stop.config(state=tk.NORMAL)   # VERBESSERUNG 4
        btn_export.config(state=tk.DISABLED)
        status_var.set(f"Onvista Abfrage — {len(bond_list)} Anleihen ...")
        last_update_var.set("")
        logger.warning(f"Scan gestartet: {len(bond_list)} Anleihen | Filter: {params}")

        threading.Thread(target=scan_bonds,
                        args=(bond_list, params, _on_row, _on_progress, _on_done),
                        daemon=True).start()

    def _on_row(row: dict):
        tags = [row["typ"], "online" if row["online"] else "meta"]
        if row["faelligkeit"] == "Unbekannt":
            tags.append("nodate")
        # Spread-Tag für Zeilenfärbung
        if row.get("spread_tag"):
            tags.append(row["spread_tag"])
        # Pari-Tag (überschreibt nur wenn kein anderer Tag gesetzt)
        if row.get("pari_tag") and not any(t in tags for t in
                ("spread_hoch", "spread_mittel")):
            tags.append(row["pari_tag"])
        typ_icon = "GOV" if row["typ"] == "gov" else "CORP"
        # HY-Tag
        if row.get("rating") == "HY":
            tags.append("rating_hy")
        values = (typ_icon, row["name"], row.get("land", ""),
                row.get("rating", ""), row["waehrung"],
                row["laufzeit"], row["faelligkeit"], row["kupon"], row["ytm"],
                row["netto"], row["kurs"], row.get("spread", "–"),
                row.get("pari", "–"), row["isin"])
        main_frame.after(0, lambda v=values, t=tuple(tags): tree.insert("", tk.END, values=v, tags=t))

    def _on_progress(done: int, total: int, name: str):
        pct = int(done / total * 100) if total else 0
        main_frame.after(0, lambda: prog_var.set(pct))
        main_frame.after(0, lambda: status_var.set(f"{name[:35]:<35}  {done}/{total}  ({pct}%)"))

    def _on_done(treffer, fehler, gefiltert, total, abgebrochen=False):
        def _ui():
            prog_var.set(0 if abgebrochen else 100)
            btn_scan.config(state=tk.NORMAL)
            btn_stop.config(state=tk.DISABLED)   # VERBESSERUNG 4
            if treffer > 0:
                btn_export.config(state=tk.NORMAL)
                _sort_by_netto_desc()
            icon = "Abgebrochen" if abgebrochen else "Fertig"
            online_count = sum(
                1 for i in tree.get_children() if "online" in tree.item(i, "tags")
            )
            status_var.set(
                f"{icon}  |  {treffer} Treffer  |  {online_count} mit Live-Kurs  |  "
                f"{gefiltert} gefiltert  |  {fehler} Fehler  (von {total})"
            )
            logger.warning(
                f"Scan beendet: {treffer} Treffer, {fehler} Fehler, "
                f"{gefiltert} gefiltert, abgebrochen={abgebrochen}"
            )
            if not abgebrochen:
                last_update_var.set(f"Stand: {datetime.now().strftime('%d.%m.%Y  %H:%M')}")
        main_frame.after(0, _ui)

    def _open_onvista():
        item = tree.focus()
        if item:
            isin = tree.set(item, "isin")
            webbrowser.open(f"https://www.onvista.de/anleihen/ISIN:{isin}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # STAMMDATEN-DIALOG
    # ═══════════════════════════════════════════════════════════════════════════════

    def stammdaten_laden_dialog():
        """
        Lädt Kupon und Fälligkeit für alle Anleihen ohne Stammdaten.
        Quelle: OpenFIGI (Batch, kostenlos) + Onvista (Namen).
        """
        # Gleiche Logik wie Banner: figi_geladen=False bedeutet
        # OpenFIGI wurde noch nicht abgefragt — unabhängig von CSV-Rohwerten.
        fehlend = [b for b in TR_BONDS if not b.get("figi_geladen", False)]
        if not fehlend:
            messagebox.showinfo("Stammdaten",
                                "Alle Anleihen haben bereits vollständige Stammdaten.")
            return

        minuten = max(3, len(fehlend) // 10 * 3 // 60)
        antwort = messagebox.askyesno(
            "Stammdaten laden",
            f"{len(fehlend)} Anleihen ohne Stammdaten.\n\n"
            f"Quelle: OpenFIGI (Kupon + Fälligkeit) + Onvista (Name)\n"
            f"Geschätzte Dauer: ca. {minuten}–{minuten+2} Minuten\n\n"
            f"Jetzt starten?"
        )
        if not antwort:
            return

        btn_stamm.config(state=tk.DISABLED)
        btn_scan.config(state=tk.DISABLED)
        prog_var.set(0)


        def _do_load():
            db    = _db_laden()
            total = len(fehlend)
            done  = 0

            alle_isins = [b["isin"] for b in fehlend]
            main_frame.after(0, lambda: status_var.set(
                f"Phase 1/2: OpenFIGI lädt {total} ISINs in Batches à 10 ..."))

            for batch_start in range(0, total, 10):
                if _cancel_event.is_set():
                    break
                batch     = alle_isins[batch_start:batch_start + 10]
                _openfigi_batch(batch)
                done_b = min(batch_start + 10, total)
                pct    = int(done_b / total * 50)
                main_frame.after(0, lambda p=pct, d=done_b, t=total:
                    [prog_var.set(p),
                    status_var.set(f"OpenFIGI ... {d}/{t} ISINs ({p}%)")])

            main_frame.after(0, lambda: status_var.set("Phase 2/2: Namen von Onvista laden ..."))

            for bond in fehlend:
                if _cancel_event.is_set():
                    break
                isin  = bond["isin"]
                figi  = _figi_cache.get(isin)
                done += 1

                kupon = None
                faell = ""
                name  = ""

                if figi:
                    ticker       = figi.get("ticker", "") or ""
                    kupon, faell = _figi_ticker_parse(ticker)
                    name         = figi.get("name", "")

                name_ov = _onvista_name(isin)
                if name_ov:
                    name = name_ov
                time.sleep(0.25)

                if not name:
                    name = db.get(isin, {}).get("name", f"Anleihe {isin}")

                nl    = name.lower()
                typ_r = "gov" if any(kw in nl for kw in _GOV_KEYWORDS) else _isin_typ(isin)

                if isin in db:
                    db[isin].update({"name": name, "faelligkeit": faell,
                                    "kupon_ref": kupon, "typ": typ_r,
                                    "figi_geladen": True})  # Flag setzen
                else:
                    db[isin] = {"isin": isin, "name": name, "typ": typ_r,
                                "waehrung": _isin_waehrung(isin),
                                "faelligkeit": faell, "kupon_ref": kupon,
                                "figi_geladen": True}

                pct = 50 + int(done / total * 50)
                main_frame.after(0, lambda p=pct, d=done, t=total, n=name:
                    [prog_var.set(p),
                    status_var.set(f"Namen laden ... {d}/{t}  {n[:35]}")])

            _db_speichern(db)
            _db_auf_tr_bonds_anwenden(db)

            fertig = sum(1 for b in TR_BONDS
                        if b.get("faelligkeit") and b.get("kupon_ref") is not None)

            def _done_ui():
                btn_stamm.config(state=tk.NORMAL)
                btn_scan.config(state=tk.NORMAL)
                prog_var.set(0)
                lbl_count.config(text=f"{len(TR_BONDS)} Anleihen in DB")
                status_var.set(
                    f"Stammdaten geladen — {fertig}/{len(TR_BONDS)} vollständig. "
                    f"Jetzt ABFRAGEN klicken.")
                banner_frame.grid_remove()
            main_frame.after(0, _done_ui)

        threading.Thread(target=_do_load, daemon=True).start()

    # ═══════════════════════════════════════════════════════════════════════════════
    # DIALOGE
    # ═══════════════════════════════════════════════════════════════════════════════

    def _typ_aus_name(name: str) -> str:
        """Nutzt die zentrale _GOV_KEYWORDS Liste."""
        nl = name.lower()
        return "gov" if any(kw in nl for kw in _GOV_KEYWORDS) else "corp"

    def neue_anleihe_dialog():
        win = tk.Toplevel(main_frame)
        win.title("Neue Anleihe hinzufügen")
        win.configure(bg=BG)
        win.geometry("550x420")
        win.grab_set()

        tk.Label(win, text="+ NEUE ANLEIHE HINZUFÜGEN",
                bg=BG, fg=PURPLE, font=("Consolas", 8, "bold")).pack(padx=20, pady=(16, 4), anchor="w")
        tk.Frame(win, bg=BORDER, height=1).pack(fill="x", padx=20, pady=(0, 12))

        form = tk.Frame(win, bg=BG)
        form.pack(fill="x", padx=20)
        tk.Label(form, text="ISIN:", bg=BG, fg=MUTED).grid(row=0, column=0, sticky="w", pady=5)
        e_isin = tk.Entry(form, bg=SURFACE, fg=TEXT, font=("Consolas", 8, "bold"), width=16)
        e_isin.grid(row=0, column=1, sticky="w", pady=5)

        def _do_lookup():
            isin_raw = e_isin.get().strip().upper()
            if len(isin_raw) != 12:
                return
            meta = _lsx_metadata_lookup(isin_raw)
            if meta:
                if meta["name"]:        e_name.delete(0, tk.END);  e_name.insert(0,  meta["name"])
                if meta["faelligkeit"]: e_faell.delete(0, tk.END); e_faell.insert(0, meta["faelligkeit"])
                if meta["kupon"]:       e_kupon.delete(0, tk.END); e_kupon.insert(0, str(meta["kupon"]))

        tk.Button(form, text="Lookup (OpenFIGI)", command=_do_lookup,
                bg=PURPLE, fg=BG, relief="flat").grid(row=0, column=2, padx=10)
        tk.Label(form, text="Name:", bg=BG, fg=MUTED).grid(row=1, column=0, sticky="w", pady=5)
        e_name  = tk.Entry(form, bg=SURFACE, fg=TEXT, width=30)
        e_name.grid(row=1, column=1, columnspan=2, sticky="w", pady=5)
        tk.Label(form, text="Fälligkeit (YYYY-MM-DD):", bg=BG, fg=MUTED).grid(row=2, column=0, sticky="w", pady=5)
        e_faell = tk.Entry(form, bg=SURFACE, fg=TEXT, width=16)
        e_faell.grid(row=2, column=1, sticky="w", pady=5)
        tk.Label(form, text="Kupon (%):", bg=BG, fg=MUTED).grid(row=3, column=0, sticky="w", pady=5)
        e_kupon = tk.Entry(form, bg=SURFACE, fg=TEXT, width=16)
        e_kupon.grid(row=3, column=1, sticky="w", pady=5)

        def _speichern():
            isin = e_isin.get().strip().upper()
            if len(isin) != 12 or not e_name.get():
                return
            db = _db_laden()
            try:
                kup = float(e_kupon.get().replace(",", ".")) if e_kupon.get() else None
            except Exception:
                kup = None
            db[isin] = {"isin": isin, "name": e_name.get(),
                        "typ": _typ_aus_name(e_name.get()),
                        "waehrung": _isin_waehrung(isin),
                        "faelligkeit": e_faell.get(), "kupon_ref": kup,
                        "figi_geladen": True}  # Manuell erfasst = vollständig
            _db_speichern(db)
            _db_auf_tr_bonds_anwenden(db)
            lbl_count.config(text=f"{len(TR_BONDS)} Anleihen in DB")
            win.destroy()

        tk.Button(win, text="SPEICHERN", command=_speichern,
                bg=GREEN, fg=BG, font=("Consolas", 7, "bold"), padx=15, pady=5).pack(pady=20)


    def excel_import_dialog():
        pfad = filedialog.askopenfilename(
            title="ISIN-Liste oder Anleihen-Export wählen",
            filetypes=[("Excel/CSV", "*.xlsx *.csv"), ("Alle Dateien", "*.*")]
        )
        if not pfad:
            return
        status_var.set("Lese Datei ...")
        btn_import.config(state=tk.DISABLED)

        def _do_import():
            isins_raw: list[dict] = []
            try:
                if pfad.lower().endswith(".xlsx"):
                    import openpyxl
                    wb = openpyxl.load_workbook(pfad, data_only=True)
                    for ws in wb.worksheets:
                        headers = {}
                        first_row = True
                        for row in ws.iter_rows(values_only=True):
                            if first_row:
                                for i, cell in enumerate(row):
                                    if isinstance(cell, str):
                                        h = cell.strip().lower()
                                        if "isin" in h:                    headers["isin"]  = i
                                        if "kupon" in h or "coupon" in h:  headers["kupon"] = i
                                        if "fällig" in h or "maturi" in h: headers["faell"] = i
                                first_row = False
                                continue
                            for cell in row:
                                if isinstance(cell, str):
                                    for isin in re.findall(r'[A-Z]{2}[A-Z0-9]{10}', cell):
                                        kupon_v = None
                                        faell_v = ""
                                        if "kupon" in headers:
                                            raw = row[headers["kupon"]]
                                            try:
                                                kupon_v = float(raw) if isinstance(raw, (int, float)) \
                                                        else float(str(raw).replace(",", ".").replace("%", "").strip())
                                            except Exception:
                                                pass
                                        if "faell" in headers:
                                            raw = row[headers["faell"]]
                                            if isinstance(raw, (date, datetime)):
                                                faell_v = raw.strftime("%Y-%m-%d") if isinstance(raw, datetime) else raw.isoformat()
                                            elif isinstance(raw, str):
                                                m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', raw)
                                                if m:
                                                    faell_v = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
                                                else:
                                                    m2 = re.search(r'(\d{4}-\d{2}-\d{2})', raw)
                                                    if m2:
                                                        faell_v = m2.group(1)
                                        isins_raw.append({"isin": isin, "kupon": kupon_v, "faelligkeit": faell_v})
                else:
                    with open(pfad, "r", encoding="utf-8", errors="ignore") as f:
                        text = f.read()
                    for isin in re.findall(r'[A-Z]{2}[A-Z0-9]{10}', text):
                        isins_raw.append({"isin": isin, "kupon": None, "faelligkeit": ""})
            except Exception as e:
                main_frame.after(0, lambda err=str(e): messagebox.showerror("Dateifehler", err))
                main_frame.after(0, lambda: btn_import.config(state=tk.NORMAL))
                return

            seen = {}
            for entry in isins_raw:
                if entry["isin"] not in seen:
                    seen[entry["isin"]] = entry
            isins_unique = list(seen.values())

            if not isins_unique:
                main_frame.after(0, lambda: messagebox.showwarning("Fehler", "Keine ISINs gefunden."))
                main_frame.after(0, lambda: btn_import.config(state=tk.NORMAL))
                return

            db = _db_laden()
            stats = [0, 0]

            def _fetch_meta(entry: dict):
                isin      = entry["isin"]
                kupon     = entry.get("kupon")
                faell     = entry.get("faelligkeit", "")
                hat_kupon = kupon is not None and kupon > 0
                hat_faell = bool(faell) and faell != "2030-01-01"
                if hat_kupon and hat_faell:
                    return (isin, {"name": db.get(isin, {}).get("name", f"Anleihe {isin}"),
                                "faelligkeit": faell, "kupon": kupon})
                meta = _lsx_metadata_lookup(isin)
                time.sleep(0.15)
                if meta:
                    return (isin, {"name": meta["name"] or db.get(isin, {}).get("name", f"Anleihe {isin}"),
                                "faelligkeit": faell if hat_faell else (meta["faelligkeit"] or ""),
                                "kupon": kupon if hat_kupon else meta["kupon"]})
                return (isin, {"name": db.get(isin, {}).get("name", f"Anleihe {isin}"),
                            "faelligkeit": faell, "kupon": kupon})

            done = 0
            total = len(isins_unique)
            with ThreadPoolExecutor(max_workers=4) as ex:
                futures = {ex.submit(_fetch_meta, e): e for e in isins_unique}
                for fut in as_completed(futures):
                    done += 1
                    entry = futures[fut]
                    isin  = entry["isin"]
                    try:
                        _, meta = fut.result()
                        name  = meta.get("name") or f"Anleihe {isin}"
                        faell = meta.get("faelligkeit") or ""
                        kupon = meta.get("kupon")
                        if isin in db:
                            db[isin].update({"name": name, "faelligkeit": faell,
                                            "kupon_ref": kupon, "typ": _typ_aus_name(name),
                                            "figi_geladen": True})
                            stats[1] += 1
                        else:
                            db[isin] = {"isin": isin, "name": name, "typ": _typ_aus_name(name),
                                        "waehrung": _isin_waehrung(isin),
                                        "faelligkeit": faell, "kupon_ref": kupon,
                                        "figi_geladen": True}
                            stats[0] += 1
                    except Exception:
                        pass
                    pct = int(done / total * 100)
                    main_frame.after(0, lambda p=pct, d=done, t=total:
                        [prog_var.set(p), status_var.set(f"Lade Stammdaten ... {d}/{t}")])

            _db_speichern(db)
            _db_auf_tr_bonds_anwenden(db)

            def _done():
                btn_import.config(state=tk.NORMAL)
                lbl_count.config(text=f"{len(TR_BONDS)} Anleihen in DB")
                messagebox.showinfo("Import abgeschlossen",
                    f"Neue: {stats[0]}  |  Aktualisiert: {stats[1]}")
                status_var.set("Import fertig. ABFRAGEN klicken.")
                prog_var.set(0)
            main_frame.after(0, _done)

        threading.Thread(target=_do_import, daemon=True).start()


    def export_data():
        rows = tree.get_children()
        if not rows:
            return

        # MITTEL 6: Export immer nach AT-Netto absteigend sortiert
        # Unabhängig davon wie der User die Tabelle zuletzt sortiert hat.
        netto_idx = next((i for i, (cid,*_) in enumerate(COL_CFG) if cid == "netto"), None)
        def _netto_key(item):
            vals = tree.item(item)["values"]
            try:
                s = str(vals[netto_idx]).replace("%","").replace("~","").strip()
                return (0, -float(s))
            except Exception:
                return (1, 0)
        sorted_rows = sorted(rows, key=_netto_key) if netto_idx is not None else list(rows)

        data    = [tree.item(r)["values"] for r in sorted_rows]
        headers = [h for _, h, *_ in COL_CFG]

        # VERBESSERUNG 6: Automatischer Dateiname mit Typ/Datum-Vorschlag
        typ    = typ_var.get()
        typ_label = {"alle": "Alle_Anleihen", "gov": "Staatsanleihen", "corp": "Unternehmensanleihen"}.get(typ, "Anleihen")
        datum  = datetime.now().strftime("%d_%m_%Y_um_%H_Uhr_%M")
        vorschlag = f"{typ_label}_Scan_{datum}"

        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=vorschlag,
            filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")]
        )
        if not path:
            return
        try:
            if path.endswith(".csv"):
                import csv
                with open(path, "w", newline="", encoding="utf-8-sig") as f:
                    w = csv.writer(f, delimiter=";")
                    w.writerow(headers)
                    w.writerows(data)
            elif _PANDAS_OK:

                raw_rows = [tree.item(r)["values"] for r in sorted_rows]

                pd.DataFrame(data, columns=headers).to_excel(path, index=False)

                engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')

                logger.warning("SQL ACCESS")

                data1 = {
        'name': ['ausgabe' + str(datum)],
        'limitx': ['1000'],
        'content': ["".join([",".join(map(str, row)) for row in raw_rows]).replace("Mrd", "MrdPROMPTSPLIT")], # splitten !!!
        'done': [0],
        'client': ['/127.0.0.1']
        }

                dfx = pd.DataFrame(data1)

                dfx.to_sql(
                name='tasks', 
                con=engine, 
                if_exists='append', 
                index=False,
                dtype={
                    'name': types.VARCHAR(255),
                    'limitx': types.VARCHAR(255),
                    'content': types.Text(),
                    'done': types.BOOLEAN(),
                    'client': types.VARCHAR(255)
                }
                )

                t1 = threading.Thread(target= lambda: pd.DataFrame(data, columns=headers).to_sql(name='ausgabe', con=engine, if_exists='append', index=False)) # SQL Server Backup !!!
                t2 = threading.Thread(target= lambda: pd.DataFrame(data, columns=headers).to_sql(name='ausgabe' + str(datum), con=engine, if_exists='append', index=False))
            
            status_var.set(f"Gespeichert: {os.path.basename(path)}")

            t1.start() 
            t2.start()
            t1.join()
            t2.join()

            status_var.set(f"Gespeichert: {os.path.basename(path)}" + " & Mit Datenbankserver online synchronisiert: " + 'ausgabe' + str(datum)) # warten mit join, danach SYNC zeigem

        except Exception as e:
            messagebox.showerror("Export-Fehler", str(e))


    # VERBESSERUNG 1: Vollständiges Log-Fenster (aus Aktien-Screener übernommen)
    def open_log():
        win = tk.Toplevel(main_frame)
        win.title("Fehler & Log")
        win.geometry("1000x650")
        win.configure(bg=BG)
        win.columnconfigure(0, weight=1)
        win.rowconfigure(1, weight=1)

        # Titelzeile mit Buttons
        hdr = tk.Frame(win, bg=BG)
        hdr.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))
        tk.Label(hdr, text="FEHLER & LOG", bg=BG, fg=RED,
                font=("Consolas", 9, "bold")).pack(side="left")
        tk.Label(hdr, text="  (alle WARNING/ERROR/CRITICAL Meldungen aus dieser Session)",
                bg=BG, fg=MUTED, font=("Consolas", 6)).pack(side="left")

        def _copy_all():
            inhalt = txt.get("1.0", tk.END).strip()
            win.clipboard_clear()
            win.clipboard_append(inhalt)
            win.update()
            btn_copy.config(text="Kopiert!")
            win.after(2000, lambda: btn_copy.config(text="Alles kopieren"))

        def _clear():
            _log_clear()
            _refresh_farben()

        def _refresh_farben():
            inhalt = _log_get()
            zeilen = [z for z in inhalt.splitlines()
                    if any(lv in z for lv in ("WARNING", "ERROR", "CRITICAL", "EXCEPTION"))]
            if not zeilen:
                zeilen = ["(Noch keine Fehlermeldungen — gut so!)"]
            txt.config(state=tk.NORMAL)
            txt.delete("1.0", tk.END)
            for zeile in zeilen:
                tag = ""
                if "CRITICAL" in zeile or "EXCEPTION" in zeile:
                    tag = "critical"
                elif "ERROR" in zeile:
                    tag = "error"
                elif "WARNING" in zeile:
                    tag = "warning"
                txt.insert(tk.END, zeile + "\n", tag)
            txt.see(tk.END)
            txt.config(state=tk.DISABLED)
            lbl_log_count.config(text=f"{len(zeilen)} Einträge")

        btn_copy      = tk.Button(hdr, text="Alles kopieren", command=_copy_all,
                                bg=BLUE, fg=BG, font=("Consolas", 6, "bold"),
                                relief="flat", padx=10, pady=3, cursor="hand2")
        btn_clear_log = tk.Button(hdr, text="Log leeren", command=_clear,
                                bg=SURFACE, fg=MUTED, font=("Consolas", 6),
                                relief="flat", padx=10, pady=3, cursor="hand2")
        btn_ref       = tk.Button(hdr, text="Aktualisieren", command=_refresh_farben,
                                bg=SURFACE, fg=MUTED, font=("Consolas", 6),
                                relief="flat", padx=10, pady=3, cursor="hand2")
        lbl_log_count = tk.Label(hdr, text="", bg=BG, fg=DIM, font=("Consolas", 6))

        btn_copy.pack(side="right",      padx=(4, 0))
        btn_clear_log.pack(side="right", padx=(4, 0))
        btn_ref.pack(side="right",       padx=(4, 0))
        lbl_log_count.pack(side="right", padx=(12, 4))

        # Textfeld mit Scrollbars
        frm = tk.Frame(win, bg=BG)
        frm.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        frm.columnconfigure(0, weight=1)
        frm.rowconfigure(0, weight=1)

        txt = tk.Text(frm, bg=SURFACE, fg=TEXT, font=("Consolas", 5),
                    wrap=tk.NONE, padx=10, pady=10, selectbackground="#2d333b")
        sb_y = ttk.Scrollbar(frm, orient=tk.VERTICAL,   command=txt.yview)
        sb_x = ttk.Scrollbar(frm, orient=tk.HORIZONTAL, command=txt.xview)
        txt.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
        sb_y.grid(row=0, column=1, sticky="ns")
        sb_x.grid(row=1, column=0, sticky="ew")
        txt.grid(row=0, column=0, sticky="nsew")

        # Farb-Tags: ERROR rot, WARNING orange, CRITICAL magenta
        txt.tag_configure("error",    foreground=RED)
        txt.tag_configure("warning",  foreground=ORANGE)
        txt.tag_configure("critical", foreground="#ff00ff")

        _refresh_farben()

        tk.Button(win, text="Schliessen", command=win.destroy,
                bg=SURFACE, fg=MUTED, font=("Consolas", 6),
                relief="flat", padx=10, pady=4).grid(row=2, column=0, pady=(0, 8))


    # ── Start ─────────────────────────────────────────────────────────────────────
    _update_typ_btns()
    btn_stop.config(state=tk.DISABLED)  # Initial deaktiviert bis Scan läuft
    #main_frame.mainloop() # NICHT !!!

BG = "#2e2e2e"

def main():
    # HAUPTFENSTER SETUP (Dein Code)
    root = tk.Tk()
    root.title(f"Aktien + Anleihen-Screener v6")
    root.geometry("1600x880")
    root.configure(bg=BG)
    
    # Layout-Gewichtung
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)

    # NOTEBOOK (TAB-SYSTEM)
    style = ttk.Style()
    style.theme_use('default') # 'clam' oder 'alt' funktionieren oft besser für Dark Mode
    
    notebook = ttk.Notebook(root)
    notebook.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

    # TABS ERSTELLEN
    # Wir erstellen leere Frames, die als "Eltern" für deine Funktionen dienen
    tab1 = tk.Frame(notebook, bg=BG)
    tab2 = tk.Frame(notebook, bg=BG)

    notebook.add(tab1, text=" Aktien-Screener v6 ")
    notebook.add(tab2, text=" Anleihen-Screener v6 ")

    # FUNKTIONEN AUFRUFEN
    # Wir übergeben den jeweiligen Tab-Frame an deine Funktionen

    # im Hintergrund via Threads !!!
    tk1(tab1)
    tk2(tab2)

    root.mainloop()

if __name__ == "__main__":
    main()