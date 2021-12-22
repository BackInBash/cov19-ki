# Import Data from intensivregister.de to SQLite DB

from db import create_struct, add_kh, add_fallzahl
from dataclasses import dataclass
from uuid import UUID
import requests
import csv
from io import StringIO

# Data Classes

@dataclass
class Meldebereiche:
    meldebereichId: str
    ardsNetzwerkMitglied: str
    meldebereichBezeichnung: str
    behandlungsschwerpunktL1: str
    behandlungsschwerpunktL2: None
    behandlungsschwerpunktL3: None

@dataclass
class Krankenhaus:
    id: int
    bezeichnung: str
    strasse: str
    hausnummer: int
    plz: str
    ort: str
    bundesland: str
    ikNummer: int
    position: str
    gemeindeschluessel: int
    meldebereiche: list[Meldebereiche]
    maxBettenStatusEinschaetzungEcmo: str
    maxBettenStatusEinschaetzungHighCare: str
    maxBettenStatusEinschaetzungLowCare: str
    letzteMeldezeitpunkt: str

@dataclass
class Fallzahlen:
    datum: str
    bundesland: int
    gemeindeschluessel: int
    anzahl_standorte: int
    anzahl_meldebereiche: int
    faelle_covid_aktuell: int
    faelle_covid_aktuell_invasiv_beatmet: int
    betten_frei: int
    betten_belegt: int
    betten_belegt_nur_erwachsen: int
    betten_frei_nur_erwachsen: int

def import_kh():
    data = requests.get("https://www.intensivregister.de/api/public/intensivregister").json()
    for object in data['data']:
        mbList = []

        for bericht in object['meldebereiche']:
            mb = Meldebereiche(bericht['meldebereichId'],
            bericht['ardsNetzwerkMitglied'],
            bericht['meldebereichBezeichnung'],
            bericht['behandlungsschwerpunktL1'],
            bericht['behandlungsschwerpunktL2'],
            bericht['behandlungsschwerpunktL3'])
            mbList.append(mb)

        kh = Krankenhaus(object['krankenhausStandort']['id'],
        object['krankenhausStandort']['bezeichnung'],
        object['krankenhausStandort']['strasse'],
        object['krankenhausStandort']['hausnummer'],
        object['krankenhausStandort']['plz'],
        object['krankenhausStandort']['ort'],
        object['krankenhausStandort']['bundesland'],
        object['krankenhausStandort']['ikNummer'],
        str(object['krankenhausStandort']['position']['latitude'])+"/"+str(object['krankenhausStandort']['position']['latitude']),
        object['krankenhausStandort']['gemeindeschluessel'],
        mbList,
        object['maxBettenStatusEinschaetzungEcmo'],
        object['maxBettenStatusEinschaetzungHighCare'],
        object['maxBettenStatusEinschaetzungLowCare'],
        object['letzteMeldezeitpunkt'],
        )

        add_kh(kh)

def csv_import_landkreis():
    data = csv.reader(StringIO(requests.get("https://diviexchange.blob.core.windows.net/%24web/zeitreihe-tagesdaten.csv").text))
    # Skip CSV Header
    next(data)
    for row in data:
        fall = Fallzahlen(row[0],
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        row[6],
        row[7],
        row[8],
        row[9],
        row[10])
        add_fallzahl(fall)

import_kh()
csv_import_landkreis()