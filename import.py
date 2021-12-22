# Import Data from intensivregister.de to SQLite DB

from db import create_struct, add_kh
from dataclasses import dataclass
from uuid import UUID
import requests
import json

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

import_kh()
