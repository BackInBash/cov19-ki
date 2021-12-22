# Create SQLite Database
import sqlite3
from sqlite3 import Error

def create_connection():
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect("data.db")
        create_struct(conn)
        return conn
    except Error as e:
        print(e)

def create_struct(conn):
    kh_liste = """CREATE TABLE IF NOT EXISTS krankenhaus (
                                        id integer PRIMARY KEY,
                                        bezeichnung text NOT NULL,
                                        strasse text NOT NULL,
                                        plz integer NOT NULL,
                                        ort text NOT NULL,
                                        bundesland text NOT NULL,
                                        ikNummer integer NOT NULL,
                                        position text NOT NULL,
                                        gemeindeschluessel text NOT NULL
                                    ); """

    kh_meldebereiche = """CREATE TABLE IF NOT EXISTS meldebereiche (
                                    meldebereichId text PRIMARY KEY,
                                    kh_id integer NOT NULL,
                                    ardsNetzwerkMitglied text NOT NULL,
                                    meldebereichBezeichnung text NOT NULL,
                                    behandlungsschwerpunktL1 text,
                                    behandlungsschwerpunktL2 text,
                                    behandlungsschwerpunktL3 text
                                );"""

    kh_status = """CREATE TABLE IF NOT EXISTS bettenstatus (
                                    id integer PRIMARY KEY AUTOINCREMENT,
                                    kh_id integer NOT NULL,
                                    maxBettenStatusEinschaetzungEcmo text NOT NULL,
                                    maxBettenStatusEinschaetzungHighCare text NOT NULL,
                                    maxBettenStatusEinschaetzungLowCare text,
                                    letzteMeldezeitpunkt text NOT NULL
                                );"""

    conn.execute(kh_liste)
    conn.execute(kh_meldebereiche)
    conn.execute(kh_status)

#
# Krankenhaus SQL Methoden
#

def meldebereich_not_exists(conn, id):
    cur = conn.cursor()
    cur.execute("SELECT * FROM meldebereiche WHERE meldebereichId = ?", (id,))
    rows = cur.fetchall()
    if len(rows) == 0:
        return True
    else:
        return False

def kh_not_exists(conn, id):
    cur = conn.cursor()
    cur.execute("SELECT * FROM krankenhaus WHERE id = ?", (id,))
    rows = cur.fetchall()
    if len(rows) == 0:
        return True
    else:
        return False

def bettenstatus_not_exists(conn, date):
    cur = conn.cursor()
    cur.execute("SELECT * FROM bettenstatus WHERE letzteMeldezeitpunkt = ?", (date,))
    rows = cur.fetchall()
    if len(rows) == 0:
        return True
    else:
        return False

def add_kh(kh):
    conn = create_connection()
    if kh_not_exists(conn, kh.id):
        sql = ''' INSERT INTO krankenhaus(id,bezeichnung,strasse,plz,ort,bundesland,ikNummer,position,gemeindeschluessel) VALUES(?,?,?,?,?,?,?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, (kh.id,kh.bezeichnung,kh.strasse,kh.plz,kh.ort,kh.bundesland,kh.ikNummer,kh.position,kh.gemeindeschluessel))
        conn.commit()

    for meldebereich in kh.meldebereiche: 
        if meldebereich_not_exists(conn, meldebereich.meldebereichId):
            sql = ''' INSERT INTO meldebereiche(meldebereichId,kh_id,ardsNetzwerkMitglied,meldebereichBezeichnung,behandlungsschwerpunktL1,behandlungsschwerpunktL2,behandlungsschwerpunktL3) VALUES(?,?,?,?,?,?,?) '''
            cur = conn.cursor()
            cur.execute(sql, (meldebereich.meldebereichId,kh.id,meldebereich.ardsNetzwerkMitglied,meldebereich.meldebereichBezeichnung,meldebereich.behandlungsschwerpunktL1,meldebereich.behandlungsschwerpunktL2,meldebereich.behandlungsschwerpunktL3))
            conn.commit()

    if bettenstatus_not_exists(conn, kh.letzteMeldezeitpunkt):
        sql = ''' INSERT INTO bettenstatus(kh_id,maxBettenStatusEinschaetzungEcmo,maxBettenStatusEinschaetzungHighCare,maxBettenStatusEinschaetzungLowCare,letzteMeldezeitpunkt) VALUES(?,?,?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, (kh.id,kh.maxBettenStatusEinschaetzungEcmo,kh.maxBettenStatusEinschaetzungHighCare,kh.maxBettenStatusEinschaetzungLowCare,kh.letzteMeldezeitpunkt))
        conn.commit()
    conn.close()