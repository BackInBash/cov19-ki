# Create SQLite Database
import sqlite3
from sqlite3 import Error
import threading

threadLimiter = threading.BoundedSemaphore(2)
lock = threading.Lock()

def create_connection():
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect("file:data.db?cache=shared", uri=True)
        return conn
    except Error as e:
        print(e)

def write_to_file():
    con = create_connection()
    with open('dump.sql', 'w') as f:
        for line in con.iterdump():
            f.write('%s\n' % line)

def create_struct():
    conn = create_connection()
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

    fallzahlen = """CREATE TABLE IF NOT EXISTS fallzahlen (
                                    id integer PRIMARY KEY AUTOINCREMENT,
                                    datum text NOT NULL,
                                    bundesland integer NOT NULL,
                                    gemeindeschluessel integer NOT NULL,
                                    anzahl_standorte integer NOT NULL,
                                    anzahl_meldebereiche integer NOT NULL,
                                    faelle_covid_aktuell integer NOT NULL,
                                    faelle_covid_aktuell_invasiv_beatmet integer NOT NULL,
                                    betten_frei integer NOT NULL,
                                    betten_belegt integer NOT NULL,
                                    betten_belegt_nur_erwachsen integer NOT NULL,
                                    betten_frei_nur_erwachsen integer NOT NULL
                                );"""
    try:
        conn.execute(kh_liste)
        conn.execute(kh_meldebereiche)
        conn.execute(kh_status)
        conn.execute(fallzahlen)
        conn.close()
    except:
        print("Error Creating schama")
        exit(1)

#
# Fallzahlen SQL Methoden
#

def fallzahl_not_exists(conn, date):
    cur = conn.cursor()
    cur.execute("SELECT * FROM fallzahlen WHERE datum = ?", (date,))
    rows = cur.fetchall()
    if len(rows) == 0:
        return True
    else:
        return False

def add_fallzahl(fall):
    try:
        threadLimiter.acquire()
        lock.acquire(True)
        conn = create_connection()
        if fallzahl_not_exists(conn, fall.datum):
            sql = ''' INSERT INTO fallzahlen(datum,bundesland,gemeindeschluessel,anzahl_standorte,anzahl_meldebereiche,faelle_covid_aktuell,faelle_covid_aktuell_invasiv_beatmet,betten_frei,betten_belegt,betten_belegt_nur_erwachsen,betten_frei_nur_erwachsen) VALUES(?,?,?,?,?,?,?,?,?,?,?) '''
            cur = conn.cursor()
            cur.execute(sql, (fall.datum,fall.bundesland,fall.gemeindeschluessel,fall.anzahl_standorte,fall.anzahl_meldebereiche,fall.faelle_covid_aktuell,fall.faelle_covid_aktuell_invasiv_beatmet,fall.betten_frei,fall.betten_belegt,fall.betten_belegt_nur_erwachsen,fall.betten_frei_nur_erwachsen))
            conn.commit()
        conn.close()
    except Exception as e:
        print(e)
        conn.close()
        lock.release()
        threadLimiter.release()
    finally:
        lock.release()
        threadLimiter.release()


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
    try:
        threadLimiter.acquire()
        lock.acquire(True)
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
    except Exception as e:
        print(e)
        conn.close()
        lock.release()
        threadLimiter.release()
    finally:
        lock.release()
        threadLimiter.release()

def execute_sql(sql):
    conn = create_connection()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()