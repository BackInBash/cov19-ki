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

    cwa = """CREATE TABLE IF NOT EXISTS cwa (
                                    id integer PRIMARY KEY AUTOINCREMENT,
                                    effective_date text NULL,
                                    update_timestamp text NULL,
                                    infections_published_daily integer NULL,
                                    app_downloads_cumulated integer NULL,
                                    app_downloads_android_cumulated integer NULL,
                                    app_downloads_ios_cumulated integer NULL,
                                    app_downloads_daily integer NULL,
                                    app_downloads_android_daily integer NULL,
                                    app_downloads_ios_daily integer NULL,
                                    app_downloads_7days_avg integer NULL,
                                    app_downloads_7days_sum integer NULL,
                                    app_downloads_android_7days_avg integer NULL,
                                    app_downloads_android_7days_sum integer NULL,
                                    app_downloads_ios_7days_avg integer NULL,
                                    app_downloads_ios_7days_sum integer NULL,
                                    tests_total_cumulated integer NULL,
                                    tests_total_daily integer NULL,
                                    tests_total_7days_avg integer NULL,
                                    tests_pcr_total_cumulated integer NULL,
                                    tests_pcr_positive_cumulated integer NULL,
                                    tests_pcr_negative_cumulated integer NULL,
                                    tests_pcr_invalide_cumulated integer NULL,
                                    tests_pcr_total_daily integer NULL,
                                    tests_pcr_positive_daily integer NULL,
                                    tests_pcr_negative_daily integer NULL,
                                    tests_pcr_invalide_daily integer NULL,
                                    tests_pcr_total_7days_avg integer NULL,
                                    tests_pcr_positive_7days_avg integer NULL,
                                    tests_pcr_negative_7days_avg integer NULL,
                                    tests_pcr_invalide_7days_avg integer NULL,
                                    tests_pcr_total_7days_sum integer NULL,
                                    tests_pcr_positive_7days_sum integer NULL,
                                    tests_pcr_negative_7days_sum integer NULL,
                                    tests_pcr_invalide_7days_sum integer NULL,
                                    tests_rat_total_cumulated integer NULL,
                                    tests_rat_positive_cumulated integer NULL,
                                    tests_rat_negative_cumulated integer NULL,
                                    tests_rat_invalide_cumulated integer NULL,
                                    tests_rat_total_daily integer NULL,
                                    tests_rat_positive_daily integer NULL,
                                    tests_rat_negative_daily integer NULL,
                                    tests_rat_invalide_daily integer NULL,
                                    tests_rat_total_7days_avg integer NULL,
                                    tests_rat_positive_7days_avg integer NULL,
                                    tests_rat_negative_7days_avg integer NULL,
                                    tests_rat_invalide_7days_avg integer NULL,
                                    tests_rat_total_7days_sum integer NULL,
                                    tests_rat_positive_7days_sum integer NULL,
                                    tests_rat_negative_7days_sum integer NULL,
                                    tests_rat_invalide_7days_sum integer NULL,
                                    qr_redeemable_cumulated integer NULL,
                                    qr_redeemed_cumulated integer NULL,
                                    qr_not_redeemed_cumulated integer NULL,
                                    qr_redeemable_daily integer NULL,
                                    qr_redeemed_daily integer NULL,
                                    qr_not_redeemed_daily integer NULL,
                                    qr_redeemable_7days_avg integer NULL,
                                    qr_redeemed_7days_avg integer NULL,
                                    qr_not_redeemed_7days_avg integer NULL,
                                    qr_redeemable_7days_sum integer NULL,
                                    qr_not_redeemed_7days_sum integer NULL,
                                    qr_redeemed_7days_sum integer NULL,
                                    teletan_redeemable_cumulated integer NULL,
                                    teletan_redeemed_cumulated integer NULL,
                                    teletan_not_redeemed_cumulated integer NULL,
                                    teletan_redeemable_daily integer NULL,
                                    teletan_redeemed_daily integer NULL,
                                    teletan_not_redeemed_daily integer NULL,
                                    teletan_redeemable_7days_avg integer NULL,
                                    teletan_redeemed_7days_avg integer NULL,
                                    teletan_not_redeemed_7days_avg integer NULL,
                                    teletan_redeemable_7days_sum integer NULL,
                                    teletan_not_redeemed_7days_sum integer NULL,
                                    teletan_redeemed_7days_sum integer NULL,
                                    qr_teletan_redeemable_cumulated integer NULL,
                                    qr_teletan_redeemable_daily integer NULL,
                                    qr_teletan_redeemable_7days_avg integer NULL,
                                    qr_teletan_redeemable_7days_sum integer NULL,
                                    ppa_total_warnings_daily integer NULL,
                                    ppa_risk_red_daily integer NULL,
                                    ppa_risk_green_daily integer NULL,
                                    ppa_total_warnings_cumulated integer NULL,
                                    ppa_risk_red_cumulated integer NULL,
                                    ppa_risk_green_cumulated integer NULL,
                                    ppa_risk_red_7days_sum integer NULL,
                                    ppa_risk_green_7days_sum integer NULL,
                                    ppa_total_warnings_7days_avg integer NULL,
                                    ppa_risk_red_7days_avg integer NULL,
                                    ppa_risk_green_7days_avg integer NULL
                                );"""

    impf_lieferung = """CREATE TABLE IF NOT EXISTS impflieferung (
                                    id integer PRIMARY KEY AUTOINCREMENT,
                                    date text NOT NULL,
                                    impfstoff text NOT NULL,
                                    region text NOT NULL,
                                    dosen integer NOT NULL,
                                    einrichtung text NOT NULL                                   
                                );"""

    impf_zahlen = """CREATE TABLE IF NOT EXISTS impfzahlen (
                                    id integer PRIMARY KEY AUTOINCREMENT,
                                    date text NULL,
                                    dosen_kumulativ integer NULL,
                                    dosen_biontech_kumulativ integer NULL,
                                    dosen_biontech_erst_kumulativ integer NULL,
                                    dosen_biontech_zweit_kumulativ integer NULL,
                                    dosen_biontech_dritt_kumulativ integer NULL,
                                    dosen_moderna_kumulativ integer NULL,
                                    dosen_moderna_erst_kumulativ integer NULL,
                                    dosen_moderna_zweit_kumulativ integer NULL,
                                    dosen_moderna_dritt_kumulativ integer NULL,
                                    dosen_astra_kumulativ integer NULL,
                                    dosen_astra_erst_kumulativ integer NULL,
                                    dosen_astra_zweit_kumulativ integer NULL,
                                    dosen_astra_dritt_kumulativ integer NULL,
                                    dosen_johnson_kumulativ integer NULL,
                                    dosen_johnson_erst_kumulativ integer NULL,
                                    dosen_johnson_zweit_kumulativ integer NULL,
                                    dosen_johnson_dritt_kumulativ integer NULL,
                                    dosen_erst_kumulativ integer NULL,
                                    dosen_zweit_kumulativ integer NULL,
                                    dosen_dritt_kumulativ integer NULL,
                                    dosen_differenz_zum_vortag integer NULL,
                                    dosen_erst_differenz_zum_vortag integer NULL,
                                    dosen_zweit_differenz_zum_vortag integer NULL,
                                    dosen_dritt_differenz_zum_vortag integer NULL,
                                    dosen_vollstaendig_differenz_zum_vortag integer NULL,
                                    dosen_erst_unvollstaendig_differenz_zum_vortag integer NULL,
                                    personen_erst_kumulativ integer NULL,
                                    personen_voll_kumulativ integer NULL,
                                    personen_auffrisch_kumulativ integer NULL,
                                    impf_quote_erst integer NULL,
                                    impf_quote_voll integer NULL,
                                    dosen_dim_kumulativ integer NULL,
                                    dosen_kbv_kumulativ integer NULL,
                                    indikation_alter_dosen integer NULL,
                                    indikation_beruf_dosen integer NULL,
                                    indikation_medizinisch_dosen integer NULL,
                                    indikation_pflegeheim_dosen integer NULL,
                                    indikation_alter_erst integer NULL,
                                    indikation_beruf_erst integer NULL,
                                    indikation_medizinisch_erst integer NULL,
                                    indikation_pflegeheim_erst integer NULL,
                                    indikation_alter_voll integer NULL,
                                    indikation_beruf_voll integer NULL,
                                    indikation_medizinisch_voll integer NULL,
                                    indikation_pflegeheim_voll integer NULL                                
                                );"""

    covid_zahlen = """CREATE TABLE IF NOT EXISTS covid (
                                    id integer PRIMARY KEY AUTOINCREMENT,
                                    IdLandkreis integer NOT NULL,
                                    Altersgruppe text NOT NULL,
                                    Geschlecht text NOT NULL,
                                    Meldedatum text NOT NULL,
                                    Refdatum text NOT NULL,
                                    IstErkrankungsbeginn integer NOT NULL,
                                    NeuerFall integer NOT NULL,
                                    NeuerTodesfall integer NOT NULL,
                                    NeuGenesen integer NOT NULL,
                                    AnzahlFall integer NOT NULL,
                                    AnzahlTodesfall integer NOT NULL,
                                    AnzahlGenesen integer NOT NULL                              
                                );"""

    try:
        conn.execute(kh_liste)
        conn.execute(kh_meldebereiche)
        conn.execute(kh_status)
        conn.execute(fallzahlen)
        conn.execute(cwa)
        conn.execute(impf_lieferung)
        conn.execute(impf_zahlen)
        conn.execute(covid_zahlen)
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
            cur.execute(sql, (fall.datum, fall.bundesland, fall.gemeindeschluessel, fall.anzahl_standorte, fall.anzahl_meldebereiche, fall.faelle_covid_aktuell,
                        fall.faelle_covid_aktuell_invasiv_beatmet, fall.betten_frei, fall.betten_belegt, fall.betten_belegt_nur_erwachsen, fall.betten_frei_nur_erwachsen))
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
    cur.execute(
        "SELECT * FROM bettenstatus WHERE letzteMeldezeitpunkt = ?", (date,))
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
            cur.execute(sql, (kh.id, kh.bezeichnung, kh.strasse, kh.plz, kh.ort,
                        kh.bundesland, kh.ikNummer, kh.position, kh.gemeindeschluessel))
            conn.commit()

        for meldebereich in kh.meldebereiche:
            if meldebereich_not_exists(conn, meldebereich.meldebereichId):
                sql = ''' INSERT INTO meldebereiche(meldebereichId,kh_id,ardsNetzwerkMitglied,meldebereichBezeichnung,behandlungsschwerpunktL1,behandlungsschwerpunktL2,behandlungsschwerpunktL3) VALUES(?,?,?,?,?,?,?) '''
                cur = conn.cursor()
                cur.execute(sql, (meldebereich.meldebereichId, kh.id, meldebereich.ardsNetzwerkMitglied, meldebereich.meldebereichBezeichnung,
                            meldebereich.behandlungsschwerpunktL1, meldebereich.behandlungsschwerpunktL2, meldebereich.behandlungsschwerpunktL3))
                conn.commit()

        if bettenstatus_not_exists(conn, kh.letzteMeldezeitpunkt):
            sql = ''' INSERT INTO bettenstatus(kh_id,maxBettenStatusEinschaetzungEcmo,maxBettenStatusEinschaetzungHighCare,maxBettenStatusEinschaetzungLowCare,letzteMeldezeitpunkt) VALUES(?,?,?,?,?) '''
            cur = conn.cursor()
            cur.execute(sql, (kh.id, kh.maxBettenStatusEinschaetzungEcmo, kh.maxBettenStatusEinschaetzungHighCare,
                        kh.maxBettenStatusEinschaetzungLowCare, kh.letzteMeldezeitpunkt))
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
