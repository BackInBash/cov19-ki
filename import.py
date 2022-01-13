# Import Data from intensivregister.de to SQLite DB

import threading
from db import create_struct, add_kh, add_fallzahl, write_to_file, execute_sql
from dataclasses import dataclass
from uuid import UUID
import requests
import csv
from io import StringIO
import re

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

threads = []

def import_impfzahlen():
    data = csv.reader(StringIO(requests.get("https://impfdashboard.de/static/data/germany_vaccinations_timeseries_v2.tsv").text))
    sql = """INSERT INTO "impfzahlen" ("date","dosen_kumulativ","dosen_biontech_kumulativ","dosen_biontech_erst_kumulativ","dosen_biontech_zweit_kumulativ","dosen_biontech_dritt_kumulativ","dosen_moderna_kumulativ","dosen_moderna_erst_kumulativ","dosen_moderna_zweit_kumulativ","dosen_moderna_dritt_kumulativ","dosen_astra_kumulativ","dosen_astra_erst_kumulativ","dosen_astra_zweit_kumulativ","dosen_astra_dritt_kumulativ","dosen_johnson_kumulativ","dosen_johnson_erst_kumulativ","dosen_johnson_zweit_kumulativ","dosen_johnson_dritt_kumulativ","dosen_erst_kumulativ","dosen_zweit_kumulativ","dosen_dritt_kumulativ","dosen_differenz_zum_vortag","dosen_erst_differenz_zum_vortag","dosen_zweit_differenz_zum_vortag","dosen_dritt_differenz_zum_vortag","dosen_vollstaendig_differenz_zum_vortag","dosen_erst_unvollstaendig_differenz_zum_vortag","personen_erst_kumulativ","personen_voll_kumulativ","personen_auffrisch_kumulativ","impf_quote_erst","impf_quote_voll","dosen_dim_kumulativ","dosen_kbv_kumulativ","indikation_alter_dosen","indikation_beruf_dosen","indikation_medizinisch_dosen","indikation_pflegeheim_dosen","indikation_alter_erst","indikation_beruf_erst","indikation_medizinisch_erst","indikation_pflegeheim_erst","indikation_alter_voll","indikation_beruf_voll","indikation_medizinisch_voll","indikation_pflegeheim_voll") VALUES """
    next(data)
    for row in data:
        tsv = row[0].split('\t')
        sql = sql+"('"+tsv[0]+"','"+tsv[1]+"',"+str(tsv[2])+","+str(tsv[3])+","+str(tsv[4])+","+str(tsv[5])+","+str(tsv[6])+","+str(tsv[7])+","+str(tsv[8])+","+str(tsv[9])+","+str(tsv[10])+","+str(tsv[11])+","+str(tsv[12])+","+str(tsv[13])+","+str(tsv[14])+","+str(tsv[15])+","+str(tsv[16])+","+str(tsv[17])+","+str(tsv[18])+","+str(tsv[19])+","+str(tsv[20])+","+str(tsv[21])+","+str(tsv[22])+","+str(tsv[23])+","+str(tsv[24])+","+str(tsv[25])+","+str(tsv[26])+","+str(tsv[27])+","+str(tsv[28])+","+str(tsv[29])+","+str(tsv[30])+","+str(tsv[31])+","+str(tsv[32])+","+str(tsv[33])+","+str(tsv[34])+","+str(tsv[35])+","+str(tsv[36])+","+str(tsv[37])+","+str(tsv[38])+","+str(tsv[39])+","+str(tsv[40])+","+str(tsv[41])+","+str(tsv[42])+","+str(tsv[43])+","+str(tsv[44])+","+str(tsv[45])+"),"
    sql = re.sub(r'.$', ';', sql)
    execute_sql(sql)

def import_impf_lieferung():
    data = csv.reader(StringIO(requests.get("https://impfdashboard.de/static/data/germany_deliveries_timeseries_v2.tsv").text))
    sql = """INSERT INTO "impflieferung" ("date","impfstoff","region","dosen","einrichtung") VALUES """
    next(data)
    for row in data:
        tsv = row[0].split('\t')
        sql = sql+"('"+tsv[0]+"','"+tsv[1]+"','"+tsv[2]+"','"+tsv[3]+"','"+tsv[4]+"'),"
    sql = re.sub(r'.$', ';', sql)
    execute_sql(sql)

def import_cwa():
    data = requests.get("https://obs.eu-de.otc.t-systems.com/obs-public-dashboard/json/v1/nested_cwa_public_dashboard_data.json").json()

    sql = """INSERT INTO "cwa" ("effective_date","update_timestamp","infections_published_daily","app_downloads_cumulated","app_downloads_android_cumulated","app_downloads_ios_cumulated","app_downloads_daily","app_downloads_android_daily","app_downloads_ios_daily","app_downloads_7days_avg","app_downloads_7days_sum","app_downloads_android_7days_avg","app_downloads_android_7days_sum","app_downloads_ios_7days_avg","app_downloads_ios_7days_sum","tests_total_cumulated","tests_total_daily","tests_total_7days_avg","tests_pcr_total_cumulated","tests_pcr_positive_cumulated","tests_pcr_negative_cumulated","tests_pcr_invalide_cumulated","tests_pcr_total_daily","tests_pcr_positive_daily","tests_pcr_negative_daily","tests_pcr_invalide_daily","tests_pcr_total_7days_avg","tests_pcr_positive_7days_avg","tests_pcr_negative_7days_avg","tests_pcr_invalide_7days_avg","tests_pcr_total_7days_sum","tests_pcr_positive_7days_sum","tests_pcr_negative_7days_sum","tests_pcr_invalide_7days_sum","tests_rat_total_cumulated","tests_rat_positive_cumulated","tests_rat_negative_cumulated","tests_rat_invalide_cumulated","tests_rat_total_daily","tests_rat_positive_daily","tests_rat_negative_daily","tests_rat_invalide_daily","tests_rat_total_7days_avg","tests_rat_positive_7days_avg","tests_rat_negative_7days_avg","tests_rat_invalide_7days_avg","tests_rat_total_7days_sum","tests_rat_positive_7days_sum","tests_rat_negative_7days_sum","tests_rat_invalide_7days_sum","qr_redeemable_cumulated","qr_redeemed_cumulated","qr_not_redeemed_cumulated","qr_redeemable_daily","qr_redeemed_daily","qr_not_redeemed_daily","qr_redeemable_7days_avg","qr_redeemed_7days_avg","qr_not_redeemed_7days_avg","qr_redeemable_7days_sum","qr_not_redeemed_7days_sum","qr_redeemed_7days_sum","teletan_redeemable_cumulated","teletan_redeemed_cumulated","teletan_not_redeemed_cumulated","teletan_redeemable_daily","teletan_redeemed_daily","teletan_not_redeemed_daily","teletan_redeemable_7days_avg","teletan_redeemed_7days_avg","teletan_not_redeemed_7days_avg","teletan_redeemable_7days_sum","teletan_not_redeemed_7days_sum","teletan_redeemed_7days_sum","qr_teletan_redeemable_cumulated","qr_teletan_redeemable_daily","qr_teletan_redeemable_7days_avg","qr_teletan_redeemable_7days_sum","ppa_total_warnings_daily","ppa_risk_red_daily","ppa_risk_green_daily","ppa_total_warnings_cumulated","ppa_risk_red_cumulated","ppa_risk_green_cumulated","ppa_risk_red_7days_sum","ppa_risk_green_7days_sum","ppa_total_warnings_7days_avg","ppa_risk_red_7days_avg","ppa_risk_green_7days_avg") VALUES """
    for row in data[0]['data']['daily']:
        sql = sql+"('"+row[0]+"','"+row[1]+"',"+str(row[2])+","+str(row[3])+","+str(row[4])+","+str(row[5])+","+str(row[6])+","+str(row[7])+","+str(row[8])+","+str(row[9])+","+str(row[10])+","+str(row[11])+","+str(row[12])+","+str(row[13])+","+str(row[14])+","+str(row[15])+","+str(row[16])+","+str(row[17])+","+str(row[18])+","+str(row[19])+","+str(row[20])+","+str(row[21])+","+str(row[22])+","+str(row[23])+","+str(row[24])+","+str(row[25])+","+str(row[26])+","+str(row[27])+","+str(row[28])+","+str(row[29])+","+str(row[30])+","+str(row[31])+","+str(row[32])+","+str(row[33])+","+str(row[34])+","+str(row[35])+","+str(row[36])+","+str(row[37])+","+str(row[38])+","+str(row[39])+","+str(row[40])+","+str(row[41])+","+str(row[42])+","+str(row[43])+","+str(row[44])+","+str(row[45])+","+str(row[46])+","+str(row[47])+","+str(row[48])+","+str(row[49])+","+str(row[50])+","+str(row[51])+","+str(row[52])+","+str(row[53])+","+str(row[54])+","+str(row[55])+","+str(row[56])+","+str(row[57])+","+str(row[58])+","+str(row[59])+","+str(row[60])+","+str(row[62])+","+str(row[63])+","+str(row[64])+","+str(row[65])+","+str(row[66])+","+str(row[67])+","+str(row[68])+","+str(row[69])+","+str(row[70])+","+str(row[71])+","+str(row[72])+","+str(row[73])+","+str(row[74])+","+str(row[75])+","+str(row[76])+","+str(row[77])+","+str(row[78])+","+str(row[79])+","+str(row[80])+","+str(row[81])+","+str(row[82])+","+str(row[83])+","+str(row[84])+","+str(row[85])+","+str(row[86])+","+str(row[87])+","+str(row[88])+","+str(row[89])+"),"
    sql = re.sub(r'.$', ';', sql)
    sql = sql.replace("None", "NULL")
    execute_sql(sql)


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

        t = threading.Thread(target=add_kh, args=(kh,))
        t.start()
        threads.append(t)

def csv_import_landkreis():
    data = csv.reader(StringIO(requests.get("https://diviexchange.blob.core.windows.net/%24web/zeitreihe-tagesdaten.csv").text))
    # Skip CSV Header
    next(data)
    sql = """INSERT INTO "fallzahlen" ("datum","bundesland","gemeindeschluessel","anzahl_standorte","anzahl_meldebereiche","faelle_covid_aktuell","faelle_covid_aktuell_invasiv_beatmet","betten_frei","betten_belegt","betten_belegt_nur_erwachsen","betten_frei_nur_erwachsen") VALUES """
    for row in data:
        sql = sql+"('"+row[0]+"',"+row[1]+","+row[2]+","+row[3]+","+row[4]+","+row[5]+","+row[6]+","+row[7]+","+row[8]+","+row[9]+","+row[10]+"),"
    sql = re.sub(r'.$', ';', sql)
    execute_sql(sql)


def is_thread_running(x):
    if x.is_alive():
        return 1
    else:
        return 0

create_struct()
import_kh()
csv_import_landkreis()
import_cwa()
import_impf_lieferung()
import_impfzahlen()

for t in threads:
    t.join()

write_to_file()