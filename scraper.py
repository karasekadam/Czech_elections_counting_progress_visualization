import pandas as pd
import os
import requests
import xml.etree.ElementTree as ET
import shutil
import time
import playsound


http_2018 = "https://www.volby.cz/pls/prez2018/vysledky_okrsky?kolo=1&davka="
http_2018_2 = "https://www.volby.cz/pls/prez2018/vysledky_okrsky?kolo=2&davka="
http_2023 = "https://www.volby.cz/pls/prez2023/vysledky_okrsky?kolo=1&davka="
http_2023_2 = "https://www.volby.cz/pls/prez2023/vysledky_okrsky?kolo=2&davka="
pos_2021 = "https://www.volby.cz/pls/ps2021/vysledky_okrsky?davka="
pos_2017 = "https://www.volby.cz/pls/ps2017/vysledky_okrsky?davka="
euro_2019 = "https://www.volby.cz/pls/ep2019/vysledky_okrsky?davka="


prez_election_2018_dict = {
    "1": "Topolánek",
    "2": "Horáček",
    "3": "Drahoš",
    "4": "Fischer",
    "5": "Hannig",
    "6": "Kulhánek",
    "7": "Zeman",
    "8": "Hilšer",
    "9": "Drahoš"
}

prez_election_2023_dict = {
    "1": "Fisher",
    "2": "Bašta",
    "4": "Pavel",
    "5": "Zima",
    "6": "Nerudová",
    "7": "Babiš",
    "8": "Diviš",
    "9": "Hilšer"
}

pos_election_2021_dict = {
    "1": "Strana zelených",
    "2": "Švýcarská demokracie",
    "3": "VOLNÝ blok",
    "4": "Svoboda a př. demokracie (SPD)",
    "5": "Česká str.sociálně demokrat.",
    "6": "Volte Pr.Blok www.cibulka.net",
    "7": "ALIANCE NÁRODNÍCH SIL",
    "8": "Trikolora Svobodní Soukromníci",
    "9": "Aliance pro budoucnost",
    "10": "Hnutí Prameny",
    "11": "Levice",
    "12": "PŘÍSAHA Roberta Šlachty",
    "13": "SPOLU – ODS, KDU-ČSL, TOP 09",
    "14": "SENIOŘI 21",
    "15": "Urza.cz: Nechceme vaše hlasy",
    "16": "Koruna Česká (monarch.strana)",
    "17": "PIRÁTI a STAROSTOVÉ",
    "18": "Komunistická str.Čech a Moravy",
    "19": "Moravské zemské hnutí",
    "20": "ANO 2011",
    "21": "Otevřeme ČR normálnímu životu",
    "22": "Moravané"
}


def try_get(http, num_of_iter):
    i = 0
    while i < num_of_iter:
        try:
            davka = requests.get(http, timeout=5)
            return davka
        except Exception as e:
            print(e)


def delete_changed_dist(changed_dist, file_path):
    if not changed_dist:
        return
    print("mazani zmenenych")
    df = pd.read_csv(file_path)
    # print(df)
    for i in changed_dist:
        old_results = df[(df["OBEC"] == int(i[1])) & (df["OKRSEK"] == int(i[0]))].index
        if len(old_results) == 1:
            df = df.drop(old_results[0])
        elif len(old_results) == 0:
            print("Při mazání změněných to nenašlo co smazat for some reason")
        else:
            raise Exception("some error with deleting changed results")
    # print(df)
    df.to_csv(file_path, index=False)


def get_wave(wave_number: int, url_base: str, results_dir: str, cand_dict: dict,
             elec_type: str, demography_file: str) -> None:
    party_ident = ""
    if elec_type == "ps":
        party_ident = "KSTRANA"
    elif elec_type == "prezident":
        party_ident = "PORADOVE_CISLO"

    data = ['OBEC', 'OKRSEK', 'SOUCET_HLASU', 'ucast', 'ZAPSANI_VOLICI']
    data += [cand_name + "_votes" for cand_name in list(cand_dict.values())]
    data += [cand_name + "_perc" for cand_name in list(cand_dict.values())]
    df = pd.DataFrame(columns=data)

    print("zpracovává se vlna " + str(wave_number))
    url = url_base + str(wave_number)
    print(url)
    davka = try_get(url, 5)
    print("url: " + str(davka))
    root = ET.fromstring(davka.text)

    changed_dist = []
    for okrsek in root.findall('{http://www.volby.cz/' + elec_type + '/}OKRSEK'):
        if okrsek.attrib["OPAKOVANE"] != '0':
            print("opakovane")
            changed_dist.append((okrsek.attrib["CIS_OKRSEK"], okrsek.attrib["CIS_OBEC"]))

        # vytahá obecná data k jednomu okrsku
        new_row = dict.fromkeys(data)

        obec = okrsek.attrib["CIS_OBEC"]
        new_row["OBEC"] = obec
        cis_okrsek = okrsek.attrib["CIS_OKRSEK"]
        new_row["OKRSEK"] = cis_okrsek
        ucast = okrsek.find('{http://www.volby.cz/' + elec_type + '/}UCAST_OKRSEK').attrib
        platne_hlasy = ucast["PLATNE_HLASY"]
        new_row["SOUCET_HLASU"] = platne_hlasy
        zapsani_volici = ucast["ZAPSANI_VOLICI"]
        new_row["ucast"] = (int(platne_hlasy) / int(zapsani_volici)) * 100
        new_row["ZAPSANI_VOLICI"] = zapsani_volici

        for cand in okrsek.findall('{http://www.volby.cz/' + elec_type + '/}HLASY_OKRSEK'):
            # vytahá data o hlasech pro jednotlivé strany
            cand_number = cand.attrib[party_ident]
            cand_name = cand_dict[cand_number]
            new_row[cand_name + "_votes"] = int(cand.attrib["HLASY"])
            new_row[cand_name + "_perc"] = round((int(cand.attrib["HLASY"]) / int(platne_hlasy)) * 100, 3)

        # df = df.append(new_row, ignore_index=True)
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df_obce = pd.read_csv(demography_file, index_col=0)
    df["OBEC"] = df["OBEC"].astype(int)
    df = df.merge(df_obce, how="left", on="OBEC")
    df = df.fillna(0)

    if not os.path.exists(results_dir):
        os.mkdir(results_dir)

    if os.path.exists(results_dir + "/" + str(wave_number - 1) + ".csv"):
        shutil.copyfile(results_dir + "/" + str(wave_number - 1) + ".csv", results_dir + "/" + str(wave_number) + ".csv")
        delete_changed_dist(changed_dist, results_dir + "/" + str(wave_number) + ".csv")
        df.to_csv(results_dir + "/" + str(wave_number) + ".csv", header=False, mode="a", index=False)
    else:
        if wave_number != 1:
            raise Exception("Error! No previous file!")
        else:
            df.to_csv(results_dir + "/" + str(wave_number) + ".csv", index=False)


def scrape_loop(http_waves: str, curr_iter: int, file_dir: str, elec_type: str, candidate_dict: dict, demography_file: str) -> None:
    while True:
        davka = try_get(http_waves + str(curr_iter), 5)
        root = ET.fromstring(davka.text)
        xml_error = root.find('{http://www.volby.cz/' + elec_type + '/}CHYBA')
        if not root:
            raise Exception("Some error with getting the xml")
        if xml_error is None:
            get_wave(curr_iter, http_waves, file_dir, candidate_dict, elec_type, demography_file)
            curr_iter += 1
            # playsound.playsound("dalsi_vlna.mp3")
            print("got a wave")
        else:
            print("waiting for next wave")
            print("xml error: " + str(root.find('{http://www.volby.cz/' + elec_type + '/}CHYBA').attrib["KOD_CHYBY"]))
            time.sleep(30)


if __name__ == "__main__":
    scrape_loop(pos_2021, 1, "pos_2021", "ps", pos_election_2021_dict, "mesta_pocet_volicu.csv")
