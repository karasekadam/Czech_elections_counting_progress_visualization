import pandas as pd
import os


def results_by_okres():
    municipality_register = pd.read_csv("pscoco.csv")
    okres_register = pd.read_csv("cnumnuts.csv")
    register = municipality_register.merge(okres_register, left_on="OKRES", right_on="NUMNUTS")[["OBEC", "NUTS"]]

    results_files = os.listdir("pos_2021")

    for file in results_files:
        if file.endswith(".csv"):
            df = pd.read_csv(f"pos_2021/{file}")
            df = df.merge(register, left_on="OBEC", right_on="OBEC")
            df = df.drop(columns=["OBEC", "OKRSEK", "NAZEVOBCE", "VOLICI_V_OBCI", "ucast"])
            df["ucast"] = df["SOUCET_HLASU"] / df["ZAPSANI_VOLICI"]
            df_grouped = df.groupby("NUTS").sum()
            df_grouped.to_csv(f"pos_2021_by_okres/{file}", index=True)


def counted_perc_by_okres():
    results_files = os.listdir("pos_2021_by_okres")
    results = {}

    for file in results_files:
        if file.endswith(".csv"):
            df = pd.read_csv(f"pos_2021_by_okres/{file}")
            results[file.replace(".csv", "")] = df

    last_result = results["75"][["NUTS", "SOUCET_HLASU"]]

    for file, result_df in results.items():
        df_counted = result_df[["NUTS", "SOUCET_HLASU"]]
        merged = last_result.merge(df_counted, left_on="NUTS", right_on="NUTS", how="left", suffixes=("_total", "_current"))
        merged["SOUCET_HLASU_current"] = merged["SOUCET_HLASU_current"].fillna(0)
        merged["counted"] = merged["SOUCET_HLASU_current"] / merged["SOUCET_HLASU_total"] * 100
        merged["counted"] = merged["counted"].round(decimals=3)
        merged = merged.drop(columns=["SOUCET_HLASU_total", "SOUCET_HLASU_current"])
        merged = merged.set_index("NUTS")
        merged.to_csv(f"pos_2021_by_okres_counted/{file}.csv", index=True)


def perc_by_party_at_wave ():
    results_files = os.listdir("pos_2021_by_okres")

    for file in results_files:
        df = pd.read_csv(f"pos_2021_by_okres/{file}")
        df_sum = df.sum()
        df_sum = df_sum[["SOUCET_HLASU", "Svoboda a př. demokracie (SPD)_votes", "SPOLU – ODS, KDU-ČSL, TOP 09_votes", "PIRÁTI a STAROSTOVÉ_votes", "ANO 2011_votes"]]
        for column in df_sum.index:
            if column != "SOUCET_HLASU":
                column_percent = column.replace("_votes", "") + "_percent"
                df_sum[column_percent] = (df_sum[column] / df_sum["SOUCET_HLASU"] * 100).round(decimals=2)
        df_sum.to_csv(f"pos_2021_by_okres_perc/{file}")


if __name__ == "__main__":
    # results_by_okres()
    # counted_perc_by_okres()
    perc_by_party_at_wave()

