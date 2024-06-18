import pandas as pd
import datetime

def create_dax_unique_parametrs(db_parametrs):
    db_parametrs = db_parametrs[db_parametrs["type_parametrs"] != "Неизвестно"]
    return db_parametrs.groupby(["parameter_id"]).min().reset_index()

def create_dax_project_coordinates(db_values, db_files):
    df = db_values[db_values['value'] == 'Базовая точка проекта'][["unique_keys"]]

    df_n_s = db_values[db_values["parameter_name"] == "С/Ю"][["unique_keys", "value"]]
    df = df.merge(df_n_s, how="left", on="unique_keys").rename(columns={"value": "С_Ю"})
    df["С_Ю"] = df["С_Ю"].apply(lambda x: round(float(x), 2))

    df_e_w = db_values[db_values["parameter_name"] == "В/З"][["unique_keys", "value"]]
    df = df.merge(df_e_w, how="left", on="unique_keys").rename(columns={"value": "В_З"})
    df["В_З"] = df["В_З"].apply(lambda x: round(float(x), 2))

    df_mark = db_values[db_values["parameter_name"] == "Отм"][["unique_keys", "value"]]
    df = df.merge(df_mark, how="left", on="unique_keys").rename(columns={"value": "Отм"})
    df["Отм"] = df["Отм"].apply(lambda x: round(float(x), 2))

    df_corner = db_values[db_values["parameter_name"] == "Угол от истинного севера"][["unique_keys", "value"]]
    df = df.merge(df_corner, how="left", on="unique_keys").rename(columns={"value": "Угол"})
    df["Угол"] = df["Угол"].apply(lambda x: round(float(x), 2))

    df["file_id"] = df["unique_keys"].apply(lambda x: x.split("_")[0])
    df = df.merge(db_files[["file_id", "section", "project_id"]].astype(str), how='left', on='file_id')
    df["type_file"] = df["section"].apply(lambda x: "Разбивочный файл" if x == "РФ" else "Рабочий файл")

    if df[df["type_file"] == "Разбивочный файл"]["type_file"].count() == 1:

        n_s = min(df[df["type_file"] == "Разбивочный файл"]["С_Ю"])
        e_w = min(df[df["type_file"] == "Разбивочный файл"]["В_З"])
        mark = min(df[df["type_file"] == "Разбивочный файл"]["Отм"])
        corner = min(df[df["type_file"] == "Разбивочный файл"]["Угол"])
        
        df['presence_of_errors'] = df.apply(lambda row: "Присутствуют" if row['С_Ю'] != n_s or 
                                                                        row['В_З'] != e_w or 
                                                                        row['Отм'] != mark or 
                                                                        row['Угол'] != corner 
                                                                        else "Отсутствуют", axis=1)
    else:
        df['presence_of_errors'] = "Отсутствуют"
    return df

def create_result_table(db_files):
    df = db_files.groupby(["section"])["project_id"].min().reset_index()
    df["datetime"] = datetime.datetime.now()
    return df 

def create_dax_ws_links(db_elements, db_values, db_files):
    result = db_elements[db_elements["category_name"] == "Связанные файлы"][["file_id", "unique_keys", "type_name"]]
    db_values = db_values[db_values["parameter_name"] == "Рабочий набор"]
    result = result.merge(db_values[["unique_keys", "value"]], how="left", on="unique_keys")
    result = result.merge(db_files[["file_id", "project_id", "section"]], how="left", on="file_id")
    result["status"] = result["value"].apply(lambda x: "Не учитывается" 
    if pd.isna(x) or "#" in x or "!" in x or "02_" in x or "_АР" in x or 
    "_ВК" in x or "_КФ" in x or "_ОВ" in x or "_ЭОМ" in x or "_СС" in x or 
    "_РФ" in x or "_КР" in x or "Связ" in x or "связ" in x else "Учитывается")
    return result.drop_duplicates()

def create_dax_duplicate_parameters(db_parametrs):
    db_parametrs = db_parametrs[db_parametrs["type_parametrs"] != "Неизвестно"]
    db_parametrs = db_parametrs.groupby(["file_id", "parameter_name"]).agg(
                                                                            parameter_count=('parameter_id', 'count'),
                                                                            type_parameters=('type_parametrs', ', '.join)).reset_index()
    db_parametrs = db_parametrs[db_parametrs["parameter_count"] > 1]
    db_parametrs["fact_verification"] = db_parametrs["type_parameters"].apply(lambda x: True if x.find("Параметр из") >= 0 else False)
    return db_parametrs

def create_dax_navisworks(db_values, db_files):
    db_values_navis = db_values[(db_values["parameter_name"] == "Имя вида") & (db_values["value"].str.lower().str.contains("navis"))]
    db_values_navis = db_values_navis[["unique_keys", "value"]].rename(columns={"value": "navis"})
    db_values_3d = db_values[db_values["parameter_name"] == "Границы 3D вида"]
    res = db_values_navis.merge(db_values_3d[["unique_keys", "value"]], how="left", on="unique_keys").rename(columns={"value": "borders_3D_view"})
    res["file_id"] = res["unique_keys"].apply(lambda x: x.split("_")[0])
    db_files = db_files[["file_id", "section"]].astype(str).merge(res, how="left", on="file_id")
    def determine_error_type(row, df):
        file_id = row['file_id']
        duplicates_count = df[df['file_id'] == file_id].shape[0]
        if duplicates_count > 1:
            return "Есть дубликат"
        if pd.isna(row['navis']):
            return "Вид отсутствует"
        if row['borders_3D_view'] == "1.00":
            return "Включена подрезка"
        return ""
    db_files['type_error'] = db_files.apply(determine_error_type, axis=1, df=db_files)
    return db_files

def create_dax_levels(db_values, db_files):

    df = db_values[db_values["value"] == "Уровень"][["unique_keys"]]
    df_unique_keys = df["unique_keys"].values
    df_name = db_values[(db_values["unique_keys"].isin(df_unique_keys)) & (db_values["parameter_name"] == "Имя")][["unique_keys", "value"]]
    df_benchmark = db_values[(db_values["unique_keys"].isin(df_unique_keys)) & (db_values["parameter_name"] == "Фасад")][["unique_keys", "value"]]
    df = df.merge(df_name, how="left", on="unique_keys").rename(columns={"value": "name"})
    df = df.merge(df_benchmark, how="left", on="unique_keys").rename(columns={"value": "benchmark"})

    df["benchmark"] = df["benchmark"].apply(lambda x: round(float(x.replace(",",".")) * 304.8) if pd.notnull(x) else x) 
    df["file_id"] = df["unique_keys"].apply(lambda x: x.split("_")[0])
    df = df.merge(db_files[["file_id", "section", "project_id", "file_name"]].astype(str), how='left', on='file_id')
    df["type_file"] = df["section"].apply(lambda x: "Разбивочный файл" if x == "РФ" else "Рабочий файл")

    df_breakdown_file = df[df['type_file'] == "Разбивочный файл"]
    df["levels_not_corresponding_bf"] = df.apply(lambda row: "Не соответствует" if row["name"] not in df_breakdown_file["name"].values else "", axis=1)

    df_work_file = df[(df['type_file'] == "Рабочий файл") & (df['benchmark'].notnull())]
    duplicate_counts = df_work_file.groupby(["file_id", "benchmark"]).size().reset_index(name="duplicate_levels")
    duplicate_counts = duplicate_counts[duplicate_counts["duplicate_levels"] > 1]

    df = df.merge(duplicate_counts, how="left", on=["file_id", "benchmark"])
    df["duplicate_levels"] = df.apply(lambda row: f"Модель: {row['file_name']} дублируется отметка: {row['benchmark']}" if pd.notnull(row["duplicate_levels"]) else "", axis=1)

    def examination_level_and_benchmark(name, benchmark, levels_not_corresponding_bf):
        if levels_not_corresponding_bf == "":
            if str(name)[:4] == "АР_-" or str(name)[:4] == "АР_+" or str(name)[:4] == "КР_-" or str(name)[:4] == "КР_+":
                benchmark_from_name = int(round(float(name.replace(",",".").split("_")[1]) * 1000))
                try:
                    if int(benchmark) != 0:
                        if benchmark_from_name / 1000 == int(benchmark):
                            return "Нет '.' в имени уровня"
                        elif int(benchmark) > benchmark_from_name:
                            return "Отметка уровная выше чем в имени"
                        elif int(benchmark) < benchmark_from_name:
                            return "Отметка уровная ниже чем в имени"
                        else:
                            return ""
                except:
                    return ""
        return ""
    
    df["examination_level_and_benchmark"] = df.apply(lambda row: examination_level_and_benchmark(row["name"], row["benchmark"], row["levels_not_corresponding_bf"]), axis=1)

    return df