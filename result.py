import pandas as pd

def result_project_coordinates(result_table, dax_project_coordinates):
    dax_project_coordinates["errors_coordinates"] = dax_project_coordinates["presence_of_errors"].apply(lambda x: 1 if x == "Присутствуют" else 0)
    dax_project_coordinates = dax_project_coordinates.groupby(["section"])["errors_coordinates"].sum().reset_index()
    result_table = result_table.merge(dax_project_coordinates, how="left", on="section")
    return result_table.fillna(0)

def result_ws_links(result_table, dax_ws_links):
    dax_ws_links = dax_ws_links[dax_ws_links["status"] == "Учитывается"]
    dax_ws_links = dax_ws_links.groupby(["section"])["file_id"].nunique().reset_index()
    dax_ws_links = dax_ws_links.rename(columns={"file_id": "errors_ws_links"})
    result_table = result_table.merge(dax_ws_links, how="left", on="section")
    return result_table.fillna(0)

def result_parameter_not_from_asml(result_table, db_parametrs, db_files):
    db_parametrs = db_parametrs[(db_parametrs["type_parametrs"] == "Параметр из проекта") | (db_parametrs["type_parametrs"] == "Параметр из неизвестного ФОП")]
    db_parametrs = db_parametrs.merge(db_files[["file_id", "section"]], how="left", on="file_id")
    db_parametrs = db_parametrs.groupby(["section"])["file_id"].nunique().reset_index()
    db_parametrs = db_parametrs.rename(columns={"file_id": "errors_checking_parameters"})
    result_table = result_table.merge(db_parametrs, how="left", on="section")
    return result_table.fillna(0)

def result_duplicate_parameters(result_table, dax_duplicate_parameters, db_files):
    dax_duplicate_parameters = dax_duplicate_parameters[dax_duplicate_parameters["fact_verification"] == True]
    dax_duplicate_parameters = dax_duplicate_parameters.merge(db_files[["file_id", "section"]], how="left", on="file_id")
    dax_duplicate_parameters = dax_duplicate_parameters.groupby(["section"])["file_id"].nunique().reset_index()
    dax_duplicate_parameters = dax_duplicate_parameters.rename(columns={"file_id": "errors_checking_parameters"})
    result_table = result_table.merge(dax_duplicate_parameters, how="left", on="section").fillna(0)
    result_table["errors_checking_parameters"] = result_table["errors_checking_parameters_x"] + result_table["errors_checking_parameters_y"]
    result_table = result_table.drop(columns=["errors_checking_parameters_x", "errors_checking_parameters_y"])
    return result_table

def result_dax_navisworks(result_table, dax_navisworks):

    no_vie = dax_navisworks[dax_navisworks["type_error"] == "Вид отсутствует"]
    no_vie = no_vie.groupby(["section"])["file_id"].nunique().reset_index()

    borders_3D_view = dax_navisworks[dax_navisworks["type_error"] == "Включена подрезка"]
    borders_3D_view = borders_3D_view.groupby(["section"])["file_id"].nunique().reset_index()

    there_duplicate = dax_navisworks[dax_navisworks["type_error"] == "Есть дубликат"]
    there_duplicate = there_duplicate.groupby(["section"])["file_id"].nunique().reset_index()

    result_table = result_table.merge(no_vie, how="left", on="section").rename(columns={"file_id": "errors_no_vie"}).fillna(0)
    result_table = result_table.merge(borders_3D_view, how="left", on="section").rename(columns={"file_id": "errors_borders_3D_view"}).fillna(0)
    result_table = result_table.merge(there_duplicate, how="left", on="section").rename(columns={"file_id": "errors_there_duplicate"}).fillna(0)

    result_table["errors_navisworks"] = result_table["errors_no_vie"] + result_table["errors_borders_3D_view"] + result_table["errors_there_duplicate"]
    result_table = result_table.drop(columns=["errors_no_vie", "errors_borders_3D_view", "errors_there_duplicate"])
    return result_table

def result_dax_levels(result_table, dax_levels):
    levels_not_corresponding_bf = dax_levels[dax_levels["levels_not_corresponding_bf"] == "Не соответствует"]
    levels_not_corresponding_bf = levels_not_corresponding_bf.groupby(["section"])["levels_not_corresponding_bf"].count().reset_index()
    result_table = result_table.merge(levels_not_corresponding_bf, how="left", on="section").fillna(0)

    examination_level_and_benchmark = dax_levels[dax_levels["examination_level_and_benchmark"] != ""]
    examination_level_and_benchmark = examination_level_and_benchmark.groupby(["section"])["examination_level_and_benchmark"].count().reset_index()
    result_table = result_table.merge(examination_level_and_benchmark, how="left", on="section").fillna(0)

    duplicate_levels = dax_levels[dax_levels["duplicate_levels"] != ""]
    duplicate_levels = duplicate_levels.groupby(["section"])["duplicate_levels"].nunique().reset_index()
    result_table = result_table.merge(duplicate_levels, how="left", on="section").fillna(0)

    result_table["errors_levels"] = result_table["levels_not_corresponding_bf"] + result_table["examination_level_and_benchmark"] + result_table["duplicate_levels"]
    result_table = result_table.drop(columns=["levels_not_corresponding_bf", "examination_level_and_benchmark", "duplicate_levels"])
    return result_table

def result_correct_family(result_table, db_elements, db_files):
    db_elements = db_elements[db_elements["filtration"] == True]
    db_elements = db_elements.merge(db_files, how="left", on="file_id")
    db_elements = db_elements[(db_elements["section"] != "РФ") & (db_elements["section"] != "КФ")]
    db_elements = db_elements[(db_elements["family_name"] != "Связанная модель Revit") & (db_elements["family_name"] != "Растровое изображение")]
    results_dict = {}
    for section in result_table["section"].values:
       db_elements_section = db_elements[db_elements["section"] == section]
       distinct_family_unique_key = db_elements_section[(db_elements_section["status_family"] != "Исключить") & (db_elements_section["status_family"] != "Модель в контексте")]["family_unique_key"].unique()
       distinct_family_unique_key_current = db_elements_section[(db_elements_section["status_family"] == "Семейство загружаемое") | (db_elements_section["status_family"] == "Семейство системное")]["family_unique_key"].unique()
       try:
         result = round(len(distinct_family_unique_key_current) / len(distinct_family_unique_key), 2)
       except:
          result = 0
       results_dict[section] = result
       result_table["correct_family"] = result_table["section"].map(results_dict)
    return result_table