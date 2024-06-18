import pandas as pd

def db_file_section(db_files):
    def section(file_name):
        patterns = {
            "_АР": "АР", "-АР": "АР",
            "_КР": "КР", "-КР": "КР",
            "_ВК": "ВК", "-ВК": "ВК",
            "_ОВ": "ОВ", "-ОВ": "ОВ",
            "_ИТП": "ИТП", "-ИТП": "ИТП",
            "_СС": "СС", "-СС": "СС",
            "_КЖ": "КР", "-КЖ": "КР",
            "_ЭОМ": "ЭОМ", "-ЭОМ": "ЭОМ",
            "_РФ": "РФ", "-РФ": "РФ",
            "_ТМ": "ИТП", "-ТМ": "ИТП",
            "_ПТ": "ВК", "-ПТ": "ВК",
            "_АИ": "АР", "-АИ": "АР",
            "_ТХ": "ТХ", "-ТХ": "ТХ",
            "_ВНС": "ВК", "-ВНС": "ВК",
            "_КФ": "КФ", "-КФ": "КФ",
            "-АПТ": "СС", "_АПТ": "СС",
            "-ТЭ": "АР", "_ТЭ": "АР"
        }
        for pattern, result in patterns.items():
            if pattern in file_name:
                return result
        return "Прочее"
    db_files['section'] = db_files['file_name'].apply(section)
    return db_files

def db_values_parameters(db_values, dax_unique_parametrs):
    return db_values.merge(dax_unique_parametrs[["parameter_id", "parameter_name"]], how='left', on='parameter_id')

def db_elements_category(db_elements, db_categories):
    db_elements = db_elements.merge(db_categories, how="left", on="category_id")
    def filtration(upload, reassigned_category, category_type, parent_id):
        if upload == 1 and  reassigned_category == "Модель" and category_type == "Model" and pd.isna(parent_id):
            return True
        else:
            return False
    db_elements["filtration"] = db_elements.apply(lambda row: filtration(row["upload"], row["reassigned_category"], row["category_type"], row["parent_id"]), axis=1)
    db_elements = db_elements.drop(columns={"built_in_сategory", "category_type", "reassigned_category", "parent_id", "upload"})
    return db_elements

def db_parametrs_type_parametrs(db_values, db_parametrs, asml_fop):
    db_values = db_values.groupby(["parameter_id"])["unique_keys"].count().reset_index()
    db_parametrs = db_parametrs.merge(db_values, how="left", on="parameter_id").fillna(0)
    db_parametrs = db_parametrs.merge(asml_fop, how="left", left_on="guid", right_on="Guid").fillna("0")
    res_list = []
    for from_shared, guid, parameter_id, Name, unique_keys in zip(db_parametrs["from_shared"], 
                                                                  db_parametrs["guid"], 
                                                                  db_parametrs["parameter_id"], 
                                                                  db_parametrs["Name"],
                                                                  db_parametrs["unique_keys"]):
        if unique_keys > 0:
            if from_shared == 0 and parameter_id > 5 and guid == "0" and Name == "0":
                res_list.append("Параметр из проекта")
            elif from_shared == 0 and parameter_id <= 5 and guid == "0" and Name == "0":
                res_list.append("Системный параметр")
            elif from_shared == 1 and parameter_id > 5 and guid != "0" and Name != "0":
                res_list.append("Параметр из ASML_ФОП")
            elif from_shared == 1 and parameter_id > 5 and guid != "0" and Name == "0":
                res_list.append("Параметр из неизвестного ФОП")
            else:
                res_list.append("Неизвестно")
        else:
           res_list.append("Неизвестно") 
    db_parametrs['type_parametrs'] = res_list
    db_parametrs.drop(["unique_keys", "Guid", "Name"], axis=1, inplace=True)
    return db_parametrs

def db_elements_search_duplicates(db_elements):
    res = []
    list_family = db_elements["family_name"].unique()
    for i in db_elements["family_name"]:
            if i != '':
                if i[:-1] in list_family:
                    res.append(True)
                else:
                    res.append(False)
            else:
                res.append(False)
    db_elements["search_duplicates"] = res
    return db_elements

def db_elements_status_family(db_elements, family_from_fm, family_sistem, db_values):

    db_values = db_values[db_values["parameter_id"] == 5][["unique_keys", "value"]]
    db_elements = db_elements.merge(family_from_fm[['id семейства', 'id версии', 'имя семейства', 'номер версии', 'статус']].drop_duplicates(), 
                                    how="left", left_on=["family_name", "family_id", "family_version_id"], right_on = ["имя семейства","id семейства", "id версии"]).rename(columns={"id семейства": "Id_семейства", "id версии": "Id_версии"})
    db_elements = db_elements.merge(family_sistem[["Семейство"]].drop_duplicates(), how="left", left_on="family_name", right_on="Семейство").rename(columns={"Семейство": "Системное семейство"})
    db_elements = db_elements.merge(db_values, how="left", on="unique_keys").rename(columns={"value": "model_contecst"})
    family_from_fm_for_family_not_unregistered = family_from_fm[family_from_fm["статус"] != "Редактируется"][["имя семейства"]].rename(columns={"имя семейства": "Семейство незарегистрированое"}).drop_duplicates()
    db_elements = db_elements.merge(family_from_fm_for_family_not_unregistered, how="left", left_on="family_name", right_on = "Семейство незарегистрированое")
    db_elements = db_elements.merge(family_from_fm[["id семейства", "id версии", 'имя семейства']].rename(columns={"имя семейства": "Семейство переименованное"}).drop_duplicates(), 
                                    how="left", left_on=["family_id", "family_version_id"], right_on = ["id семейства", "id версии"]).drop(columns={"id семейства", "id версии"})
    family_from_fm_edited = family_from_fm[family_from_fm["статус"] == "Редактируется"][["имя семейства"]].rename(columns={"имя семейства": "Семейство на исключение"}).drop_duplicates()
    db_elements = db_elements.merge(family_from_fm_edited, how="left", left_on="family_name", right_on = "Семейство на исключение")

    def status_family(family_name,
                      status,
                      family_sistem,
                      model_contecst,
                      family_not_unregistered,
                      duplicates,
                      family_rename,
                      family_edited):
        if status == "Разрешено":
            return "Семейство загружаемое"
        elif status == "Устарело":
            return "Обновить версию семейства"
        elif status == "Архив":
            return "Семейство в архиве"
        elif status == "Запрещено":
            return "Семейство запрещено"
        elif family_name == family_not_unregistered:
            return "Семейство незарегистрированное"
        elif not(pd.isna(family_sistem)):
            return "Семейство системное"
        elif model_contecst == "1.00":
            return "Модель в контексте"
        elif duplicates == True:
            return "Семейство задублировано"
        elif not(pd.isna(family_rename)):
            return "Семейство переименованное"
        elif not(pd.isna(family_edited)):
            return "Исключить"
        else:
            return "Семейство неизвестное"

    db_elements["status_family"] = db_elements.apply(lambda row: status_family(row["family_name"], 
                                                                               row["статус"],
                                                                               row["Системное семейство"],
                                                                               row["model_contecst"],
                                                                               row["Семейство незарегистрированое"],
                                                                               row["search_duplicates"],
                                                                               row["Семейство переименованное"],
                                                                               row['Семейство на исключение']
                                                                               ), axis=1)
    
    db_elements = db_elements.drop(columns={'имя семейства', 'Id_семейства', 'Id_версии',
                                            'номер версии', 'статус', 'Системное семейство', 'model_contecst',
                                            'Семейство незарегистрированое', 'Семейство переименованное',
                                            'Семейство на исключение', "search_duplicates"}).fillna("")
    
    
    db_elements["family_unique_key"] = db_elements.apply(lambda row: str(row["family_name"]) + "_" + str(row['family_id']) + "_" + str(row['family_version_id']), axis=1)

    return db_elements