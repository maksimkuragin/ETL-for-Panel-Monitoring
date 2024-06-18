import pandas as pd 

def query_asml_fop():
    path = r"//sd.local/G/BIM/SHARED/02_REVIT/02_Настройки/00_ФОП/ASML_ФОП.txt"
    with open(path, "r", encoding="utf-16le") as file:
        data = list()
        for line in file:
            values = line.strip().split('\t')
            data.append(values)
    df = pd.DataFrame(data)
    df = df[df[0] == "PARAM"].reset_index()
    df = df[[1,2]]
    df = df.rename(columns={1: "Guid", 2: "Name"})
    return df 

# def query_family_from_fm():
#     df = pd.read_excel(r"//sd.local/G/BIM/WIP/00_DATA/Отчеты PBI/Шаблоны/Панель мониторинга/Системное/Выгрузка FM.xlsx")
#     df = df[["Id семейства", "Id версии", "Имя семейства", "Номер версии", "Статус", "Функциональный Подтип", "Функциональный Тип"]]
#     df = df.drop_duplicates()
#     return df

def query_family_sistem():
    df = pd.read_excel(r"//sd.local/G/BIM/WIP/00_DATA/Проекты развития/17_Отчёты. Аналитика/01_Панель мониторинга/Оптимизация/Данные для отчета/Сиситемные семейства.xlsx")
    df = df[["Типоразмер семейства", "Семейство", "Раздел"]]
    df = df.drop_duplicates()
    return df

# def query_table_distribution():
#     df = pd.read_excel(r"//sd.local/G/BIM/WIP/00_DATA/Проекты развития/17_Отчёты. Аналитика/01_Панель мониторинга/Оптимизация/Данные для отчета/BIM_Распределение по проектам.xlsx", sheet_name="Сводная таблица")
#     df = df[["Название объекта", "Краткое наименование", "Этап", "Дом", "Стадия", "Статус", "Версия бренд-листа", "Класс жилья", "Регион", "Версия EIR", "Версия Revit", "Ответственный", "АР", "КР", "ОВ", "ВК", "ЭОМ", "СС", "Код проекта ПМ"]]
#     df = df.drop_duplicates()
#     return df

# def query_family_pe():
#     sections = ["АР_Семейства", "ВК_Семейства", "КР_Семейства", "ИТП_Семейства", "ОВ_Семейства", "СС_Семейства", "ЭОМ_Семейства"]
#     df_res = pd.DataFrame({'Проектный элемент' : [], 'Семейство' : [], 'Раздел' : []})
#     for section in sections:
#         df = pd.read_excel(r"//sd.local/G/BIM/WIP/00_DATA/Проекты развития/17_Отчёты. Аналитика/01_Панель мониторинга/Оптимизация/Данные для отчета/Обоснование объемов моделирования.xlsx", sheet_name=section)
#         df = df[["Проектный элемент", "Семейство"]]
#         df = df.dropna(subset=["Семейство"])
#         df = df[(df["Семейство"] != "–") & (df["Семейство"] != "-") & (df["Семейство"] != "/xa0")]
#         df["Раздел"] = section.split("_")[0]
#         df_res = pd.concat([df_res, df])
#     return df_res