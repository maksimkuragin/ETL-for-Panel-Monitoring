import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=UserWarning)

import pandas as pd
import pypyodbc
import datetime

from sql_query import sql_query_db_project
from sql_query import sql_query_db_files
from sql_query import sql_query_db_elements
from sql_query import sql_query_db_values
from sql_query import sql_query_db_parametrs
from sql_query import sql_query_db_categories
from sql_query import sql_query_family_manager

from excel_query import query_asml_fop
from excel_query import query_family_sistem
#from excel_query import query_family_from_fm
#from excel_query import query_table_distribution
#from excel_query import query_family_pe

from transform import db_file_section
from transform import db_values_parameters
from transform import db_elements_category
from transform import db_parametrs_type_parametrs
from transform import db_elements_search_duplicates
from transform import db_elements_status_family

from create_table import create_result_table
from create_table import create_dax_unique_parametrs
from create_table import create_dax_project_coordinates
from create_table import create_dax_ws_links
from create_table import create_dax_duplicate_parameters
from create_table import create_dax_navisworks
from create_table import create_dax_levels

from result import result_project_coordinates
from result import result_ws_links
from result import result_parameter_not_from_asml
from result import result_duplicate_parameters
from result import result_dax_navisworks
from result import result_dax_levels
from result import result_correct_family

conn = pypyodbc.connect(
    "Driver={SQL Server};"
    "Server=XXX;"
    "Database=XXX;"
    "UID=XXX;"
    "PWD=XXX;")

sql_db_project = sql_query_db_project()
db_project = pd.read_sql_query(sql_db_project, conn)

for name in db_project.name:
    try:
        print(name)
        print(datetime.datetime.now())
        asml_fop =           query_asml_fop()
        family_sistem =      query_family_sistem()
        
        sql_family_manager = sql_query_family_manager()
        family_from_fm =     pd.read_sql_query(sql_family_manager, conn)

        sql_db_files =      sql_query_db_files(name)
        sql_db_elements =   sql_query_db_elements(name)
        sql_db_values =     sql_query_db_values(name)
        sql_db_parametrs =  sql_query_db_parametrs(name)
        sql_db_categories = sql_query_db_categories()

        db_files =          pd.read_sql_query(sql_db_files, conn)
        db_elements =       pd.read_sql_query(sql_db_elements, conn)
        db_values =         pd.read_sql_query(sql_db_values, conn)
        db_parametrs =      pd.read_sql_query(sql_db_parametrs, conn)
        db_categories =     pd.read_sql_query(sql_db_categories, conn)

        db_files =                 db_file_section(db_files)
        result_table =             create_result_table(db_files)
        db_parametrs =             db_parametrs_type_parametrs(db_values, db_parametrs, asml_fop)
        dax_unique_parametrs =     create_dax_unique_parametrs(db_parametrs)
        db_values =                db_values_parameters(db_values, dax_unique_parametrs)
        dax_project_coordinates =  create_dax_project_coordinates(db_values, db_files)
        result_table =             result_project_coordinates(result_table, dax_project_coordinates)
        db_elements =              db_elements_category(db_elements, db_categories)
        dax_ws_links =             create_dax_ws_links(db_elements, db_values, db_files)
        result_table =             result_ws_links(result_table, dax_ws_links)
        result_table =             result_parameter_not_from_asml(result_table, dax_unique_parametrs, db_files)
        dax_duplicate_parameters = create_dax_duplicate_parameters(db_parametrs)
        result_table =             result_duplicate_parameters(result_table, dax_duplicate_parameters, db_files)
        dax_navisworks =           create_dax_navisworks(db_values, db_files)
        result_table =             result_dax_navisworks(result_table, dax_navisworks)
        dax_levels =               create_dax_levels(db_values, db_files)
        result_table =             result_dax_levels(result_table, dax_levels)
        db_elements =              db_elements_search_duplicates(db_elements)
        db_elements =              db_elements_status_family(db_elements, family_from_fm, family_sistem, db_values)
        result_table =             result_correct_family(result_table, db_elements, db_files)

        
        insert_query = """
        INSERT INTO Clash_TEST.dbo.Dynamics_for_PM 
        (datetime, project_id, section, errors_coordinates, errors_levels, errors_navisworks, errors_ws_links, errors_checking_parameters, correct_family) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor = conn.cursor()
        for row in zip(result_table.datetime, 
                    result_table.project_id, 
                    result_table.section, 
                    result_table.errors_coordinates, 
                    result_table.errors_levels, 
                    result_table.errors_navisworks, 
                    result_table.errors_ws_links, 
                    result_table.errors_checking_parameters, 
                    result_table.correct_family):
            cursor.execute(insert_query, row)
        conn.commit()
        cursor.close()
    except:
        print(f"Ошибка при обработке {name}")
        continue
conn.close()