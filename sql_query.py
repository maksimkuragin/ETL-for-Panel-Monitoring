def sql_query_db_project():
    sql_query = """SELECT Name FROM [RevitElementsParameters ].dbo.Project_DB
                   WHERE Status <> 'Не выгружать'"""
    return sql_query

def sql_query_db_files(name):
    sql_query = """WITH project AS
                    (
                    SELECT ID FROM [RevitElementsParameters ].dbo.Project_DB
                    WHERE Name = '""" + name + """'
                    ),
                    models AS
                    (
                    SELECT * FROM [RevitElementsParameters ].dbo.File_DB
                    WHERE Project_DB_ID = (SELECT ID FROM project)
                    )
                    SELECT 
                        Project_DB_ID AS Project_id
                        ,Id AS File_id
                       ,Name File_name 
                    FROM models"""
    return sql_query

def sql_query_db_elements(name):
    sql_query = """WITH project AS 
                        (
                        SELECT ID FROM [RevitElementsParameters ].dbo.Project_DB
                        WHERE Name = '""" + name + """'
                        ),
                        models AS
                        (
                        SELECT * FROM [RevitElementsParameters ].dbo.File_DB
                        WHERE Project_DB_ID IN (SELECT ID FROM project)
                        ),
                        elements AS
                        (
                        SELECT * FROM [RevitElementsParameters ].dbo.ElementDtoInstance
                        WHERE TypeId <> -1 AND FileDbId IN (SELECT Id FROM models)
                        ),
                        category_and_type AS 
                        (
                        SELECT * FROM [RevitElementsParameters ].dbo.ElementDtoType
                        WHERE FileDbId IN (SELECT Id FROM models) 
                        ),
                        family AS 
                        (
                        SELECT * FROM [RevitElementsParameters ].dbo.FamilyDto
                        WHERE FileDbId IN (SELECT Id FROM models) 
                        )

                        SELECT
                            e.ElementIdDto AS Element_id
                            ,e.FileDbId AS File_id
                            ,e.CategoryId AS Category_id
                            ,ct.Name AS Type_name
                            ,ct.FamilyName AS Family_name
                            ,f.FM_FamilyId AS Family_id
                            ,f.FM_FamilyVersionId AS Family_version_id
                            ,CONCAT(e.FileDbId, '_', e.ElementIdDto) AS Unique_keys
                        FROM elements e
                        LEFT JOIN category_and_type ct
                        ON e.TypeId = ct.ElementIdDto AND e.FileDbId = ct.FileDbId
                        LEFT JOIN family f
                        ON e.RevitFamilyId = f.RevitFamilyId AND e.FileDbId = f.FileDbId"""
    return sql_query

def sql_query_db_values(name):
    sql_query = """WITH project AS 
                    (
                    SELECT ID FROM [RevitElementsParameters ].dbo.Project_DB
                    WHERE Name = '""" + name + """'
                    ),
                    models AS
                    (
                    SELECT * FROM [RevitElementsParameters ].dbo.File_DB
                    WHERE Project_DB_ID IN (SELECT ID FROM project)
                    ),
                    elements AS
                    (
                    SELECT * FROM [RevitElementsParameters ].dbo.ElementDtoInstance
                    WHERE TypeId <> -1 AND FileDbId IN (SELECT Id FROM models)
                    ),
                    value_type AS 
                    (
                    SELECT * FROM [RevitElementsParameters ].dbo.ParameterValueDtoType pvdt
                    WHERE pvdt.FileDbId IN (SELECT Id FROM models)
                    ),
                    value_type_elemets AS (
                    SELECT
                        e.ElementIdDto as ElementId
                        ,vt.ParameterId
                        ,vt.FileDbId
                        ,vt.ValueInt
                        ,vt.ValueStr
                        ,vt.ValueDouble
                        ,vt.DataValueType
                        ,vt.AsValueStr
                    FROM value_type vt
                    RIGHT JOIN elements e 
                    ON vt.ElementId = e.TypeId  AND vt.FileDbId = e.FileDbId
                    ),
                    value_instance AS 
                    (
                    SELECT * FROM [RevitElementsParameters ].dbo.ParameterValueDtoInstance 
                    WHERE FileDbId IN (SELECT Id FROM models)
                    ),
                    all_values AS (
                    SELECT * FROM value_instance
                    UNION ALL
                    SELECT * FROM value_type_elemets
                    )
                    SELECT
                        ParameterId AS Parameter_id
                        ,CASE
                            WHEN ParameterId = -1002053 THEN COALESCE(ValueStr, AsValueStr, ValueStr)
                            WHEN DataValueType > 0 THEN FORMAT(COALESCE(ValueDouble, ValueInt, ValueDouble), '0.00#####')
                            ELSE COALESCE(ValueStr, AsValueStr, ValueStr)
                            END AS Value
                        ,CONCAT(FileDbId, '_', ElementId) AS Unique_keys
                    FROM all_values"""
    return sql_query

def sql_query_db_parametrs(name):
    sql_query = """WITH 
                project AS 
                (
                SELECT ID FROM [RevitElementsParameters ].dbo.Project_DB
                WHERE Name = '""" + name + """'
                ),
                models AS
                (
                SELECT * FROM [RevitElementsParameters ].dbo.File_DB
                WHERE Project_DB_ID IN (SELECT ID FROM project)
                ),
                parameters AS 
                (
                SELECT * FROM [RevitElementsParameters ].dbo.ParametersDto
                WHERE FileDbId IN (SELECT Id FROM models)
                )
                SELECT 
                    ParameterId AS Parameter_id
                    ,FileDbId AS File_id
                    ,Name AS Parameter_name
                    ,FromShared AS From_shared
                    ,Guid
                FROM parameters"""
    return sql_query

def sql_query_db_categories():
    sql_query = """SELECT
                    BuiltInCategory AS Built_in_сategory,
                    BuiltInCategoryId AS Category_id,
                    Name AS Category_name,
                    CategoryType AS Category_type,
                    ReassignedCategory AS Reassigned_category,
                    ParentId AS Parent_id,
                    Upload
                    FROM [RevitElementsParameters ].dbo.Categories"""
    return sql_query

def sql_query_family_manager():
    sql_query = """SELECT DISTINCT 
                    family_id AS 'Id семейства'
                    ,family_version_id AS 'Id версии'
                    ,family_name AS 'Имя семейства'
                    ,family_version_num AS 'Номер версии'
                    ,version_status_name AS 'Статус'
                    ,functional_type_name AS 'Функциональный Тип'
                    ,functional_subtype_name AS 'Функциональный Подтип'
                FROM [RevitElementsParameters ].dbo.Family_manager"""
    return sql_query