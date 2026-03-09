DECLARE @sep NVARCHAR(10) = N', ' + CHAR(13);
 
WITH real_cols AS (
    SELECT
        t.object_id,
        s.name AS SchemaName,
        t.name AS TableName,
        c.column_id,
        c.name AS ColumnName,
        ty.name AS DataType,
 
        CASE
            WHEN ty.name IN ('char','varchar','nchar','nvarchar','binary','varbinary')
                THEN '(' +
                       CASE
                          WHEN c.max_length = -1 THEN 'MAX'
                          WHEN ty.name LIKE 'n%' THEN CAST(c.max_length/2 AS VARCHAR(10))
                          ELSE CAST(c.max_length AS VARCHAR(10))
                       END +
                     ')'
            WHEN ty.name IN ('decimal','numeric')
                THEN '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
            ELSE ''
        END AS TypeParams,
 
        CASE WHEN c.is_nullable = 1 THEN 'NULL' ELSE 'NOT NULL' END AS Nullability
 
    FROM sys.tables t
    JOIN sys.schemas s  ON s.schema_id = t.schema_id
    JOIN sys.columns c  ON c.object_id = t.object_id
    JOIN sys.types ty   ON ty.user_type_id = c.user_type_id  -- ← este é o tipo REAL definido
 
    WHERE ty.name <> 'sysname'                -- ← IGNORA COLUNAS FANTASMA
)
 
, col_agg AS (
    SELECT
        object_id,
        SchemaName,
        TableName,
        STRING_AGG(
            CAST(
                QUOTENAME(ColumnName) + ' ' + DataType + TypeParams + ' ' + Nullability
            AS NVARCHAR(MAX)),
            @sep
        ) WITHIN GROUP (ORDER BY column_id) AS ColumnsText
    FROM real_cols
    GROUP BY object_id, SchemaName, TableName
)
 
, dist AS (
    SELECT
        t.object_id,
        tdp.distribution_policy_desc AS DistributionType
    FROM sys.tables t
    JOIN sys.pdw_table_distribution_properties tdp
      ON t.object_id = tdp.object_id
)
 
, hashcols AS (
    SELECT
        cdp.object_id,
        STRING_AGG(QUOTENAME(c.name), ', ') AS HashCols
    FROM sys.pdw_column_distribution_properties cdp
    JOIN sys.columns c
      ON c.object_id = cdp.object_id
     AND c.column_id = cdp.column_id
    WHERE cdp.distribution_ordinal > 0
    GROUP BY cdp.object_id
)
 
, storage AS (
    SELECT
        t.object_id,
        CASE WHEN i.type = 5 THEN 'CLUSTERED COLUMNSTORE INDEX' ELSE 'HEAP' END AS StorageType
    FROM sys.tables t
    JOIN sys.indexes i ON t.object_id = i.object_id
    WHERE i.index_id < 2
)
 
SELECT
    c.SchemaName,
    c.TableName,
    CAST(
        'CREATE TABLE '
        + QUOTENAME(c.SchemaName) + '.' + QUOTENAME(c.TableName)
        + CHAR(13) + '(' + CHAR(13)
        + c.ColumnsText + CHAR(13)
        + ') WITH ( DISTRIBUTION = '
        + CASE
             WHEN d.DistributionType = 'HASH'
                 THEN 'HASH(' + ISNULL(h.HashCols, '') + ')'
             ELSE d.DistributionType
          END
        + ', ' + s.StorageType
        + ' );'
    AS NVARCHAR(MAX)) AS CreateTableScript
 
FROM col_agg c
LEFT JOIN dist d    ON d.object_id = c.object_id
LEFT JOIN hashcols h ON h.object_id = c.object_id
LEFT JOIN storage s ON s.object_id = c.object_id
ORDER BY c.SchemaName, c.TableName;
