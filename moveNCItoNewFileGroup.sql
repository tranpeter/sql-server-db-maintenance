--
-- The MIT License (MIT) 
-- Copyright (c) <2016> <tranpeter at yahoo dot com>
--
-- Permission is hereby granted, free of charge, to any person obtaining 
-- a copy of this software and associated documentation files (the "Software"), 
-- to deal in the Software without restriction, including without limitation 
-- the rights to use, copy, modify, merge, publish, distribute, sublicense, 
-- and/or sell copies of the Software, and to permit persons to whom the Software 
-- is furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included 
-- in all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
-- INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A 
-- PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
-- HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF 
-- CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE 
-- OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
--

--
-- This script will will move all the NONCLUSTERED indexes in a SQL Server 
-- database from the old filegroup to the new filegroup. 
-- 
-- Handles: UNIQUE vs. NON-UNIQUE
-- Handles: INCLUDE columns.
--
-- TODO: Handle XML indexes
-- TODO: Handle UNIQUE CLUSTERED indexes that are NOT PK
--
-- NOTE, This script does not handle DESCENDING ORDER for index COLUMN.
--
-- This script assumes it will be executed during a maintenace period and will utilize
-- all threads available on the server to move the index via MAXDOP = 0.
--
-- Please monitor transaction log growth and size as appropriate.
--
-- 20161123 - Tested on SQL Server 2012 and SQL Server 2016
--

DECLARE
	@newFileGroup NVARCHAR(128),
	@oldFileGroup NVARCHAR(128);
BEGIN
	--
	-- REQUIRED: Specify the old and new filegroup name
	--
	SET @oldFileGroup = N'PRIMARY';
	SET @newFileGroup = N'PROS_DEFAULT';

	DECLARE @schemaName NVARCHAR(128);
	DECLARE @tableName  NVARCHAR(128);
	DECLARE @indexName  NVARCHAR(128);
	DECLARE @isUnique   BIT;

    DECLARE index_name_cur CURSOR FAST_FORWARD FOR
		--
		-- Query to get schema, table, index name, and whether index is unqiue
		-- for all non-Primary Key indexes not including
		-- any table containing any XML indexes.
		-- TODO: Need to handle XML indexes
		--
		SELECT s.name AS SCHEMA_NAME,
			   OBJECT_NAME(tb.object_id) AS TABLE_NAME,
			   ind.name AS COLUMN_NAME,
			   ind.is_unique as IS_UNIQUE_INDEX
	    FROM sys.indexes ind INNER JOIN sys.tables tb
				ON ind.object_id = tb.object_id 
			 INNER JOIN sys.schemas s 
				ON s.schema_id = tb.schema_id
			 INNER JOIN sys.filegroups fg
				ON ind.data_space_id = fg.data_space_id
		WHERE is_primary_key = 0  
		  AND ind.type = 2			-- NONCLUSTERED
		  AND fg.name = @oldFileGroup
		  AND tb.object_id NOT IN (SELECT xml.object_id FROM sys.xml_indexes xml)
		  --AND tb.name = 'ProductReview' -- For testing a single table
		ORDER BY s.name, tb.object_id, ind.name;
    OPEN index_name_cur;
    FETCH NEXT FROM index_name_cur INTO @schemaName, @tableName, @indexName, @isUnique;
    WHILE (@@fetch_status = 0)
    BEGIN
        -- PRINT @tableName + ' ' + @indexName;
		DECLARE @cmd NVARCHAR(512);
		DECLARE @cnt int;					-- Used to handle multi-column PK indexes
		DECLARE @isFirstIncludedColumn BIT; -- Used to handle include columns

		SET @cmd = 'CREATE ';
		IF (@isUnique = 1)
			BEGIN
				SET @cmd = @cmd + 'UNIQUE ';
			END
		SET @cmd = @cmd + 'NONCLUSTERED INDEX ' + @indexName +
				   ' ON ' + @schemaName + '.' + @tableName + '(';
		SET @cnt = 0;
		SET @isFirstIncludedColumn = 0;

		DECLARE @columnName NVARCHAR(128);
		DECLARE @isIncludedColumn BIT;
		DECLARE index_column_cur CURSOR FAST_FORWARD FOR
			--
			-- Query to get the column name(s) for a given index in key order
			-- IMPORTANT: Do not change the ORDER BY columns.
			--
			SELECT c.name AS COLUMN_NAME, is_included_column AS INC_COLUMN
			FROM sys.index_columns ic 
				 INNER JOIN
				 sys.columns c ON 
					ic.object_id = c.object_id AND ic.column_id = c.column_id
				 INNER JOIN 
				 sys.indexes ind ON
					ind.object_id = ic.object_id AND ind.index_id = ic.index_id
			WHERE ind.is_primary_key = 0
			  AND ind.type = 2 -- NONCLUSTERED
		  	  AND ind.name = @indexName
			ORDER BY ind.object_id, ind.name, ic.is_included_column, ic.key_ordinal;
		OPEN index_column_cur;
		FETCH NEXT FROM index_column_cur INTO @columnName, @isIncludedColumn;
		WHILE (@@fetch_status = 0)
		BEGIN
			IF (@isIncludedColumn = 1 AND @isFirstIncludedColumn = 0)
				BEGIN
					-- If this is the first include column in the set then,
					-- close the index columns and add the INCLUDE keyword
					SET @cmd = @cmd + ') INCLUDE (' + @columnName;
					SET @isFirstIncludedColumn = 1;
				END;
			ELSE IF (@cnt > 0)
				BEGIN
					-- Handle multi-columns in index or INCLUDE
					SET @cmd = @cmd + ', '
					SET @cmd = @cmd + @columnName;
				END;
			ELSE
				BEGIN
					SET @cmd = @cmd + @columnName;
				END;
			SET @cnt = @cnt + 1;

			FETCH NEXT FROM index_column_cur INTO @columnName, @isIncludedColumn;
		END;
		CLOSE index_column_cur;
		DEALLOCATE index_column_cur;

		--
		-- NOTE, this script assumes that it will be executed during a mainteance
		-- winddow therefore, it sets MAXDOP = 0 to use all threads available.
		--
		SET @cmd = @cmd + ') WITH (MAXDOP = 0, DROP_EXISTING = ON) ON [' + @newFileGroup + ']'

		PRINT @cmd;
		EXEC (@cmd);

        FETCH NEXT FROM index_name_cur INTO @schemaName, @tableName, @indexName, @isUnique;
    END;
    CLOSE index_name_cur;
    DEALLOCATE index_name_cur;
END;

