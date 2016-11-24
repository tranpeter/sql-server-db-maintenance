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
-- This script will will move all the heap tables in a SQL Server 
-- database from the old filegroup to the new filegroup. 
--
-- This script assumes it will be executed during a maintenace period and will utilize
-- all threads available on the server to move the index via MAXDOP = 0.
--
-- To move a heap table, we create a clustered index on a single column of the
-- heap table in the new filegroup. We then drop the index.
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

    DECLARE index_name_cur CURSOR FAST_FORWARD FOR
		--
		-- Query to get schema name, table name for all heap tables not including
		-- any table containing any XML indexes.
		-- TODO: Need to handle XML indexes
		SELECT s.name AS SCHEMA_NAME,
			   OBJECT_NAME(tb.object_id) AS TABLE_NAME
	    FROM sys.objects o INNER JOIN sys.tables tb 
				ON o.object_id = tb.object_id 
			 INNER JOIN sys.schemas s 
				ON s.schema_id = tb.schema_id 
			 INNER JOIN sys.partitions p 
				ON p.object_id = o.object_id
			 INNER JOIN sys.indexes ind
			    ON ind.object_id = tb.object_id
			 INNER JOIN sys.filegroups fg
				ON ind.data_space_id = fg.data_space_id
		WHERE o.type = 'U'     -- USER TABLES
		  AND p.index_id = 0   -- HEAP 
		  AND tb.object_id NOT IN (SELECT xml.object_id FROM sys.xml_indexes xml)
		  AND fg.name = @oldFileGroup
		  AND fg.type = N'FG'
		  -- AND tb.name = 'heap1' -- For testing a single table
		ORDER BY s.name, tb.object_id;
    OPEN index_name_cur;
    FETCH NEXT FROM index_name_cur INTO @schemaName, @tableName
    WHILE (@@fetch_status = 0)
    BEGIN
		DECLARE @cmdCreate NVARCHAR(512);
		DECLARE @cmdDrop NVARCHAR(512);

		SET @cmdCreate = 'CREATE CLUSTERED INDEX ' + @tableName + '_TEMP_CI' +
				   ' ON ' + @schemaName + '.' + @tableName + '(';
		SET @cmdDrop = 'DROP INDEX ' + @tableName + '_TEMP_CI' +
				   ' ON ' + @schemaName + '.' + @tableName;

		DECLARE @columnName NVARCHAR(128);
		DECLARE table_column_cur CURSOR FAST_FORWARD FOR
			--
			-- Query to get a single column. We don't need more than a
			-- single column to create the index since it's not unique.
			--
			SELECT TOP 1 c.name AS COLUMN_NAME
			FROM sys.columns c INNER JOIN sys.tables t ON
					t.object_id = c.object_id
			WHERE t.name = @tableName;
		OPEN table_column_cur;
		FETCH NEXT FROM table_column_cur INTO @columnName;
		WHILE (@@fetch_status = 0)
		BEGIN
			SET @cmdCreate = @cmdCreate + @columnName;

			FETCH NEXT FROM table_column_cur INTO @columnName;
		END;
		CLOSE table_column_cur;
		DEALLOCATE table_column_cur;

		--
		-- NOTE, this script assumes that it will be executed during a mainteance
		-- winddow therefore, it sets MAXDOP = 0 to use all threads available.
		--
		SET @cmdCreate = @cmdCreate + ') WITH (MAXDOP = 0) ON [' + @newFileGroup + ']';

		PRINT @cmdCreate;
		PRINT @cmdDrop;
		EXEC (@cmdCreate);
		EXEC (@cmdDrop);

        FETCH NEXT FROM index_name_cur INTO @schemaName, @tableName;
    END;
    CLOSE index_name_cur;
    DEALLOCATE index_name_cur;
END;

