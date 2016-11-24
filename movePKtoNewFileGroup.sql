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
-- This script will will move all the PRIMARY KEY (PK) indexes in a SQL Server database 
-- from the old filegroup to the new filegroup.
--
-- Primary Keys are UNQIUE and CLUSTERED by default. This syntax allows us to move
-- the PK to the new filegroup without having to mess with the primary constraint
-- via ALTER TABLE.
--
-- This script assumes it will be executed during a maintenace period and will utilize
-- all threads available on the server to move the index via MAXDOP = 0.
--
-- Please monitor transaction log growth and size as appropriate.
--
-- 20161121 - Tested on SQL Server 2012 and SQL Serve 2016
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

    DECLARE index_name_cur CURSOR FAST_FORWARD FOR
		--
		-- Query to get schema, table, and index name for all PK indexes not including
		-- any table containing any XML indexes.
		-- TODO: Need to handle XML indexes
		--
		SELECT s.name AS SCHEMA_NAME,
			   OBJECT_NAME(tb.object_id) AS TABLE_NAME,
			   ind.name AS COLUMN_NAME
	    FROM sys.indexes ind INNER JOIN sys.tables tb
				ON ind.object_id = tb.object_id 
			 INNER JOIN sys.schemas s 
				ON s.schema_id = tb.schema_id
			 INNER JOIN sys.filegroups fg
				ON ind.data_space_id = fg.data_space_id
		WHERE is_primary_key = 1  
		  AND ind.type_desc = 'CLUSTERED'
		  AND tb.object_id NOT IN (SELECT xml.object_id FROM sys.xml_indexes xml)
		  AND fg.name = @oldFileGroup
		ORDER BY s.name, tb.object_id, ind.name;
    OPEN index_name_cur;
    FETCH NEXT FROM index_name_cur INTO @schemaName, @tableName, @indexName;
    WHILE (@@fetch_status = 0)
    BEGIN
        -- PRINT @tableName + ' ' + @indexName;
		DECLARE @cmd NVARCHAR(512);
		DECLARE @cnt int; -- Used to handle multi-column PK indexes

		SET @cmd = 'CREATE UNIQUE CLUSTERED INDEX ' + @indexName +
				   ' ON ' + @schemaName + '.' + @tableName + '(';
		SET @cnt = 0;

		DECLARE @columnName NVARCHAR(128);
		DECLARE index_column_cur CURSOR FAST_FORWARD FOR
			--
			-- Query to get the column name(s) for a given index in key order
			--
			SELECT c.name AS COLUMN_NAME
			FROM sys.index_columns ic INNER JOIN
				 sys.columns c ON 
					ic.object_id = c.object_id AND ic.column_id = c.column_id
				 INNER JOIN 
				 sys.indexes ind ON
				 ind.object_id = ic.object_id AND ind.index_id = ic.index_id
			WHERE ind.is_primary_key = 1 
			  AND ind.type_desc = 'CLUSTERED'
			  AND ind.name = @indexName
			ORDER BY ind.object_id, ind.name, ic.key_ordinal;
		OPEN index_column_cur;
		FETCH NEXT FROM index_column_cur INTO @columnName;
		WHILE (@@fetch_status = 0)
		BEGIN
			-- PRINT '          ' + @columnName;
			IF (@cnt > 0)
				SET @cmd = @cmd + ', '
			SET @cmd = @cmd + @columnName;
			SET @cnt = @cnt + 1;

			FETCH NEXT FROM index_column_cur INTO @columnName;
		END;
		CLOSE index_column_cur;
		DEALLOCATE index_column_cur;

		--
		-- NOTE, this script assumes that it will be executed during a mainteance
		-- winddow therefore, it sets MAXDOP = 0 to use all threads available.
		--
		SET @cmd = @cmd + ') WITH (MAXDOP = 0, DROP_EXISTING = ON) ON [' + @newFileGroup + ']'

		PRINT @cmd;
		--EXEC (@cmd);

        FETCH NEXT FROM index_name_cur INTO @schemaName, @tableName, @indexName;
    END;
    CLOSE index_name_cur;
    DEALLOCATE index_name_cur;
END;

