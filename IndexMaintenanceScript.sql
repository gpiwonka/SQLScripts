-- =============================================
-- Advanced Index Maintenance Script for SQL Server
-- Analyzes fragmentation and performs appropriate maintenance
-- Supports both tables and indexed views with email reporting
-- Multi-database support with flexible scope options
-- =============================================
 
SET NOCOUNT ON;
 
-- =============================================
-- CONFIGURATION SECTION
-- =============================================

-- Fragmentation Thresholds
DECLARE @FragmentationThresholdReorganize FLOAT = 10.0;  -- From 10% fragmentation: Reorganize
DECLARE @FragmentationThresholdRebuild FLOAT = 30.0;     -- From 30% fragmentation: Rebuild
DECLARE @MinPageCount INT = 1000;                        -- Minimum page count for maintenance

-- Execution Options
DECLARE @ExecuteCommands BIT = 1;                        -- 1 = Execute, 0 = Analysis only
DECLARE @IncludeViews BIT = 1;                          -- 1 = Include views, 0 = Tables only

-- Database Scope Options
DECLARE @DatabaseScope NVARCHAR(20) = 'CURRENT';        -- 'CURRENT', 'ALL_USER', 'SPECIFIC'
DECLARE @SpecificDatabase NVARCHAR(128) = '';           -- Only used when @DatabaseScope = 'SPECIFIC'

-- Email Configuration
DECLARE @SendEmail BIT = 1;                             -- 1 = Send email report, 0 = No email
DECLARE @EmailProfile NVARCHAR(128) = 'Default';        -- Database Mail profile name
DECLARE @EmailRecipients NVARCHAR(MAX) = 'dba@company.com;admin@company.com'; -- Semicolon separated
DECLARE @EmailSubjectPrefix NVARCHAR(100) = '[SQL Server]'; -- Subject prefix

-- =============================================
-- VARIABLES AND TEMP TABLES
-- =============================================

DECLARE @DatabaseList TABLE (
    DatabaseName NVARCHAR(128),
    DatabaseID INT
);

DECLARE @CurrentDatabase NVARCHAR(128);
DECLARE @CurrentDatabaseID INT;
DECLARE @SQL NVARCHAR(MAX);
DECLARE @EmailSubject NVARCHAR(255);
DECLARE @EmailBody NVARCHAR(MAX) = '';
DECLARE @ReportSummary NVARCHAR(MAX) = '';

-- Global counters for email report
DECLARE @TotalDatabases INT = 0;
DECLARE @TotalIndexesAnalyzed INT = 0;
DECLARE @TotalIndexesRebuilt INT = 0;
DECLARE @TotalIndexesReorganized INT = 0;
DECLARE @TotalErrors INT = 0;

-- Temporary table for index information (global scope)
IF OBJECT_ID('tempdb..#IndexStats') IS NOT NULL
    DROP TABLE #IndexStats;
 
CREATE TABLE #IndexStats (
    DatabaseName NVARCHAR(128),
    SchemaName NVARCHAR(128),
    ObjectName NVARCHAR(128),
    ObjectType NVARCHAR(10),
    IndexName NVARCHAR(128),
    IndexType NVARCHAR(60),
    FragmentationPercent FLOAT,
    PageCount BIGINT,
    RecommendedAction NVARCHAR(50),
    MaintenanceCommand NVARCHAR(MAX),
    ExecutionStatus NVARCHAR(50) DEFAULT 'PENDING'
);

-- =============================================
-- BUILD DATABASE LIST BASED ON SCOPE
-- =============================================

IF @DatabaseScope = 'CURRENT'
BEGIN
    INSERT INTO @DatabaseList (DatabaseName, DatabaseID)
    VALUES (DB_NAME(), DB_ID());
END
ELSE IF @DatabaseScope = 'ALL_USER'
BEGIN
    INSERT INTO @DatabaseList (DatabaseName, DatabaseID)
    SELECT name, database_id 
    FROM sys.databases 
    WHERE database_id > 4  -- Exclude system databases
        AND state = 0      -- Online databases only
        AND is_read_only = 0; -- Exclude read-only databases
END
ELSE IF @DatabaseScope = 'SPECIFIC'
BEGIN
    INSERT INTO @DatabaseList (DatabaseName, DatabaseID)
    SELECT name, database_id 
    FROM sys.databases 
    WHERE name = @SpecificDatabase
        AND state = 0
        AND is_read_only = 0;
    
    IF @@ROWCOUNT = 0
    BEGIN
        PRINT 'ERROR: Database ''' + @SpecificDatabase + ''' not found or not accessible!';
        RETURN;
    END
END

SELECT @TotalDatabases = COUNT(*) FROM @DatabaseList;

-- =============================================
-- MAIN PROCESSING LOOP
-- =============================================

PRINT '=============================================';
PRINT 'ADVANCED INDEX MAINTENANCE ANALYSIS';
PRINT '=============================================';
PRINT '';
PRINT 'Configuration:';
PRINT 'Database Scope: ' + @DatabaseScope + CASE WHEN @DatabaseScope = 'SPECIFIC' THEN ' (' + @SpecificDatabase + ')' ELSE '' END;
PRINT 'Databases to process: ' + CAST(@TotalDatabases AS VARCHAR(10));
PRINT 'Reorganize from: ' + CAST(@FragmentationThresholdReorganize AS VARCHAR(10)) + '% fragmentation';
PRINT 'Rebuild from: ' + CAST(@FragmentationThresholdRebuild AS VARCHAR(10)) + '% fragmentation';
PRINT 'Minimum page count: ' + CAST(@MinPageCount AS VARCHAR(10));
PRINT 'Include indexed views: ' + CASE WHEN @IncludeViews = 1 THEN 'YES' ELSE 'NO' END;
PRINT 'Execute commands: ' + CASE WHEN @ExecuteCommands = 1 THEN 'YES' ELSE 'NO (analysis only)' END;
PRINT 'Send email report: ' + CASE WHEN @SendEmail = 1 THEN 'YES' ELSE 'NO' END;
PRINT '';

-- Initialize email body
SET @EmailBody = @EmailBody + 'Index Maintenance Report - ' + CONVERT(NVARCHAR(19), GETDATE(), 120) + CHAR(13) + CHAR(10);
SET @EmailBody = @EmailBody + '=====================================================' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10);
SET @EmailBody = @EmailBody + 'Configuration:' + CHAR(13) + CHAR(10);
SET @EmailBody = @EmailBody + 'Database Scope: ' + @DatabaseScope + CASE WHEN @DatabaseScope = 'SPECIFIC' THEN ' (' + @SpecificDatabase + ')' ELSE '' END + CHAR(13) + CHAR(10);
SET @EmailBody = @EmailBody + 'Databases processed: ' + CAST(@TotalDatabases AS VARCHAR(10)) + CHAR(13) + CHAR(10);
SET @EmailBody = @EmailBody + 'Execution mode: ' + CASE WHEN @ExecuteCommands = 1 THEN 'EXECUTE' ELSE 'ANALYSIS ONLY' END + CHAR(13) + CHAR(10);
SET @EmailBody = @EmailBody + CHAR(13) + CHAR(10);

-- Process each database
DECLARE db_cursor CURSOR FOR
SELECT DatabaseName, DatabaseID FROM @DatabaseList ORDER BY DatabaseName;

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @CurrentDatabase, @CurrentDatabaseID;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '=============================================';
    PRINT 'PROCESSING DATABASE: ' + @CurrentDatabase;
    PRINT '=============================================';
    
    SET @EmailBody = @EmailBody + 'Database: ' + @CurrentDatabase + CHAR(13) + CHAR(10);
    SET @EmailBody = @EmailBody + '----------------------------------------' + CHAR(13) + CHAR(10);
    
    -- Collect index statistics for tables
    SET @SQL = '
    USE [' + @CurrentDatabase + '];
    INSERT INTO #IndexStats (
        DatabaseName, SchemaName, ObjectName, ObjectType, IndexName, IndexType, 
        FragmentationPercent, PageCount, RecommendedAction, MaintenanceCommand
    )
    SELECT 
        ''' + @CurrentDatabase + ''',
        s.name AS SchemaName,
        t.name AS ObjectName,
        ''TABLE'' AS ObjectType,
        i.name AS IndexName,
        i.type_desc AS IndexType,
        ips.avg_fragmentation_in_percent AS FragmentationPercent,
        ips.page_count AS PageCount,
        CASE 
            WHEN ips.avg_fragmentation_in_percent >= ' + CAST(@FragmentationThresholdRebuild AS VARCHAR(10)) + ' AND ips.page_count >= ' + CAST(@MinPageCount AS VARCHAR(10)) + ' 
                THEN ''REBUILD''
            WHEN ips.avg_fragmentation_in_percent >= ' + CAST(@FragmentationThresholdReorganize AS VARCHAR(10)) + ' AND ips.page_count >= ' + CAST(@MinPageCount AS VARCHAR(10)) + ' 
                THEN ''REORGANIZE''
            ELSE ''NO ACTION''
        END AS RecommendedAction,
        CASE 
            WHEN ips.avg_fragmentation_in_percent >= ' + CAST(@FragmentationThresholdRebuild AS VARCHAR(10)) + ' AND ips.page_count >= ' + CAST(@MinPageCount AS VARCHAR(10)) + ' 
                THEN ''ALTER INDEX ['' + i.name + ''] ON ['' + s.name + ''].['' + t.name + ''] REBUILD WITH (ONLINE = OFF, FILLFACTOR = 90);''
            WHEN ips.avg_fragmentation_in_percent >= ' + CAST(@FragmentationThresholdReorganize AS VARCHAR(10)) + ' AND ips.page_count >= ' + CAST(@MinPageCount AS VARCHAR(10)) + ' 
                THEN ''ALTER INDEX ['' + i.name + ''] ON ['' + s.name + ''].['' + t.name + ''] REORGANIZE;''
            ELSE NULL
        END AS MaintenanceCommand
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, ''DETAILED'') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE i.name IS NOT NULL
        AND i.is_disabled = 0
        AND ips.page_count > 0;';
    
    EXEC sp_executesql @SQL;
    
    -- Collect index statistics for views (if enabled)
    IF @IncludeViews = 1
    BEGIN
        SET @SQL = '
        USE [' + @CurrentDatabase + '];
        INSERT INTO #IndexStats (
            DatabaseName, SchemaName, ObjectName, ObjectType, IndexName, IndexType, 
            FragmentationPercent, PageCount, RecommendedAction, MaintenanceCommand
        )
        SELECT 
            ''' + @CurrentDatabase + ''',
            s.name AS SchemaName,
            v.name AS ObjectName,
            ''VIEW'' AS ObjectType,
            i.name AS IndexName,
            i.type_desc AS IndexType,
            ips.avg_fragmentation_in_percent AS FragmentationPercent,
            ips.page_count AS PageCount,
            CASE 
                WHEN ips.avg_fragmentation_in_percent >= ' + CAST(@FragmentationThresholdRebuild AS VARCHAR(10)) + ' AND ips.page_count >= ' + CAST(@MinPageCount AS VARCHAR(10)) + ' 
                    THEN ''REBUILD''
                WHEN ips.avg_fragmentation_in_percent >= ' + CAST(@FragmentationThresholdReorganize AS VARCHAR(10)) + ' AND ips.page_count >= ' + CAST(@MinPageCount AS VARCHAR(10)) + ' 
                    THEN ''REORGANIZE''
                ELSE ''NO ACTION''
            END AS RecommendedAction,
            CASE 
                WHEN ips.avg_fragmentation_in_percent >= ' + CAST(@FragmentationThresholdRebuild AS VARCHAR(10)) + ' AND ips.page_count >= ' + CAST(@MinPageCount AS VARCHAR(10)) + ' 
                    THEN ''ALTER INDEX ['' + i.name + ''] ON ['' + s.name + ''].['' + v.name + ''] REBUILD WITH (ONLINE = OFF, FILLFACTOR = 90);''
                WHEN ips.avg_fragmentation_in_percent >= ' + CAST(@FragmentationThresholdReorganize AS VARCHAR(10)) + ' AND ips.page_count >= ' + CAST(@MinPageCount AS VARCHAR(10)) + ' 
                    THEN ''ALTER INDEX ['' + i.name + ''] ON ['' + s.name + ''].['' + v.name + ''] REORGANIZE;''
                ELSE NULL
            END AS MaintenanceCommand
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, ''DETAILED'') ips
        INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
        INNER JOIN sys.views v ON i.object_id = v.object_id
        INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE i.name IS NOT NULL
            AND i.is_disabled = 0
            AND ips.page_count > 0;';
        
        EXEC sp_executesql @SQL;
    END
    
    -- Display statistics for current database
    DECLARE @DBIndexCount INT, @DBRebuilds INT, @DBReorganizes INT;
    
    SELECT 
        @DBIndexCount = COUNT(*),
        @DBRebuilds = SUM(CASE WHEN RecommendedAction = 'REBUILD' THEN 1 ELSE 0 END),
        @DBReorganizes = SUM(CASE WHEN RecommendedAction = 'REORGANIZE' THEN 1 ELSE 0 END)
    FROM #IndexStats 
    WHERE DatabaseName = @CurrentDatabase;
    
    PRINT 'Indexes analyzed: ' + CAST(@DBIndexCount AS VARCHAR(10));
    PRINT 'Rebuilds needed: ' + CAST(@DBRebuilds AS VARCHAR(10));
    PRINT 'Reorganizations needed: ' + CAST(@DBReorganizes AS VARCHAR(10));
    
    SET @EmailBody = @EmailBody + 'Indexes analyzed: ' + CAST(@DBIndexCount AS VARCHAR(10)) + CHAR(13) + CHAR(10);
    SET @EmailBody = @EmailBody + 'Rebuilds needed: ' + CAST(@DBRebuilds AS VARCHAR(10)) + CHAR(13) + CHAR(10);
    SET @EmailBody = @EmailBody + 'Reorganizations needed: ' + CAST(@DBReorganizes AS VARCHAR(10)) + CHAR(13) + CHAR(10);
    SET @EmailBody = @EmailBody + CHAR(13) + CHAR(10);
    
    -- Update global counters
    SET @TotalIndexesAnalyzed = @TotalIndexesAnalyzed + @DBIndexCount;
    
    FETCH NEXT FROM db_cursor INTO @CurrentDatabase, @CurrentDatabaseID;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;

-- =============================================
-- EXECUTE MAINTENANCE COMMANDS
-- =============================================

DECLARE @MaintenanceCommands INT;
SELECT @MaintenanceCommands = COUNT(*) FROM #IndexStats WHERE MaintenanceCommand IS NOT NULL;

IF @MaintenanceCommands > 0
BEGIN
    PRINT '';
    PRINT '=============================================';
    PRINT 'EXECUTING MAINTENANCE COMMANDS';
    PRINT '=============================================';
    PRINT 'Total commands: ' + CAST(@MaintenanceCommands AS VARCHAR(10));
    
    SET @EmailBody = @EmailBody + 'Maintenance Execution:' + CHAR(13) + CHAR(10);
    SET @EmailBody = @EmailBody + '=====================' + CHAR(13) + CHAR(10);
    
    IF @ExecuteCommands = 1
    BEGIN
        DECLARE @CurrentCommand NVARCHAR(MAX);
        DECLARE @CurrentDB NVARCHAR(128);
        DECLARE @CurrentAction NVARCHAR(50);
        DECLARE @Counter INT = 0;
        
        DECLARE maintenance_cursor CURSOR FOR
        SELECT DatabaseName, MaintenanceCommand, RecommendedAction
        FROM #IndexStats
        WHERE MaintenanceCommand IS NOT NULL
        ORDER BY DatabaseName, 
            CASE ObjectType WHEN 'TABLE' THEN 1 ELSE 2 END,
            CASE RecommendedAction WHEN 'REBUILD' THEN 1 WHEN 'REORGANIZE' THEN 2 END,
            FragmentationPercent DESC;
        
        OPEN maintenance_cursor;
        FETCH NEXT FROM maintenance_cursor INTO @CurrentDB, @CurrentCommand, @CurrentAction;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @Counter = @Counter + 1;
            PRINT 'Executing ' + CAST(@Counter AS VARCHAR(10)) + '/' + CAST(@MaintenanceCommands AS VARCHAR(10)) + ' on ' + @CurrentDB;
            
            SET @SQL = 'USE [' + @CurrentDB + ']; ' + @CurrentCommand;
            
            BEGIN TRY
                EXEC sp_executesql @SQL;
                
                UPDATE #IndexStats 
                SET ExecutionStatus = 'SUCCESS'
                WHERE DatabaseName = @CurrentDB AND MaintenanceCommand = @CurrentCommand;
                
                IF @CurrentAction = 'REBUILD'
                    SET @TotalIndexesRebuilt = @TotalIndexesRebuilt + 1;
                ELSE
                    SET @TotalIndexesReorganized = @TotalIndexesReorganized + 1;
                    
                PRINT '-- ✓ Success';
            END TRY
            BEGIN CATCH
                UPDATE #IndexStats 
                SET ExecutionStatus = 'ERROR: ' + ERROR_MESSAGE()
                WHERE DatabaseName = @CurrentDB AND MaintenanceCommand = @CurrentCommand;
                
                SET @TotalErrors = @TotalErrors + 1;
                PRINT '-- ✗ Error: ' + ERROR_MESSAGE();
            END CATCH
            
            FETCH NEXT FROM maintenance_cursor INTO @CurrentDB, @CurrentCommand, @CurrentAction;
        END
        
        CLOSE maintenance_cursor;
        DEALLOCATE maintenance_cursor;
        
        -- Update statistics for maintained objects
        PRINT '';
        PRINT 'Updating statistics...';
        
        DECLARE stats_cursor CURSOR FOR
        SELECT DISTINCT DatabaseName, 'UPDATE STATISTICS [' + SchemaName + '].[' + ObjectName + '] WITH FULLSCAN;'
        FROM #IndexStats
        WHERE RecommendedAction IN ('REBUILD', 'REORGANIZE') AND ExecutionStatus = 'SUCCESS';
        
        OPEN stats_cursor;
        FETCH NEXT FROM stats_cursor INTO @CurrentDB, @CurrentCommand;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @SQL = 'USE [' + @CurrentDB + ']; ' + @CurrentCommand;
            
            BEGIN TRY
                EXEC sp_executesql @SQL;
            END TRY
            BEGIN CATCH
                PRINT '-- Warning: Statistics update failed for ' + @CurrentDB;
            END CATCH
            
            FETCH NEXT FROM stats_cursor INTO @CurrentDB, @CurrentCommand;
        END
        
        CLOSE stats_cursor;
        DEALLOCATE stats_cursor;
    END
    ELSE
    BEGIN
        PRINT 'Analysis mode - commands not executed';
        SET @EmailBody = @EmailBody + 'ANALYSIS MODE - Commands not executed' + CHAR(13) + CHAR(10);
    END
END
ELSE
BEGIN
    PRINT 'No maintenance required!';
    SET @EmailBody = @EmailBody + 'No maintenance required!' + CHAR(13) + CHAR(10);
END

-- =============================================
-- FINAL SUMMARY AND EMAIL REPORT
-- =============================================

PRINT '';
PRINT '=============================================';
PRINT 'FINAL SUMMARY';
PRINT '=============================================';
PRINT 'Databases processed: ' + CAST(@TotalDatabases AS VARCHAR(10));
PRINT 'Total indexes analyzed: ' + CAST(@TotalIndexesAnalyzed AS VARCHAR(10));
PRINT 'Indexes rebuilt: ' + CAST(@TotalIndexesRebuilt AS VARCHAR(10));
PRINT 'Indexes reorganized: ' + CAST(@TotalIndexesReorganized AS VARCHAR(10));
PRINT 'Errors encountered: ' + CAST(@TotalErrors AS VARCHAR(10));

-- Build email summary
SET @ReportSummary = 'Final Summary:' + CHAR(13) + CHAR(10);
SET @ReportSummary = @ReportSummary + '=============' + CHAR(13) + CHAR(10);
SET @ReportSummary = @ReportSummary + 'Databases processed: ' + CAST(@TotalDatabases AS VARCHAR(10)) + CHAR(13) + CHAR(10);
SET @ReportSummary = @ReportSummary + 'Total indexes analyzed: ' + CAST(@TotalIndexesAnalyzed AS VARCHAR(10)) + CHAR(13) + CHAR(10);
SET @ReportSummary = @ReportSummary + 'Indexes rebuilt: ' + CAST(@TotalIndexesRebuilt AS VARCHAR(10)) + CHAR(13) + CHAR(10);
SET @ReportSummary = @ReportSummary + 'Indexes reorganized: ' + CAST(@TotalIndexesReorganized AS VARCHAR(10)) + CHAR(13) + CHAR(10);
SET @ReportSummary = @ReportSummary + 'Errors encountered: ' + CAST(@TotalErrors AS VARCHAR(10)) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10);

-- Add detailed results if there were maintenance actions
IF EXISTS (SELECT 1 FROM #IndexStats WHERE RecommendedAction IN ('REBUILD', 'REORGANIZE'))
BEGIN
    SET @ReportSummary = @ReportSummary + 'Detailed Results:' + CHAR(13) + CHAR(10);
    SET @ReportSummary = @ReportSummary + '=================' + CHAR(13) + CHAR(10);
    
    DECLARE detail_cursor CURSOR FOR
    SELECT DatabaseName + '.' + SchemaName + '.' + ObjectName + ' [' + IndexName + '] - ' + 
           RecommendedAction + ' (' + CAST(CAST(FragmentationPercent AS DECIMAL(5,2)) AS VARCHAR(10)) + '%) - ' + 
           ExecutionStatus
    FROM #IndexStats
    WHERE RecommendedAction IN ('REBUILD', 'REORGANIZE')
    ORDER BY DatabaseName, ExecutionStatus DESC, FragmentationPercent DESC;
    
    DECLARE @DetailLine NVARCHAR(500);
    OPEN detail_cursor;
    FETCH NEXT FROM detail_cursor INTO @DetailLine;
    
    WHILE @@FETCH_STATUS = 0 AND LEN(@ReportSummary) < 3000 -- Limit email size
    BEGIN
        SET @ReportSummary = @ReportSummary + @DetailLine + CHAR(13) + CHAR(10);
        FETCH NEXT FROM detail_cursor INTO @DetailLine;
    END
    
    CLOSE detail_cursor;
    DEALLOCATE detail_cursor;
END

SET @EmailBody = @EmailBody + @ReportSummary;

-- Send email report
IF @SendEmail = 1
BEGIN
    SET @EmailSubject = @EmailSubjectPrefix + ' Index Maintenance Report - ' + 
                       CASE 
                           WHEN @TotalErrors > 0 THEN 'ERRORS ENCOUNTERED'
                           WHEN @TotalIndexesRebuilt + @TotalIndexesReorganized > 0 THEN 'MAINTENANCE COMPLETED'
                           ELSE 'NO ACTION REQUIRED'
                       END;
    
    BEGIN TRY
        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = @EmailProfile,
            @recipients = @EmailRecipients,
            @subject = @EmailSubject,
            @body = @EmailBody,
            @body_format = 'TEXT';
        
        PRINT '';
        PRINT 'Email report sent successfully to: ' + @EmailRecipients;
    END TRY
    BEGIN CATCH
        PRINT '';
        PRINT 'ERROR: Failed to send email report: ' + ERROR_MESSAGE();
        PRINT 'Check Database Mail configuration and profile: ' + @EmailProfile;
    END CATCH
END

-- Cleanup
DROP TABLE #IndexStats;

PRINT '';
PRINT '=============================================';
PRINT 'INDEX MAINTENANCE COMPLETED';
PRINT '=============================================';

-- Final notes
IF @ExecuteCommands = 0
BEGIN
    PRINT '';
    PRINT 'NOTE: To execute maintenance commands, set @ExecuteCommands = 1';
END

IF @SendEmail = 0
BEGIN
    PRINT 'NOTE: To enable email reports, set @SendEmail = 1 and configure email settings';
END