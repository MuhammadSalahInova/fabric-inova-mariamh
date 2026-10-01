CREATE TABLE [dbo].[Warehouse_Load_Control] (
    [GoldTable]     VARCHAR (100) NOT NULL,
    [SilverTable]   VARCHAR (100) NULL,
    [SilverVersion] BIGINT        NULL,
    [LastLoadedAt]  DATETIME2 (0) NULL,
    [SourceMinDate] DATE          NULL,
    [SourceMaxDate] DATE          NULL
);


GO