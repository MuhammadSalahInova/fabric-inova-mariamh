CREATE TABLE [dbo].[Dim_Category] (
    [CategoryKey]  BIGINT         IDENTITY NOT NULL,
    [CategoryID]   INT            NOT NULL,
    [CategoryName] VARCHAR (255)  NULL,
    [Description]  VARCHAR (4000) NULL,
    [StartDate]    DATE           NULL,
    [EndDate]      DATE           NULL,
    [IsCurrent]    BIT            NULL
);


GO