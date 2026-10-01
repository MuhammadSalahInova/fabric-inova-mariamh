CREATE TABLE [dbo].[Dim_Shipper] (
    [ShipperKey]  BIGINT        IDENTITY NOT NULL,
    [ShipperID]   INT           NOT NULL,
    [CompanyName] VARCHAR (255) NULL,
    [Phone]       VARCHAR (100) NULL,
    [StartDate]   DATE          NULL,
    [EndDate]     DATE          NULL,
    [IsCurrent]   BIT           NULL
);


GO