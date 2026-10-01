CREATE TABLE [dbo].[Dim_Customer] (
    [CustomerKey]  BIGINT        IDENTITY NOT NULL,
    [CustomerID]   VARCHAR (50)  NOT NULL,
    [CompanyName]  VARCHAR (255) NULL,
    [ContactName]  VARCHAR (255) NULL,
    [ContactTitle] VARCHAR (255) NULL,
    [Address]      VARCHAR (500) NULL,
    [City]         VARCHAR (100) NULL,
    [Region]       VARCHAR (100) NULL,
    [PostalCode]   VARCHAR (50)  NULL,
    [Country]      VARCHAR (100) NULL,
    [Phone]        VARCHAR (100) NULL,
    [Fax]          VARCHAR (100) NULL,
    [StartDate]    DATE          NULL,
    [EndDate]      DATE          NULL,
    [IsCurrent]    BIT           NULL
);


GO