CREATE TABLE [dbo].[Dim_Product] (
    [ProductKey]      BIGINT          IDENTITY NOT NULL,
    [ProductName]     VARCHAR (255)   NULL,
    [SupplierID]      INT             NULL,
    [CategoryID]      INT             NULL,
    [QuantityPerUnit] VARCHAR (255)   NULL,
    [UnitPrice]       DECIMAL (18, 2) NULL,
    [UnitsInStock]    INT             NULL,
    [UnitsOnOrder]    INT             NULL,
    [ReorderLevel]    INT             NULL,
    [Discontinued]    BIT             NULL,
    [StartDate]       DATE            NULL,
    [EndDate]         DATE            NULL,
    [IsCurrent]       BIT             NULL,
    [ProductID]       INT             NULL
);


GO