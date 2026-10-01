CREATE TABLE [dbo].[Fact_Orders] (
    [OrderKey]        BIGINT          IDENTITY NOT NULL,
    [OrderID]         INT             NOT NULL,
    [CustomerKey]     BIGINT          NULL,
    [EmployeeKey]     BIGINT          NULL,
    [ShipperKey]      BIGINT          NULL,
    [OrderDateKey]    INT             NULL,
    [RequiredDateKey] INT             NULL,
    [ShippedDateKey]  INT             NULL,
    [Freight]         DECIMAL (18, 2) NULL,
    [OrderCount]      INT             NOT NULL,
    [ShipName]        VARCHAR (255)   NULL,
    [ShipAddress]     VARCHAR (500)   NULL,
    [ShipCity]        VARCHAR (100)   NULL,
    [ShipRegion]      VARCHAR (100)   NULL,
    [ShipPostalCode]  VARCHAR (50)    NULL,
    [ShipCountry]     VARCHAR (100)   NULL,
    [ShippedStatus]   VARCHAR (100)   NULL
);


GO