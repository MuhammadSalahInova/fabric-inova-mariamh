CREATE TABLE [dbo].[Fact_Order_Details] (
    [OrderDetailKey] BIGINT          IDENTITY NOT NULL,
    [OrderID]        INT             NOT NULL,
    [ProductKey]     BIGINT          NULL,
    [CustomerKey]    BIGINT          NULL,
    [OrderDateKey]   INT             NULL,
    [UnitPrice]      DECIMAL (18, 2) NULL,
    [Quantity]       INT             NULL,
    [Discount]       DECIMAL (18, 4) NULL,
    [LineCount]      INT             NOT NULL
);


GO