CREATE TABLE [dbo].[Dim_Date] (
    [DateKey]     INT          NOT NULL,
    [Date]        DATE         NOT NULL,
    [Year]        INT          NOT NULL,
    [Quarter]     INT          NOT NULL,
    [Month]       INT          NOT NULL,
    [MonthName]   VARCHAR (20) NOT NULL,
    [MonthNumber] INT          NOT NULL,
    [WeekNumber]  INT          NOT NULL,
    [Day]         INT          NOT NULL,
    [DayName]     VARCHAR (20) NOT NULL,
    [IsWeekend]   BIT          NOT NULL,
    [YearMonth]   VARCHAR (7)  NOT NULL,
    [YearQuarter] VARCHAR (7)  NOT NULL
);


GO