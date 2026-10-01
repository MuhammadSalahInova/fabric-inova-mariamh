CREATE TABLE [dbo].[Dim_Employee] (
    [EmployeeKey]      BIGINT         IDENTITY NOT NULL,
    [EmployeeID]       INT            NOT NULL,
    [LastName]         VARCHAR (100)  NULL,
    [FirstName]        VARCHAR (100)  NULL,
    [Title]            VARCHAR (255)  NULL,
    [TitleOfCourtesy]  VARCHAR (50)   NULL,
    [BirthDate]        DATE           NULL,
    [HireDate]         DATE           NULL,
    [Address]          VARCHAR (500)  NULL,
    [City]             VARCHAR (100)  NULL,
    [Region]           VARCHAR (100)  NULL,
    [PostalCode]       VARCHAR (50)   NULL,
    [Country]          VARCHAR (100)  NULL,
    [HomePhone]        VARCHAR (100)  NULL,
    [Extension]        VARCHAR (50)   NULL,
    [Notes]            VARCHAR (4000) NULL,
    [ReportsTo]        INT            NULL,
    [ReportsToMissing] BIT            NULL,
    [StartDate]        DATE           NULL,
    [EndDate]          DATE           NULL,
    [IsCurrent]        BIT            NULL
);


GO