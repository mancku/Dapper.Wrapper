CREATE TABLE public.Product (
    ProductID SERIAL PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    ProductNumber VARCHAR(25) NOT NULL,
    Color VARCHAR(15),
    StandardCost MONEY NOT NULL,
    ListPrice MONEY NOT NULL,
    Size VARCHAR(5),
    Weight DECIMAL(8, 2),
    ProductCategoryID INT,
    ProductModelID INT,
    SellStartDate TIMESTAMP NOT NULL,
    SellEndDate TIMESTAMP,
    DiscontinuedDate TIMESTAMP,
    ThumbNailPhoto BYTEA,
    ThumbnailPhotoFileName VARCHAR(50),
    rowguid UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMP NOT NULL
);

CREATE TABLE public.ProductDescription (
    ProductDescriptionID SERIAL PRIMARY KEY,
    Description VARCHAR(400) NOT NULL,
    rowguid UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMP NOT NULL
);

CREATE TABLE public.ProductModel (
    ProductModelID SERIAL PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    rowguid UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMP NOT NULL
);

CREATE TABLE public.ProductModelProductDescription (
    ProductModelID INT NOT NULL,
    ProductDescriptionID INT NOT NULL,
    Culture CHAR(6) NOT NULL,
    rowguid UUID NOT NULL DEFAULT gen_random_uuid(),
    ModifiedDate TIMESTAMP NOT NULL,
    PRIMARY KEY (ProductModelID, ProductDescriptionID, Culture)
);