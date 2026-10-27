
-- =====================================================
-- BRONZE layer, source: AdventureWorks2022 (AW22)
-- Columns = same as MSSQL source (lowercased) + load_date
-- Table names prefixed aw22_ (same convention as nw1_ / cnk1_)
-- Snapshot design: RANGE-partitioned by load_date (yearly),
-- PRIMARY KEY (business key, load_date).
-- NOT loaded (Spark JDBC cannot read these MSSQL types):
--   Person.Person: AdditionalContactInfo, Demographics (xml)
--   Sales.Store: Demographics (xml)
--   Person.Address: SpatialLocation (geography)
--   HumanResources.Employee: OrganizationNode (hierarchyid)
-- uniqueidentifier (rowguid) is stored as VARCHAR(36).
-- "group" in aw22_salesterritory is a reserved word -> always quoted.
-- =====================================================
CREATE SCHEMA IF NOT EXISTS bronze;

-- re-runnable: drops old versions
DROP TABLE IF EXISTS bronze.aw22_salesorderheader CASCADE;
DROP TABLE IF EXISTS bronze.aw22_salesorderdetail CASCADE;
DROP TABLE IF EXISTS bronze.aw22_customer CASCADE;
DROP TABLE IF EXISTS bronze.aw22_salesperson CASCADE;
DROP TABLE IF EXISTS bronze.aw22_salesterritory CASCADE;
DROP TABLE IF EXISTS bronze.aw22_store CASCADE;
DROP TABLE IF EXISTS bronze.aw22_person CASCADE;
DROP TABLE IF EXISTS bronze.aw22_address CASCADE;
DROP TABLE IF EXISTS bronze.aw22_stateprovince CASCADE;
DROP TABLE IF EXISTS bronze.aw22_countryregion CASCADE;
DROP TABLE IF EXISTS bronze.aw22_businessentityaddress CASCADE;
DROP TABLE IF EXISTS bronze.aw22_employee CASCADE;
DROP TABLE IF EXISTS bronze.aw22_product CASCADE;
DROP TABLE IF EXISTS bronze.aw22_productsubcategory CASCADE;
DROP TABLE IF EXISTS bronze.aw22_productcategory CASCADE;
DROP TABLE IF EXISTS bronze.aw22_workorder CASCADE;
DROP TABLE IF EXISTS bronze.aw22_purchaseorderheader CASCADE;
DROP TABLE IF EXISTS bronze.aw22_purchaseorderdetail CASCADE;
DROP TABLE IF EXISTS bronze.aw22_vendor CASCADE;

-- source: AdventureWorks2022.Sales.SalesOrderHeader
CREATE TABLE bronze.aw22_salesorderheader (
    salesorderid                INTEGER,
    revisionnumber              SMALLINT,
    orderdate                   TIMESTAMP,
    duedate                     TIMESTAMP,
    shipdate                    TIMESTAMP,
    status                      SMALLINT,
    onlineorderflag             BOOLEAN,
    salesordernumber            VARCHAR(25),
    purchaseordernumber         VARCHAR(25),
    accountnumber               VARCHAR(15),
    customerid                  INTEGER,
    salespersonid               INTEGER,
    territoryid                 INTEGER,
    billtoaddressid             INTEGER,
    shiptoaddressid             INTEGER,
    shipmethodid                INTEGER,
    creditcardid                INTEGER,
    creditcardapprovalcode      VARCHAR(15),
    currencyrateid              INTEGER,
    subtotal                    NUMERIC(19,4),
    taxamt                      NUMERIC(19,4),
    freight                     NUMERIC(19,4),
    totaldue                    NUMERIC(19,4),
    comment                     VARCHAR(128),
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (salesorderid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Sales.SalesOrderDetail
CREATE TABLE bronze.aw22_salesorderdetail (
    salesorderid                INTEGER,
    salesorderdetailid          INTEGER,
    carriertrackingnumber       VARCHAR(25),
    orderqty                    SMALLINT,
    productid                   INTEGER,
    specialofferid              INTEGER,
    unitprice                   NUMERIC(19,4),
    unitpricediscount           NUMERIC(19,4),
    linetotal                   NUMERIC(38,6),
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (salesorderid, salesorderdetailid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Sales.Customer
CREATE TABLE bronze.aw22_customer (
    customerid                  INTEGER,
    personid                    INTEGER,
    storeid                     INTEGER,
    territoryid                 INTEGER,
    accountnumber               VARCHAR(10),
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (customerid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Sales.SalesPerson
CREATE TABLE bronze.aw22_salesperson (
    businessentityid            INTEGER,
    territoryid                 INTEGER,
    salesquota                  NUMERIC(19,4),
    bonus                       NUMERIC(19,4),
    commissionpct               NUMERIC(10,4),
    salesytd                    NUMERIC(19,4),
    saleslastyear               NUMERIC(19,4),
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (businessentityid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Sales.SalesTerritory
CREATE TABLE bronze.aw22_salesterritory (
    territoryid                 INTEGER,
    name                        VARCHAR(50),
    countryregioncode           VARCHAR(3),
    "group"                     VARCHAR(50),
    salesytd                    NUMERIC(19,4),
    saleslastyear               NUMERIC(19,4),
    costytd                     NUMERIC(19,4),
    costlastyear                NUMERIC(19,4),
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (territoryid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Sales.Store
CREATE TABLE bronze.aw22_store (
    businessentityid            INTEGER,
    name                        VARCHAR(50),
    salespersonid               INTEGER,
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (businessentityid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Person.Person
CREATE TABLE bronze.aw22_person (
    businessentityid            INTEGER,
    persontype                  CHAR(2),
    namestyle                   BOOLEAN,
    title                       VARCHAR(8),
    firstname                   VARCHAR(50),
    middlename                  VARCHAR(50),
    lastname                    VARCHAR(50),
    suffix                      VARCHAR(10),
    emailpromotion              INTEGER,
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (businessentityid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Person.Address
CREATE TABLE bronze.aw22_address (
    addressid                   INTEGER,
    addressline1                VARCHAR(60),
    addressline2                VARCHAR(60),
    city                        VARCHAR(30),
    stateprovinceid             INTEGER,
    postalcode                  VARCHAR(15),
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (addressid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Person.StateProvince
CREATE TABLE bronze.aw22_stateprovince (
    stateprovinceid             INTEGER,
    stateprovincecode           CHAR(3),
    countryregioncode           VARCHAR(3),
    isonlystateprovinceflag     BOOLEAN,
    name                        VARCHAR(50),
    territoryid                 INTEGER,
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (stateprovinceid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Person.CountryRegion
CREATE TABLE bronze.aw22_countryregion (
    countryregioncode           VARCHAR(3),
    name                        VARCHAR(50),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (countryregioncode, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Person.BusinessEntityAddress
CREATE TABLE bronze.aw22_businessentityaddress (
    businessentityid            INTEGER,
    addressid                   INTEGER,
    addresstypeid               INTEGER,
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (businessentityid, addressid, addresstypeid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.HumanResources.Employee
CREATE TABLE bronze.aw22_employee (
    businessentityid            INTEGER,
    nationalidnumber            VARCHAR(15),
    loginid                     VARCHAR(256),
    organizationlevel           SMALLINT,
    jobtitle                    VARCHAR(50),
    birthdate                   DATE,
    maritalstatus               CHAR(1),
    gender                      CHAR(1),
    hiredate                    DATE,
    salariedflag                BOOLEAN,
    vacationhours               SMALLINT,
    sickleavehours              SMALLINT,
    currentflag                 BOOLEAN,
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (businessentityid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Production.Product
CREATE TABLE bronze.aw22_product (
    productid                   INTEGER,
    name                        VARCHAR(50),
    productnumber               VARCHAR(25),
    makeflag                    BOOLEAN,
    finishedgoodsflag           BOOLEAN,
    color                       VARCHAR(15),
    safetystocklevel            SMALLINT,
    reorderpoint                SMALLINT,
    standardcost                NUMERIC(19,4),
    listprice                   NUMERIC(19,4),
    size                        VARCHAR(5),
    sizeunitmeasurecode         CHAR(3),
    weightunitmeasurecode       CHAR(3),
    weight                      NUMERIC(8,2),
    daystomanufacture           INTEGER,
    productline                 CHAR(2),
    class                       CHAR(2),
    style                       CHAR(2),
    productsubcategoryid        INTEGER,
    productmodelid              INTEGER,
    sellstartdate               TIMESTAMP,
    sellenddate                 TIMESTAMP,
    discontinueddate            TIMESTAMP,
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (productid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Production.ProductSubcategory
CREATE TABLE bronze.aw22_productsubcategory (
    productsubcategoryid        INTEGER,
    productcategoryid           INTEGER,
    name                        VARCHAR(50),
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (productsubcategoryid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Production.ProductCategory
CREATE TABLE bronze.aw22_productcategory (
    productcategoryid           INTEGER,
    name                        VARCHAR(50),
    rowguid                     VARCHAR(36),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (productcategoryid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Production.WorkOrder
CREATE TABLE bronze.aw22_workorder (
    workorderid                 INTEGER,
    productid                   INTEGER,
    orderqty                    INTEGER,
    stockedqty                  INTEGER,
    scrappedqty                 SMALLINT,
    startdate                   TIMESTAMP,
    enddate                     TIMESTAMP,
    duedate                     TIMESTAMP,
    scrapreasonid               SMALLINT,
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (workorderid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Purchasing.PurchaseOrderHeader
CREATE TABLE bronze.aw22_purchaseorderheader (
    purchaseorderid             INTEGER,
    revisionnumber              SMALLINT,
    status                      SMALLINT,
    employeeid                  INTEGER,
    vendorid                    INTEGER,
    shipmethodid                INTEGER,
    orderdate                   TIMESTAMP,
    shipdate                    TIMESTAMP,
    subtotal                    NUMERIC(19,4),
    taxamt                      NUMERIC(19,4),
    freight                     NUMERIC(19,4),
    totaldue                    NUMERIC(19,4),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (purchaseorderid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Purchasing.PurchaseOrderDetail
CREATE TABLE bronze.aw22_purchaseorderdetail (
    purchaseorderid             INTEGER,
    purchaseorderdetailid       INTEGER,
    duedate                     TIMESTAMP,
    orderqty                    SMALLINT,
    productid                   INTEGER,
    unitprice                   NUMERIC(19,4),
    linetotal                   NUMERIC(19,4),
    receivedqty                 NUMERIC(8,2),
    rejectedqty                 NUMERIC(8,2),
    stockedqty                  NUMERIC(9,2),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (purchaseorderid, purchaseorderdetailid, load_date)
) PARTITION BY RANGE (load_date);

-- source: AdventureWorks2022.Purchasing.Vendor
CREATE TABLE bronze.aw22_vendor (
    businessentityid            INTEGER,
    accountnumber               VARCHAR(15),
    name                        VARCHAR(50),
    creditrating                SMALLINT,
    preferredvendorstatus       BOOLEAN,
    activeflag                  BOOLEAN,
    purchasingwebserviceurl     VARCHAR(1024),
    modifieddate                TIMESTAMP,
    load_date                   DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (businessentityid, load_date)
) PARTITION BY RANGE (load_date);

-- =====================================================
-- Partitions (explicit, one block per table): 2026, 2027 + DEFAULT
-- =====================================================

-- aw22_salesorderheader
CREATE TABLE bronze.aw22_salesorderheader_2026 PARTITION OF bronze.aw22_salesorderheader
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_salesorderheader_2027 PARTITION OF bronze.aw22_salesorderheader
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_salesorderheader_default PARTITION OF bronze.aw22_salesorderheader DEFAULT;

-- aw22_salesorderdetail
CREATE TABLE bronze.aw22_salesorderdetail_2026 PARTITION OF bronze.aw22_salesorderdetail
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_salesorderdetail_2027 PARTITION OF bronze.aw22_salesorderdetail
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_salesorderdetail_default PARTITION OF bronze.aw22_salesorderdetail DEFAULT;

-- aw22_customer
CREATE TABLE bronze.aw22_customer_2026 PARTITION OF bronze.aw22_customer
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_customer_2027 PARTITION OF bronze.aw22_customer
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_customer_default PARTITION OF bronze.aw22_customer DEFAULT;

-- aw22_salesperson
CREATE TABLE bronze.aw22_salesperson_2026 PARTITION OF bronze.aw22_salesperson
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_salesperson_2027 PARTITION OF bronze.aw22_salesperson
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_salesperson_default PARTITION OF bronze.aw22_salesperson DEFAULT;

-- aw22_salesterritory
CREATE TABLE bronze.aw22_salesterritory_2026 PARTITION OF bronze.aw22_salesterritory
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_salesterritory_2027 PARTITION OF bronze.aw22_salesterritory
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_salesterritory_default PARTITION OF bronze.aw22_salesterritory DEFAULT;

-- aw22_store
CREATE TABLE bronze.aw22_store_2026 PARTITION OF bronze.aw22_store
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_store_2027 PARTITION OF bronze.aw22_store
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_store_default PARTITION OF bronze.aw22_store DEFAULT;

-- aw22_person
CREATE TABLE bronze.aw22_person_2026 PARTITION OF bronze.aw22_person
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_person_2027 PARTITION OF bronze.aw22_person
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_person_default PARTITION OF bronze.aw22_person DEFAULT;

-- aw22_address
CREATE TABLE bronze.aw22_address_2026 PARTITION OF bronze.aw22_address
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_address_2027 PARTITION OF bronze.aw22_address
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_address_default PARTITION OF bronze.aw22_address DEFAULT;

-- aw22_stateprovince
CREATE TABLE bronze.aw22_stateprovince_2026 PARTITION OF bronze.aw22_stateprovince
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_stateprovince_2027 PARTITION OF bronze.aw22_stateprovince
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_stateprovince_default PARTITION OF bronze.aw22_stateprovince DEFAULT;

-- aw22_countryregion
CREATE TABLE bronze.aw22_countryregion_2026 PARTITION OF bronze.aw22_countryregion
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_countryregion_2027 PARTITION OF bronze.aw22_countryregion
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_countryregion_default PARTITION OF bronze.aw22_countryregion DEFAULT;

-- aw22_businessentityaddress
CREATE TABLE bronze.aw22_businessentityaddress_2026 PARTITION OF bronze.aw22_businessentityaddress
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_businessentityaddress_2027 PARTITION OF bronze.aw22_businessentityaddress
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_businessentityaddress_default PARTITION OF bronze.aw22_businessentityaddress DEFAULT;

-- aw22_employee
CREATE TABLE bronze.aw22_employee_2026 PARTITION OF bronze.aw22_employee
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_employee_2027 PARTITION OF bronze.aw22_employee
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_employee_default PARTITION OF bronze.aw22_employee DEFAULT;

-- aw22_product
CREATE TABLE bronze.aw22_product_2026 PARTITION OF bronze.aw22_product
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_product_2027 PARTITION OF bronze.aw22_product
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_product_default PARTITION OF bronze.aw22_product DEFAULT;

-- aw22_productsubcategory
CREATE TABLE bronze.aw22_productsubcategory_2026 PARTITION OF bronze.aw22_productsubcategory
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_productsubcategory_2027 PARTITION OF bronze.aw22_productsubcategory
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_productsubcategory_default PARTITION OF bronze.aw22_productsubcategory DEFAULT;

-- aw22_productcategory
CREATE TABLE bronze.aw22_productcategory_2026 PARTITION OF bronze.aw22_productcategory
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_productcategory_2027 PARTITION OF bronze.aw22_productcategory
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_productcategory_default PARTITION OF bronze.aw22_productcategory DEFAULT;

-- aw22_workorder
CREATE TABLE bronze.aw22_workorder_2026 PARTITION OF bronze.aw22_workorder
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_workorder_2027 PARTITION OF bronze.aw22_workorder
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_workorder_default PARTITION OF bronze.aw22_workorder DEFAULT;

-- aw22_purchaseorderheader
CREATE TABLE bronze.aw22_purchaseorderheader_2026 PARTITION OF bronze.aw22_purchaseorderheader
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_purchaseorderheader_2027 PARTITION OF bronze.aw22_purchaseorderheader
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_purchaseorderheader_default PARTITION OF bronze.aw22_purchaseorderheader DEFAULT;

-- aw22_purchaseorderdetail
CREATE TABLE bronze.aw22_purchaseorderdetail_2026 PARTITION OF bronze.aw22_purchaseorderdetail
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_purchaseorderdetail_2027 PARTITION OF bronze.aw22_purchaseorderdetail
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_purchaseorderdetail_default PARTITION OF bronze.aw22_purchaseorderdetail DEFAULT;

-- aw22_vendor
CREATE TABLE bronze.aw22_vendor_2026 PARTITION OF bronze.aw22_vendor
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.aw22_vendor_2027 PARTITION OF bronze.aw22_vendor
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.aw22_vendor_default PARTITION OF bronze.aw22_vendor DEFAULT;
