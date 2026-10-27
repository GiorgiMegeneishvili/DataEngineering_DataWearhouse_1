-- =====================================================
-- BRONZE layer, source: Chinook (CNK1)
-- Columns = same as MSSQL source (lowercased) + load_date
-- Table names prefixed with cnk1_ (same convention as nw1_)
-- Snapshot design: every table is RANGE-partitioned by load_date (yearly),
-- PRIMARY KEY (business key, load_date): one row per key per snapshot day;
-- the same key may repeat on different load_dates (history).
-- PK index also serves as the (key, load_date) lookup index.
-- =====================================================
CREATE SCHEMA IF NOT EXISTS bronze;

-- re-runnable: drops old (non-partitioned) versions
DROP TABLE IF EXISTS bronze.cnk1_customer CASCADE;
DROP TABLE IF EXISTS bronze.cnk1_employee CASCADE;
DROP TABLE IF EXISTS bronze.cnk1_artist CASCADE;
DROP TABLE IF EXISTS bronze.cnk1_album CASCADE;
DROP TABLE IF EXISTS bronze.cnk1_track CASCADE;
DROP TABLE IF EXISTS bronze.cnk1_genre CASCADE;
DROP TABLE IF EXISTS bronze.cnk1_mediatype CASCADE;
DROP TABLE IF EXISTS bronze.cnk1_invoice CASCADE;
DROP TABLE IF EXISTS bronze.cnk1_invoiceline CASCADE;

-- source: Chinook.dbo.Customer
CREATE TABLE bronze.cnk1_customer (
    customerid    INTEGER,
    firstname     VARCHAR(40),
    lastname      VARCHAR(20),
    company       VARCHAR(80),
    address       VARCHAR(70),
    city          VARCHAR(40),
    state         VARCHAR(40),
    country       VARCHAR(40),
    postalcode    VARCHAR(10),
    phone         VARCHAR(24),
    fax           VARCHAR(24),
    email         VARCHAR(60),
    supportrepid  INTEGER,
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (customerid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Chinook.dbo.Employee
CREATE TABLE bronze.cnk1_employee (
    employeeid    INTEGER,
    lastname      VARCHAR(20),
    firstname     VARCHAR(20),
    title         VARCHAR(30),
    reportsto     INTEGER,
    birthdate     TIMESTAMP,
    hiredate      TIMESTAMP,
    address       VARCHAR(70),
    city          VARCHAR(40),
    state         VARCHAR(40),
    country       VARCHAR(40),
    postalcode    VARCHAR(10),
    phone         VARCHAR(24),
    fax           VARCHAR(24),
    email         VARCHAR(60),
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (employeeid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Chinook.dbo.Artist
CREATE TABLE bronze.cnk1_artist (
    artistid      INTEGER,
    name          VARCHAR(120),
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (artistid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Chinook.dbo.Album
CREATE TABLE bronze.cnk1_album (
    albumid       INTEGER,
    title         VARCHAR(160),
    artistid      INTEGER,
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (albumid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Chinook.dbo.Track
CREATE TABLE bronze.cnk1_track (
    trackid       INTEGER,
    name          VARCHAR(200),
    albumid       INTEGER,
    mediatypeid   INTEGER,
    genreid       INTEGER,
    composer      VARCHAR(220),
    milliseconds  INTEGER,
    bytes         INTEGER,
    unitprice     NUMERIC(10,2),
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (trackid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Chinook.dbo.Genre
CREATE TABLE bronze.cnk1_genre (
    genreid       INTEGER,
    name          VARCHAR(120),
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (genreid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Chinook.dbo.MediaType
CREATE TABLE bronze.cnk1_mediatype (
    mediatypeid   INTEGER,
    name          VARCHAR(120),
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (mediatypeid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Chinook.dbo.Invoice
CREATE TABLE bronze.cnk1_invoice (
    invoiceid          INTEGER,
    customerid         INTEGER,
    invoicedate        TIMESTAMP,
    billingaddress     VARCHAR(70),
    billingcity        VARCHAR(40),
    billingstate       VARCHAR(40),
    billingcountry     VARCHAR(40),
    billingpostalcode  VARCHAR(10),
    total              NUMERIC(10,2),
    load_date          DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (invoiceid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Chinook.dbo.InvoiceLine
CREATE TABLE bronze.cnk1_invoiceline (
    invoicelineid INTEGER,
    invoiceid     INTEGER,
    trackid       INTEGER,
    unitprice     NUMERIC(10,2),
    quantity      INTEGER,
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (invoicelineid, load_date)
) PARTITION BY RANGE (load_date);

-- =====================================================
-- Partitions (explicit, one block per table): 2026, 2027 + DEFAULT
-- =====================================================

-- cnk1_customer
CREATE TABLE bronze.cnk1_customer_2026 PARTITION OF bronze.cnk1_customer
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.cnk1_customer_2027 PARTITION OF bronze.cnk1_customer
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.cnk1_customer_default PARTITION OF bronze.cnk1_customer DEFAULT;

-- cnk1_employee
CREATE TABLE bronze.cnk1_employee_2026 PARTITION OF bronze.cnk1_employee
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.cnk1_employee_2027 PARTITION OF bronze.cnk1_employee
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.cnk1_employee_default PARTITION OF bronze.cnk1_employee DEFAULT;

-- cnk1_artist
CREATE TABLE bronze.cnk1_artist_2026 PARTITION OF bronze.cnk1_artist
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.cnk1_artist_2027 PARTITION OF bronze.cnk1_artist
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.cnk1_artist_default PARTITION OF bronze.cnk1_artist DEFAULT;

-- cnk1_album
CREATE TABLE bronze.cnk1_album_2026 PARTITION OF bronze.cnk1_album
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.cnk1_album_2027 PARTITION OF bronze.cnk1_album
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.cnk1_album_default PARTITION OF bronze.cnk1_album DEFAULT;

-- cnk1_track
CREATE TABLE bronze.cnk1_track_2026 PARTITION OF bronze.cnk1_track
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.cnk1_track_2027 PARTITION OF bronze.cnk1_track
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.cnk1_track_default PARTITION OF bronze.cnk1_track DEFAULT;

-- cnk1_genre
CREATE TABLE bronze.cnk1_genre_2026 PARTITION OF bronze.cnk1_genre
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.cnk1_genre_2027 PARTITION OF bronze.cnk1_genre
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.cnk1_genre_default PARTITION OF bronze.cnk1_genre DEFAULT;

-- cnk1_mediatype
CREATE TABLE bronze.cnk1_mediatype_2026 PARTITION OF bronze.cnk1_mediatype
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.cnk1_mediatype_2027 PARTITION OF bronze.cnk1_mediatype
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.cnk1_mediatype_default PARTITION OF bronze.cnk1_mediatype DEFAULT;

-- cnk1_invoice
CREATE TABLE bronze.cnk1_invoice_2026 PARTITION OF bronze.cnk1_invoice
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.cnk1_invoice_2027 PARTITION OF bronze.cnk1_invoice
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.cnk1_invoice_default PARTITION OF bronze.cnk1_invoice DEFAULT;

-- cnk1_invoiceline
CREATE TABLE bronze.cnk1_invoiceline_2026 PARTITION OF bronze.cnk1_invoiceline
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.cnk1_invoiceline_2027 PARTITION OF bronze.cnk1_invoiceline
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.cnk1_invoiceline_default PARTITION OF bronze.cnk1_invoiceline DEFAULT;