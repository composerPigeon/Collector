CREATE DATABASE sales;

BEGIN;
\connect sales;

DROP TABLE IF EXISTS Customers CASCADE;
DROP TABLE IF EXISTS Address   CASCADE;

-- ── Address table ───────────────────────────────────────────────
CREATE TABLE Address (
    id           SERIAL PRIMARY KEY,
    street       VARCHAR(100) NOT NULL,
    houseNumber  VARCHAR(10)  NOT NULL,
    city         VARCHAR(100) NOT NULL,
    zipcode      VARCHAR(20)  NOT NULL
);

-- ── Customers table ─────────────────────────────────────────────
CREATE TABLE Customers (
    id         SERIAL PRIMARY KEY,
    fname      VARCHAR(50)  NOT NULL,
    lname      VARCHAR(50)  NOT NULL,
    age        INT          NOT NULL CHECK (age >= 0),
    addressId  INT          NOT NULL REFERENCES Address(id)
);

-- ── Populate Address ────────────────────────────────────────────
INSERT INTO Address (id, street, houseNumber, city, zipcode) VALUES
    (1,  'Aspen Way',       '135', 'Ashland',      '89546'),
    (2,  'Elm Street',      '347', 'Arlington',    '46238'),
    (3,  'Poplar Lane',     '213', 'Greenville',   '94041'),
    (4,  'Magnolia Avenue', '77',  'Madison',      '88262'),
    (5,  'Dogwood Lane',    '358', 'Milford',      '10987'),
    (6,  'Walnut Drive',    '504', 'Oakland',      '45213'),
    (7,  'Willow Road',     '98',  'Salem',        '61734'),
    (8,  'Cedar Lane',      '690', 'Dayton',       '73001'),
    (9,  'Pine Road',       '402', 'Franklin',     '67822'),
    (10, 'Spruce Street',   '321', 'Shelbyville',  '83674'),
    (11, 'Hickory Drive',   '23',  'Springfield',  '92015'),
    (12, 'Magnolia Avenue', '172', 'Georgetown',   '23879'),
    (13, 'Ash Avenue',      '551', 'Lexington',    '50391'),
    (14, 'Sycamore Court',  '444', 'Arlington',    '41908'),
    (15, 'Maple Street',    '247', 'Ashland',      '34790'),
    (16, 'Oak Avenue',      '412', 'Riverview',    '93516'),
    (17, 'Evergreen Road',  '310', 'Auburn',       '14621'),
    (18, 'Elm Street',      '250', 'Bristol',      '60913'),
    (19, 'Sycamore Court',  '175', 'Centerville',  '77602'),
    (20, 'Hickory Drive',   '156', 'Bristol',      '56743'),
    (21, 'Sycamore Court',  '75',  'Clinton',      '30879'),
    (22, 'Cedar Lane',      '112', 'Oakland',      '99114'),
    (23, 'Willow Road',     '461', 'Madison',      '21569'),
    (24, 'Walnut Drive',    '408', 'Kingston',     '48251'),
    (25, 'Juniper Street',  '62',  'Riverview',    '27384'),
    (26, 'Hickory Drive',   '88',  'Springfield',  '79406'),
    (27, 'Ash Avenue',      '391', 'Dayton',       '68907'),
    (28, 'Pine Road',       '58',  'Fairview',     '83217'),
    (29, 'Magnolia Avenue', '329', 'Milford',      '54138'),
    (30, 'Birch Way',       '178', 'Centerville',  '26734'),
    (31, 'Poplar Lane',     '133', 'Greenville',   '72541'),
    (32, 'Juniper Street',  '432', 'Salem',        '60142'),
    (33, 'Evergreen Road',  '496', 'Springfield',  '64359'),
    (34, 'Willow Road',     '153', 'Auburn',       '52436'),
    (35, 'Ash Avenue',      '450', 'Franklin',     '60711'),
    (36, 'Juniper Street',  '112', 'Fairview',     '28564'),
    (37, 'Dogwood Lane',    '412', 'Clinton',      '71658'),
    (38, 'Chestnut Street', '244', 'Kingston',     '20509'),
    (39, 'Cedar Lane',      '498', 'Lexington',    '25599'),
    (40, 'Laurel Street',   '52',  'Bristol',      '97344'),
    (41, 'Spruce Street',   '320', 'Georgetown',   '35206'),
    (42, 'Oak Avenue',      '90',  'Fairview',     '61593'),
    (43, 'Poplar Lane',     '207', 'Shelbyville',  '73307'),
    (44, 'Evergreen Road',  '274', 'Dayton',       '67084'),
    (45, 'Sycamore Court',  '309', 'Fairview',     '64392'),
    (46, 'Cedar Lane',      '188', 'Riverview',    '22496'),
    (47, 'Hickory Drive',   '278', 'Georgetown',   '14599'),
    (48, 'Birch Way',       '289', 'Greenville',   '63481'),
    (49, 'Oak Avenue',      '62',  'Salem',        '32988'),
    (50, 'Willow Road',     '428', 'Clinton',      '14445');

-- ── Populate Customers ──────────────────────────────────────────
INSERT INTO Customers (id, fname, lname, age, addressId) VALUES
    (1,  'Mia',       'Martinez',   63,  1),
    (2,  'Nicholas',  'Green',      45,  2),
    (3,  'Daniel',    'Johnson',    55,  3),
    (4,  'Anthony',   'Lopez',      60,  4),
    (5,  'Kevin',     'Sanchez',    40,  5),
    (6,  'Amelia',    'Clark',      48,  6),
    (7,  'Matthew',   'White',      29,  7),
    (8,  'Ryan',      'Walker',     31,  8),
    (9,  'Joseph',    'Moore',      22,  9),
    (10, 'Emily',     'King',       70, 10),
    (11, 'Grace',     'Wilson',     64, 11),
    (12, 'Isabella',  'Torres',     68, 12),
    (13, 'Brian',     'Adams',      33, 13),
    (14, 'Sophia',    'Jackson',    54, 14),
    (15, 'Brandon',   'Young',      46, 15),
    (16, 'Lily',      'Allen',      23, 16),
    (17, 'Benjamin',  'Perez',      28, 17),
    (18, 'Abigail',   'Miller',     35, 18),
    (19, 'Alexander', 'Hernandez',  52, 19),
    (20, 'Chloe',     'Davis',      62, 20),
    (21, 'Christopher','Thompson',  59, 21),
    (22, 'Joshua',    'Brown',      36, 22),
    (23, 'Andrew',    'Robinson',   24, 23),
    (24, 'Hannah',    'Flores',     71, 24),
    (25, 'Harper',    'Garcia',     53, 25),
    (26, 'Evelyn',    'Williams',   58, 26),
    (27, 'Justin',    'Taylor',     19, 27),
    (28, 'Scarlett',  'Nguyen',     25, 28),
    (29, 'Olivia',    'Scott',      47, 29),
    (30, 'James',     'Johnson',    44, 30),
    (31, 'Ella',      'Lewis',      42, 31),
    (32, 'David',     'Anderson',   37, 32),
    (33, 'Charlotte', 'Ramirez',    38, 33),
    (34, 'John',      'Hill',       73, 34),
    (35, 'Jacob',     'Moore',      20, 35),
    (36, 'Alexander', 'Harris',     57, 36),
    (37, 'Evelyn',    'Anderson',   34, 37),
    (38, 'Avery',     'Taylor',     61, 38),
    (39, 'Joshua',    'Thomas',     39, 39),
    (40, 'Elizabeth', 'Brown',      67, 40),
    (41, 'Joseph',    'White',      48, 41),
    (42, 'Grace',     'Jones',      41, 42),
    (43, 'Benjamin',  'Jackson',    21, 43),
    (44, 'Alexander', 'Walker',     56, 44),
    (45, 'Chloe',     'Gonzalez',   32, 45),
    (46, 'David',     'Clark',      71, 46),
    (47, 'Victoria',  'Thompson',   52, 47),
    (48, 'Justin',    'Garcia',     28, 48),
    (49, 'James',     'Smith',      60, 49),
    (50, 'Isabella',  'Lewis',      30, 50);

COMMIT;