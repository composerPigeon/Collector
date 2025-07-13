db = db.getSiblingDB('admin');

db.auth('mongo', 'password');

db = db.getSiblingDB('sales');

db.createCollection('customers');
db.customers.insertMany([
  {
    "fname": "Mia",
    "lname": "Martinez",
    "age": 63,
    "address": {
      "street": "Aspen Way",
      "house_number": "135",
      "city": "Ashland",
      "zipcode": "89546"
    }
  },
  {
    "fname": "Nicholas",
    "lname": "Green",
    "age": 45,
    "address": {
      "street": "Elm Street",
      "house_number": "347",
      "city": "Arlington",
      "zipcode": "46238"
    }
  },
  {
    "fname": "Daniel",
    "lname": "Johnson",
    "age": 55,
    "address": {
      "street": "Poplar Lane",
      "house_number": "213",
      "city": "Greenville",
      "zipcode": "94041"
    }
  },
  {
    "fname": "Anthony",
    "lname": "Lopez",
    "age": 60,
    "address": {
      "street": "Magnolia Avenue",
      "house_number": "77",
      "city": "Madison",
      "zipcode": "88262"
    }
  },
  {
    "fname": "Kevin",
    "lname": "Sanchez",
    "age": 40,
    "address": {
      "street": "Dogwood Lane",
      "house_number": "358",
      "city": "Milford",
      "zipcode": "10987"
    }
  },
  {
    "fname": "Amelia",
    "lname": "Clark",
    "age": 48,
    "address": {
      "street": "Walnut Drive",
      "house_number": "504",
      "city": "Oakland",
      "zipcode": "45213"
    }
  },
  {
    "fname": "Matthew",
    "lname": "White",
    "age": 29,
    "address": {
      "street": "Willow Road",
      "house_number": "98",
      "city": "Salem",
      "zipcode": "61734"
    }
  },
  {
    "fname": "Ryan",
    "lname": "Walker",
    "age": 31,
    "address": {
      "street": "Cedar Lane",
      "house_number": "690",
      "city": "Dayton",
      "zipcode": "73001"
    }
  },
  {
    "fname": "Joseph",
    "lname": "Moore",
    "age": 22,
    "address": {
      "street": "Pine Road",
      "house_number": "402",
      "city": "Franklin",
      "zipcode": "67822"
    }
  },
  {
    "fname": "Emily",
    "lname": "King",
    "age": 70,
    "address": {
      "street": "Spruce Street",
      "house_number": "321",
      "city": "Shelbyville",
      "zipcode": "83674"
    }
  },
  {
    "fname": "Grace",
    "lname": "Wilson",
    "age": 64,
    "address": {
      "street": "Hickory Drive",
      "house_number": "23",
      "city": "Springfield",
      "zipcode": "92015"
    }
  },
  {
    "fname": "Isabella",
    "lname": "Torres",
    "age": 68,
    "address": {
      "street": "Magnolia Avenue",
      "house_number": "172",
      "city": "Georgetown",
      "zipcode": "23879"
    }
  },
  {
    "fname": "Brian",
    "lname": "Adams",
    "age": 33,
    "address": {
      "street": "Ash Avenue",
      "house_number": "551",
      "city": "Lexington",
      "zipcode": "50391"
    }
  },
  {
    "fname": "Sophia",
    "lname": "Jackson",
    "age": 54,
    "address": {
      "street": "Sycamore Court",
      "house_number": "444",
      "city": "Arlington",
      "zipcode": "41908"
    }
  },
  {
    "fname": "Brandon",
    "lname": "Young",
    "age": 46,
    "address": {
      "street": "Maple Street",
      "house_number": "247",
      "city": "Ashland",
      "zipcode": "34790"
    }
  },
  {
    "fname": "Lily",
    "lname": "Allen",
    "age": 23,
    "address": {
      "street": "Oak Avenue",
      "house_number": "412",
      "city": "Riverview",
      "zipcode": "93516"
    }
  },
  {
    "fname": "Benjamin",
    "lname": "Perez",
    "age": 28,
    "address": {
      "street": "Evergreen Road",
      "house_number": "310",
      "city": "Auburn",
      "zipcode": "14621"
    }
  },
  {
    "fname": "Abigail",
    "lname": "Miller",
    "age": 35,
    "address": {
      "street": "Elm Street",
      "house_number": "250",
      "city": "Bristol",
      "zipcode": "60913"
    }
  },
  {
    "fname": "Alexander",
    "lname": "Hernandez",
    "age": 52,
    "address": {
      "street": "Sycamore Court",
      "house_number": "175",
      "city": "Centerville",
      "zipcode": "77602"
    }
  },
  {
    "fname": "Chloe",
    "lname": "Davis",
    "age": 62,
    "address": {
      "street": "Hickory Drive",
      "house_number": "156",
      "city": "Bristol",
      "zipcode": "56743"
    }
  },
  {
    "fname": "Christopher",
    "lname": "Thompson",
    "age": 59,
    "address": {
      "street": "Sycamore Court",
      "house_number": "75",
      "city": "Clinton",
      "zipcode": "30879"
    }
  },
  {
    "fname": "Joshua",
    "lname": "Brown",
    "age": 36,
    "address": {
      "street": "Cedar Lane",
      "house_number": "112",
      "city": "Oakland",
      "zipcode": "99114"
    }
  },
  {
    "fname": "Andrew",
    "lname": "Robinson",
    "age": 24,
    "address": {
      "street": "Willow Road",
      "house_number": "461",
      "city": "Madison",
      "zipcode": "21569"
    }
  },
  {
    "fname": "Hannah",
    "lname": "Flores",
    "age": 71,
    "address": {
      "street": "Walnut Drive",
      "house_number": "408",
      "city": "Kingston",
      "zipcode": "48251"
    }
  },
  {
    "fname": "Harper",
    "lname": "Garcia",
    "age": 53,
    "address": {
      "street": "Juniper Street",
      "house_number": "62",
      "city": "Riverview",
      "zipcode": "27384"
    }
  },
  {
    "fname": "Evelyn",
    "lname": "Williams",
    "age": 58,
    "address": {
      "street": "Hickory Drive",
      "house_number": "88",
      "city": "Springfield",
      "zipcode": "79406"
    }
  },
  {
    "fname": "Justin",
    "lname": "Taylor",
    "age": 19,
    "address": {
      "street": "Ash Avenue",
      "house_number": "391",
      "city": "Dayton",
      "zipcode": "68907"
    }
  },
  {
    "fname": "Scarlett",
    "lname": "Nguyen",
    "age": 25,
    "address": {
      "street": "Pine Road",
      "house_number": "58",
      "city": "Fairview",
      "zipcode": "83217"
    }
  },
  {
    "fname": "Olivia",
    "lname": "Scott",
    "age": 47,
    "address": {
      "street": "Magnolia Avenue",
      "house_number": "329",
      "city": "Milford",
      "zipcode": "54138"
    }
  },
  {
    "fname": "James",
    "lname": "Johnson",
    "age": 44,
    "address": {
      "street": "Birch Way",
      "house_number": "178",
      "city": "Centerville",
      "zipcode": "26734"
    }
  },
  {
    "fname": "Ella",
    "lname": "Lewis",
    "age": 42,
    "address": {
      "street": "Poplar Lane",
      "house_number": "133",
      "city": "Greenville",
      "zipcode": "72541"
    }
  },
  {
    "fname": "David",
    "lname": "Anderson",
    "age": 37,
    "address": {
      "street": "Juniper Street",
      "house_number": "432",
      "city": "Salem",
      "zipcode": "60142"
    }
  },
  {
    "fname": "Charlotte",
    "lname": "Ramirez",
    "age": 38,
    "address": {
      "street": "Evergreen Road",
      "house_number": "496",
      "city": "Springfield",
      "zipcode": "64359"
    }
  },
  {
    "fname": "John",
    "lname": "Hill",
    "age": 73,
    "address": {
      "street": "Willow Road",
      "house_number": "153",
      "city": "Auburn",
      "zipcode": "52436"
    }
  },
  {
    "fname": "Jacob",
    "lname": "Moore",
    "age": 20,
    "address": {
      "street": "Ash Avenue",
      "house_number": "450",
      "city": "Franklin",
      "zipcode": "60711"
    }
  },
  {
    "fname": "Alexander",
    "lname": "Harris",
    "age": 57,
    "address": {
      "street": "Juniper Street",
      "house_number": "112",
      "city": "Fairview",
      "zipcode": "28564"
    }
  },
  {
    "fname": "Evelyn",
    "lname": "Anderson",
    "age": 34,
    "address": {
      "street": "Dogwood Lane",
      "house_number": "412",
      "city": "Clinton",
      "zipcode": "71658"
    }
  },
  {
    "fname": "Avery",
    "lname": "Taylor",
    "age": 61,
    "address": {
      "street": "Chestnut Street",
      "house_number": "244",
      "city": "Kingston",
      "zipcode": "20509"
    }
  },
  {
    "fname": "Joshua",
    "lname": "Thomas",
    "age": 39,
    "address": {
      "street": "Cedar Lane",
      "house_number": "498",
      "city": "Lexington",
      "zipcode": "25599"
    }
  },
  {
    "fname": "Elizabeth",
    "lname": "Brown",
    "age": 67,
    "address": {
      "street": "Laurel Street",
      "house_number": "52",
      "city": "Bristol",
      "zipcode": "97344"
    }
  },
  {
    "fname": "Joseph",
    "lname": "White",
    "age": 48,
    "address": {
      "street": "Spruce Street",
      "house_number": "320",
      "city": "Georgetown",
      "zipcode": "35206"
    }
  },
  {
    "fname": "Grace",
    "lname": "Jones",
    "age": 41,
    "address": {
      "street": "Oak Avenue",
      "house_number": "90",
      "city": "Fairview",
      "zipcode": "61593"
    }
  },
  {
    "fname": "Benjamin",
    "lname": "Jackson",
    "age": 21,
    "address": {
      "street": "Poplar Lane",
      "house_number": "207",
      "city": "Shelbyville",
      "zipcode": "73307"
    }
  },
  {
    "fname": "Alexander",
    "lname": "Walker",
    "age": 56,
    "address": {
      "street": "Evergreen Road",
      "house_number": "274",
      "city": "Dayton",
      "zipcode": "67084"
    }
  },
  {
    "fname": "Chloe",
    "lname": "Gonzalez",
    "age": 32,
    "address": {
      "street": "Sycamore Court",
      "house_number": "309",
      "city": "Fairview",
      "zipcode": "64392"
    }
  },
  {
    "fname": "David",
    "lname": "Clark",
    "age": 71,
    "address": {
      "street": "Cedar Lane",
      "house_number": "188",
      "city": "Riverview",
      "zipcode": "22496"
    }
  },
  {
    "fname": "Victoria",
    "lname": "Thompson",
    "age": 52,
    "address": {
      "street": "Hickory Drive",
      "house_number": "278",
      "city": "Georgetown",
      "zipcode": "14599"
    }
  },
  {
    "fname": "Justin",
    "lname": "Garcia",
    "age": 28,
    "address": {
      "street": "Birch Way",
      "house_number": "289",
      "city": "Greenville",
      "zipcode": "63481"
    }
  },
  {
    "fname": "James",
    "lname": "Smith",
    "age": 60,
    "address": {
      "street": "Oak Avenue",
      "house_number": "62",
      "city": "Salem",
      "zipcode": "32988"
    }
  },
  {
    "fname": "Isabella",
    "lname": "Lewis",
    "age": 30,
    "address": {
      "street": "Willow Road",
      "house_number": "428",
      "city": "Clinton",
      "zipcode": "14445"
    }
  },
  {
    "fname": "Matthew",
    "lname": "Lee",
    "age": 46,
    "address": {
      "street": "Laurel Street",
      "house_number": "329",
      "city": "Arlington",
      "zipcode": "29556"
    }
  }
]);