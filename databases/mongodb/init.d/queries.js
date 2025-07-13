db = db.getSiblingDB('admin');

db.auth('mongo', 'password');

db = db.getSiblingDB('queries');
db.createCollection('executions');
