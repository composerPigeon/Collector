#!/bin/bash
set -euC

if [ -f /import/done ]; then
    echo "Skip import process"
    return
fi

echo "Start the database deletion process"
rm -rf /data/databases
rm -rf /data/transactions
echo "Complete the database deletion process"

echo "Start the data import process"
bin/neo4j-admin database import full sales \
  --nodes=/import/customers.csv \
  --nodes=/import/addresses.csv \
  --relationships=/import/customer_has_address.csv
echo "Complete the data import process"

touch /import/done
echo "Start ownership change"
chown -R neo4j:neo4j /data
chown -R neo4j:neo4j /logs
echo "Complete ownership change"