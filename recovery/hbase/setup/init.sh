echo 'Starting HBase...'
start-hbase.sh &
hbase thrift start -p 9090 &

echo 'Waiting for HBase to be up...'
while ! echo 'status' | hbase shell &>/dev/null; do sleep 5; done

sleep 10

echo 'Creating tables...'
# if [ ! -f table.blocks ]; then
#   echo "create 'blocks', 'x'" | hbase shell
#   touch table.blocks
# fi

# if [ ! -f table.entries ]; then
#   echo "create 'entries', 'x'" | hbase shell
#   touch table.entries
# fi

function create_table_with_disabled_split_policy() {
  TABLE_NAME=$1
  if ! echo "list" | hbase shell | grep -q "$TABLE_NAME"; then
    echo "create '$TABLE_NAME', {NAME => 'x'}, {SPLIT_POLICY => 'org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy'}" | hbase shell
    touch "table.$TABLE_NAME"
    echo "Table '$TABLE_NAME' created."
  else
    echo "Table '$TABLE_NAME' already exists."
  fi
}

# Create tables with disabled region split policy
echo 'Creating tables with disabled region split policy...'
create_table_with_disabled_split_policy "blocks"
create_table_with_disabled_split_policy "entries"

echo 'Create tables with normal region split policy...'
if [ ! -f table.tx ]; then
  echo "create 'tx', 'x'" | hbase shell
  touch table.tx
fi

if [ ! -f table.tx-by-addr ]; then
  echo "create 'tx-by-addr', 'x'" | hbase shell
  touch table.tx-by-addr
fi

if [ ! -f table.tx_full ]; then
  echo "create 'tx_full', 'x'" | hbase shell
  touch table.tx_full
fi



echo 'Tables created successfully...'

touch /tmp/hbase_ready
wait