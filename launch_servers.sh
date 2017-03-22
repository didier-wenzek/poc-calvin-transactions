TNX_MNG_COUNT=1
PART_COUNT=3

for ID in $(seq 1 $TNX_MNG_COUNT)
do
  xterm -e ./transaction_manager.native $ID $TNX_MNG_COUNT &
done

for ID in $(seq 1 $PART_COUNT)
do
  xterm -e ./partition_server.native $ID $PART_COUNT &
done
