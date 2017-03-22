TNX_MNG_COUNT=1
PART_COUNT=3

bin/kafka-topics.sh --partitions 1 --replication-factor 1 --create --topic db_transaction_request --zookeeper localhost
bin/kafka-topics.sh --partitions $TNX_MNG_COUNT --replication-factor 1 --create --topic db_transaction_outcome --zookeeper localhost
bin/kafka-topics.sh --partitions $PART_COUNT --replication-factor 1 --create --topic db_partition_request --zookeeper localhost
bin/kafka-topics.sh --partitions $TNX_MNG_COUNT --replication-factor 1 --create --topic db_transaction_criteria --zookeeper localhost
