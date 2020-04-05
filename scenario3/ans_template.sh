name=$(basename $0 .sh)
hive -f  $name.sql
