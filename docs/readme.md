# Global
## Compile
```
mvn package -DskipTests
```
## Store pid
```
echo $! > sqlancer.pid
```
## Kill process
```
kill $(cat sqlancer.pid)
```
## Get process info
```
ps aux | grep sqlancer
```
# Mysql
## Activate mysql conda
```
conda activate sqlancer-env
```
## login mysql
```
mysql -u root -p -P 3307 -h 127.0.0.1
```
## Replay oracle1&2
```
python mysql_replay.py \
    --input  replay_input.log \
    --output replay_output.log

python replay.py \
    --input  replay_input.log \
    --output replay_output.log \
    --oracle oracle2 \
    --seed xxx
```
## Get bug report
```
python3 list_bugs.py logs/mysql/ --save logs/mysql_bug_list.txt

python3 list_bugs_3.py ./logs/mysql --save logs/mysql_bug_list.txt
```
## Backup logs
```
./mysql_backup_logs.sh
```
## Oracle1&2 run
```
nohup java \
  -Xmx4g -Xms512m \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=logs/oom_dump.hprof \
  -Dsqlancer.subset.verbose=false \
  -jar target/sqlancer-2.0.0.jar \
  --num-threads 4 \
  --num-queries 10000000 \
  --num-tries 40 \
  --max-generated-databases 10000000 \
  --timeout-seconds 86400 \
  --host 127.0.0.1 --port 3307 \
  mysql --oracle SUBSET \
  > logs/mysql_run.log 2>&1 &
```
## Oracle3 run
```
nohup java \
  -Xmx8g -Xms8g \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=logs/oom_dump.hprof \
  -Dsqlancer.subset.verbose=false \
  -jar target/sqlancer-2.0.0.jar \
  --num-threads 4 \
  --num-queries 10000000 \
  --num-tries 10 \
  --max-generated-databases 10000000 \
  --timeout-seconds 86400 \
  --host 127.0.0.1 --port 3307 \
  mysql --oracle SUBSET3 \
  > logs/mysql_run.log 2>&1 &
```
## Oracle3 test
```
nohup java \
  -Xmx4g -Xms512m \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=logs/oom_dump.hprof \
  -Dsqlancer.subset.verbose=true \
  -jar target/sqlancer-2.0.0.jar \
  --num-threads 1 \
  --num-queries 1000000 \
  --num-tries 1 \
  --max-generated-databases 1000000 \
  --timeout-seconds 86400 \
  --host 127.0.0.1 --port 3307 \
  mysql --oracle SUBSET3 \
  > logs/mysql_run.log 2>&1 &
```
# MariaDB
## Activate mariadb conda
```
conda activate mariadb-env
```
## Login mariadb
```
mariadb --defaults-file=$HOME/mariadb/my.cnf -u root -p
```
## Test oracle3
```
nohup java \
  -Xmx8g -Xms8g \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=logs/oom_dump.hprof \
  -Dsqlancer.subset.verbose=true \
  -jar target/sqlancer-2.0.0.jar \
  --num-threads 4 \
  --num-queries 10000000 \
  --num-tries 1 \
  --max-generated-databases 10000000 \
  --timeout-seconds 86400 \
  --host 127.0.0.1 --port 3309 \
  mariadb --oracle SUBSET3 \
  > logs/mariadb_run.log 2>&1 &
```
## Get bug report
```
python3 list_bugs_3.py ./logs/mariadb --save logs/mariadb_bug_list.txt
```
## Replay oracle3
```
python mariadb_replay.py \
    --input  replay_input.log \
    --output replay_output.log
```
## Backup logs
```
./mariadb_backup_logs.sh
```
## Run oracle3
```
nohup java \
  -Xmx8g -Xms8g \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=logs/oom_dump.hprof \
  -Dsqlancer.subset.verbose=false \
  -jar target/sqlancer-2.0.0.jar \
  --num-threads 4 \
  --num-queries 10000000 \
  --num-tries 10 \
  --max-generated-databases 10000000 \
  --timeout-seconds 86400 \
  --host 127.0.0.1 --port 3309 \
  mariadb --oracle SUBSET3 \
  > logs/mariadb_run.log 2>&1 &
```