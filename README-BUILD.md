# Maven
mvn -q -DskipTests package


#
java ^
 -Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled -XX:MaxDirectMemorySize=1g -XX:+AlwaysPreTouch ^
 -Ddir=./outbox-soak-data ^
 -DreaderOnly=false ^
 -DdurationSec=3600 ^
 -DwarmupSec=10 ^
 -DtargetQps=40000 ^
 -Dwindow=4096 ^
 -Dval=128 ^
 -DsegBytes=268435456 ^
 -Dpartitions=1 ^
 -DconsumerPartition=0 ^
 -DdurableAck=true ^
 -DsaveEveryN=20000 ^
 -DpruneEveryN=60000 ^
 -DpruneSafety=0 ^
 -DcompactEverySec=20 ^
 -DlatCap=80000 ^
 -cp "target/classes;target/test-classes" aodb.OutboxSoak





java -Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled -XX:MaxDirectMemorySize=1g -XX:+AlwaysPreTouch -Ddir=./outbox-soak-data -DreaderOnly=false -DdurationSec=3600 -DwarmupSec=10 -DtargetQps=20000 -Dwindow=4096 -Dval=128 -DsegBytes=268435456 -Dpartitions=1 -DconsumerPartition=0 -DdurableAck=true -DsaveEveryN=20000 -DpruneEveryN=60000 -DpruneSafety=1 -DcompactEverySec=20 -DlatCap=80000 -cp "target/classes;target/test-classes" aodb.OutboxSoak




