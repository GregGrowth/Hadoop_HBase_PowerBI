docker cp mapper.py hadoop-master:/root/
docker cp reducer.py hadoop-master:/root/
docker cp word.txt hadoop-master:/root/
docker cp job01.sh hadoop-master:/root/
docker exec hadoop-slave1 /bin/bash -c './service_slv.sh'
docker exec hadoop-slave2 /bin/bash -c './service_slv.sh'
docker exec hadoop-master /bin/bash -c './job01.sh'