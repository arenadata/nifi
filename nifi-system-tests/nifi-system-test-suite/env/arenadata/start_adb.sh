#!/usr/bin/env bash
# Main
export IFS=","

dbid=2 # dbid=1 is a master segment
segid=0

is_mirrored=$DOCKER_GP_WITH_MIRROR
primary_segments_per_host=$DOCKER_GP_PRIMARY_SEGMENTS_PER_HOST

# Base config
CONFIG="ARRAY_NAME='ADB Cluster for trino-adb testing'
TRUSTED_SHELL=ssh
CHECK_POINT_SEGMENTS=8
ENCODING=UNICODE
SEG_PREFIX=seg
HEAP_CHECKSUM=on
HBA_HOSTNAMES=0
QD_PRIMARY_ARRAY=$DOCKER_GP_MASTER_SERVER~$DOCKER_GP_MASTER_SERVER~6000~/data/gpdata/master/gpseg-1~1~-1
declare PRIMARY_ARRAY=("

# Add primary segments to the config
servers=($DOCKER_GP_SEGMENT_SERVERS)
count=${#servers[@]}
mirror_port=7001
mirror_server_index=0
mirror_config=""
for ((i=0; i<$count; i++));
do
  server=${servers[$i]}
  primary_port=6001
  for (( j=1 ; j<=$primary_segments_per_host ; j++ ));
  do
      echo -e -n "primary: host=$server; dbid=$dbid; segid=$segid, port=$primary_port\n"
      CONFIG+="$server~$server~$primary_port~/data/gpdata/primary/gpseg$segid~$dbid~$segid "

      # Prepare mirrors config start
      mirror_server=${servers[($mirror_server_index) % $count]}
      if [[ "$server" == "$mirror_server" ]]; then
        ((mirror_server_index++))
        mirror_server=${servers[($mirror_server_index) % $count]}
      fi
      ((dbid++))
      echo -e -n "mirror : host=$mirror_server; dbid=$dbid; segid=$segid, port=$mirror_port\n"
      mirror_config+="$mirror_server~$mirror_server~$mirror_port~/data/gpdata/mirror/gpseg$segid~$dbid~$segid "
      ((mirror_server_index++))
      ((mirror_port++))
      # Prepare mirrors config end

      ((dbid++))
      ((segid++))
      ((primary_port++))
  done
done
CONFIG+=")"

# Add mirror segments to the config
if [ -z "$is_mirrored" ] || [ "$is_mirrored" == "true" ]; then
  ((segid--))
  CONFIG+=" declare MIRROR_ARRAY=("
  CONFIG+="$mirror_config"
  CONFIG+=")"
  echo "Run cluster with mirrors. Config is:"
  echo -e "$CONFIG"
else
  echo "Run cluster without mirrors. Config is:"
  echo -e "$CONFIG"
fi

# Set hostname
os_hostname=$(hostname)
if [ -n "$HOSTNAME" ] && [ "$HOSTNAME" != "$os_hostname" ]; then
  echo "****************************************"
  echo "Set hostname inside the docker container"
  echo "****************************************"
  bash -c "hostname  $HOSTNAME"
  echo "Hostname is set to $(hostname)"
fi

if [ "$HOSTNAME" == "$DOCKER_GP_MASTER_SERVER" ] || [ "$HOSTNAME" == "$DOCKER_GP_STANDBY_SERVER" ]; then
  BASH_PROFILE='export PGPORT=6000
  export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
  source /usr/local/greenplum-db-devel/greenplum_path.sh
  export PGBACKREST_CONFIG=/home/gpadmin/pgbackrest.conf
  export PATH_ARENADATA_CONFIGS=/home/gpadmin
  export GP_PLUGIN_PATH=/usr/lib/gpdb/bin
  PATH=\$PATH:/usr/lib/gpdb/bin/:/usr/local/greenplum-db-devel/bin/
  export PATH'
else
  BASH_PROFILE='source /usr/local/greenplum-db-devel/greenplum_path.sh
  export PGBACKREST_CONFIG=/home/gpadmin/pgbackrest.conf
  export PATH_ARENADATA_CONFIGS=/home/gpadmin
  export GP_PLUGIN_PATH=/usr/lib/gpdb/bin
  PATH=\$PATH:/usr/lib/gpdb/bin/:/usr/local/greenplum-db-devel/bin/
  export PATH'
fi

# Run sshd
bash -c "/usr/sbin/sshd"

echo "**********************************"
echo "Get ssh public keys of hosts"
echo "**********************************"
keys=()
max_iterations=10
wait_seconds=3
iterations=0

servers=()
for server in $DOCKER_GP_CLUSTER_HOSTS; do
  servers+=("$server")
done

while true
do
  ((iterations++))
  echo "Get public key. Attempt $iterations"
  status=0
  toremove=()
  for server in "${servers[@]}"
  do
      echo "Get public key for $server"
      key=$(ssh-keyscan $server)
      if [ $? -ne 0 ] ||  [[ $key != *"ssh-rsa"* ]]; then
        echo "Server $server doesn't have the public key yet. We will try again..."
        status=1
        break
      else
        echo "Add key for server $server"
        keys+=("$key")
        toremove+=("$server")
      fi
  done
  if [ $status -eq 0 ]; then
    echo "All ADB servers have public key"
    break
  elif [ "$iterations" -ge "$max_iterations" ]; then
    echo "Error to get public key for some ADB server after $max_iterations tries. Exit from script!"
    exit 1
  else
    for server in "${toremove[@]}"; do
      for i in "${!servers[@]}"; do
        if [[ ${servers[i]} = "$server" ]]; then
          unset 'servers[i]'
        fi
      done
    done
    echo "The following servers don't have the key yet: ${servers[*]}"
    echo "Wait $wait_seconds seconds and try again to get public key"
    sleep $wait_seconds
  fi
done

# Create linux cgroup for GP
echo "**************************"
echo "Create linux cgroup for GP"
echo "**************************"
bash -c "mkdir -p /sys/fs/cgroup/{memory,cpu,cpuset,cpuacct}/gpdb"
bash -c "chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset,cpuacct}/gpdb"
bash -c "chown -R gpadmin:gpadmin /sys/fs/cgroup/{memory,cpu,cpuset,cpuacct}/gpdb"

# Create config files
echo "**********************************************"
echo "Copy keys, set bash profile and create configs"
echo "**********************************************"
for key in "${keys[@]}"
do
  bash -c "echo $key >> /home/gpadmin/.ssh/known_hosts"
done
bash -c "cat /root/.ssh/id_rsa > /home/gpadmin/.ssh/id_rsa && cat /root/.ssh/id_rsa.pub > /home/gpadmin/.ssh/id_rsa.pub && cat /root/.ssh/authorized_keys > /home/gpadmin/.ssh/authorized_keys"
bash -c "echo \"$CONFIG\" > /home/gpadmin/gpdb_src/gpAux/gpdemo/create_cluster.conf"
sudo -H -u gpadmin bash -c "echo \"$BASH_PROFILE\" > /home/gpadmin/.bash_profile && echo \"$BASH_PROFILE\" > /home/gpadmin/.bashrc"
bash -c "ksh -c env | grep ADB | sed 's/^/export /' >> /home/gpadmin/container_env.sh &&
         ksh -c env | grep ADB | sed 's/^/export /' >> /home/gpadmin/.bash_profile"

echo "***********************************"
echo "Remove sudo permission from gpadmin"
echo "***********************************"
bash -c "sed -i 's/gpadmin ALL = NOPASSWD : ALL/# gpadmin ALL = NOPASSWD : ALL/g' /etc/sudoers"

echo "*********************"
echo "Network configuration"
echo "*********************"
bash -c "ip a"

echo "*********************************"
echo "Check ssh connection to the hosts"
echo "*********************************"
max_iterations=20
wait_seconds=5
iterations=0
while true
do
  ((iterations++))
  echo "Check SSH connection. Attempt $iterations"
  status=0
  for server in $DOCKER_GP_CLUSTER_HOSTS
  do
      echo "Check SSH connection to the $server"
      sudo -H -u gpadmin bash -c "ssh $server hostname"
      if ! [ $? -eq 0 ]; then
        echo "Server $server is not available for ssh connection. We will try again..."
        status=1
        break
      fi
  done
  if [ $status -eq 0 ]; then
    echo "All ADB servers are available for SSH connection"
    break
  elif [ "$iterations" -ge "$max_iterations" ]; then
    echo "Error to connect to some ADB server via SSH after $max_iterations tries. Exit from script!"
    exit 1
  else
    echo "Wait $wait_seconds seconds and try again to connect to the servers"
    sleep $wait_seconds
  fi
done

# Install cluster
if [ "$HOSTNAME" == "$DOCKER_GP_MASTER_SERVER" ]; then
  if ! [ -d "/data/gpdata/master/gpseg-1" ]; then
    echo "****************************"
    echo "Run ADB cluster installation"
    echo "****************************"
    sudo -H -u gpadmin bash -c "source /home/gpadmin/.bash_profile &&
        /usr/local/greenplum-db-devel/bin/gpinitsystem -a -I /home/gpadmin/gpdb_src/gpAux/gpdemo/create_cluster.conf -l /home/gpadmin/gpAdminLogs"
    sudo -H -u gpadmin bash -c "source /home/gpadmin/.bash_profile &&
        /usr/local/greenplum-db-devel/bin/psql -d postgres -Atc 'create database adb;' &&
        psql -d adb -Atc 'create extension if not exists gp_pitr;' &&
        psql -d postgres -Atc 'create extension if not exists gp_pitr;' &&
        echo 'host    all     all     0.0.0.0/0       md5' >> /data/gpdata/master/gpseg-1/pg_hba.conf &&
        /usr/local/greenplum-db-devel/bin/psql -d postgres -Atc \"alter role gpadmin password 'gpadmin'\" &&
        gpconfig -c gp_resource_manager -v group &&
        gpstop -aM fast && gpstart -a"

    # Check cluster
    echo "*******************************"
    echo "Check connection to ADB cluster"
    echo "*******************************"
    for i in {1..5}; do
      result="$( sudo -H -u gpadmin bash -c "source /home/gpadmin/.bash_profile && /usr/local/greenplum-db-devel/bin/psql -d postgres -Atc 'SELECT 1;'" )"
      if [ "${result}" == "1" ]; then
        if ! [[ -z "$DOCKER_GP_STANDBY_SERVER" ]]; then
              # Activate standby master server
              echo "******************************"
              echo "Activate standby master server"
              echo "******************************"
              sudo -H -u gpadmin bash -c "source /home/gpadmin/.bash_profile && /usr/local/greenplum-db-devel/bin/gpinitstandby -a -s $DOCKER_GP_STANDBY_SERVER"
        fi
        echo -e "\e[1;32m ######################################\e[0m"
        echo -e "\e[1;32m Fantastic!!! ADB cluster is available!\e[0m"
        echo -e "\e[1;32m ######################################\e[0m"
        break
      else
        echo -e "\e[1;31m ###############################\e[0m"
        echo -e "\e[1;31m Error to connect to ADB cluster\e[0m"
        echo -e "\e[1;31m ###############################\e[0m"
        sleep 2
      fi
    done
  else
    echo "********************"
    echo "Starting ADB cluster"
    echo "********************"
    sudo -H -u gpadmin bash -c "source /home/gpadmin/.bash_profile && gpstart -a"
    for i in {1..5}; do
      result="$( sudo -H -u gpadmin bash -c "source /home/gpadmin/.bash_profile && /usr/local/greenplum-db-devel/bin/psql -d postgres -Atc 'SELECT 1;'" )"
      if [ "${result}" == "1" ]; then
        echo -e "\e[1;32m ######################################\e[0m"
        echo -e "\e[1;32m Fantastic!!! ADB cluster is available!\e[0m"
        echo -e "\e[1;32m ######################################\e[0m"
        break
      else
        echo -e "\e[1;31m ###############################\e[0m"
        echo -e "\e[1;31m Error to connect to ADB cluster\e[0m"
        echo -e "\e[1;31m ###############################\e[0m"
        sleep 2
      fi
    done
  fi
else
    echo "********************"
    echo "Segment is started"
    echo "********************"
fi
if [ "$HOSTNAME" == "$DOCKER_GP_MASTER_SERVER" ]; then
    tail -f /data/gpdata/master/gpseg-1/pg_log/gpdb-*.csv
else
    tail -f /dev/null
fi
