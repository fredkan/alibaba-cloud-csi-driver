#!/bin/sh
echo "Starting deploy oss globalpath...."

ossfsVer="1.80.6"
# install OSSFS
mkdir -p /host/etc/csi-tool/
if [ ! `nsenter --mount=/proc/1/ns/mnt which ossfs` ]; then
    echo "First install ossfs...."
    cp /root/ossfs_${ossfsVer}_centos7.0_x86_64.rpm /host/etc/csi-tool/
    nsenter --mount=/proc/1/ns/mnt yum localinstall -y /etc/csi-tool/ossfs_${ossfsVer}_centos7.0_x86_64.rpm
fi

## install/update csi connector
updateConnector="true"
if [ ! -f "/host/etc/csi-tool/csiplugin-connector" ];then
    mkdir -p /host/etc/csi-tool/
    echo "mkdir /etc/csi-tool/ directory..."
else
    updateConnector="false"
fi

if [ "$updateConnector" = "true" ]; then
    echo "copy csiplugin-connector...."
    cp /bin/csiplugin-connector /host/etc/csi-tool/csiplugin-connector
    chmod 755 /host/etc/csi-tool/csiplugin-connector
fi


# install/update csiplugin connector service
updateConnectorService="true"
if [ -f "/host/usr/lib/systemd/system/csiplugin-connector.service" ];then
    updateConnectorService="false"
fi

if [ "$updateConnectorService" = "true" ]; then
    echo "install csiplugin connector service...."
    cp /bin/csiplugin-connector.service /host/usr/lib/systemd/system/csiplugin-connector.service
    nsenter --mount=/proc/1/ns/mnt systemctl daemon-reload
fi

rm -rf /var/log/alicloud/connector.pid
nsenter --mount=/proc/1/ns/mnt systemctl enable csiplugin-connector.service
nsenter --mount=/proc/1/ns/mnt systemctl restart csiplugin-connector.service

# start daemon
if [ "$OSS_BUCKET" = "" ]; then
    echo "bucket is empty"
    exit 1
fi

if [ "$OSS_PATH" = "" ]; then
    OSS_PATH=/
fi

if [ "$AK_ID" = "" ]; then
    echo "AK ID is empty"
    exit 1
fi

if [ "$AK_SEC" = "" ]; then
    echo "AK Secret is empty"
    exit 1
fi

localPath=$MNTPATH
if [ "$MNTPATH" = "" ]; then
    echo "MNTPATH is empty, use default"
    localPath="/mnt/ossfs/$OSS_BUCKET"
fi

if [ "$OSS_EP" = "" ]; then
    regionid=`curl http://100.100.100.200/latest/meta-data/region-id`
    OSS_EP="oss-"$regionid"-internal.aliyuncs.com"
    echo "Oss endpoint is $OSS_EP...."
fi


echo "$OSS_BUCKET:$AK_ID:$AK_SEC" > /host/etc/passwd-ossfs
chmod 600 /host/etc/passwd-ossfs

ossmntline=`nsenter --mount=/proc/1/ns/mnt findmnt $localPath | grep fuse.ossfs | grep -v grep | wc -l`

if [ "$ossmntline" = "0" ]; then
    echo "Do ossfs Mount: Bucket($OSS_BUCKET), Path($OSS_PATH), LocalPath($localPath), url($OSS_OPTIONS)"
    nsenter --mount=/proc/1/ns/mnt mkdir -p $localPath
    echo "systemd-run --scope -- /usr/local/bin/ossfs $OSS_BUCKET:$OSS_PATH $localPath -ourl=$OSS_EP $OSS_OPTIONS" | ncat -U /host/etc/csi-tool/connector.sock -w 5
fi

while true;
do
  sleep 1000;
done