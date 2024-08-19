#!/usr/bin/env bash
set -euo pipefail

MOUNT_DIR=$1
NUMJOBS=$2
USE_DIRECT_IO=$3

# We use fullblock mode to ensure count is accurate in terms of the amount of data read
if [[ "${USE_DIRECT_IO}" == "true" ]]; then
    DD_IFLAG="iflag=fullblock,direct"
else
    DD_IFLAG="iflag=fullblock"
fi

# 10x 1024M blocks, 10GiB. We shouldn't need to read the full 100GiB.
/usr/bin/seq 0 1 ${NUMJOBS} | /usr/bin/awk '{print "j"$0"_100GiB_nochecksum.bin"}' | /usr/bin/parallel -P ${NUMJOBS} dd if=${MOUNT_DIR}/{} of=/dev/null bs=1024M ${DD_IFLAG} count=10
