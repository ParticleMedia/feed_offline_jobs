# !/bin/bash
set -x
set -e
set -u

bash run_local_cross_geo_first_cate.sh

bash run_local_cross_geo_second_cate.sh

bash run_local_cross_geo_channel.sh
