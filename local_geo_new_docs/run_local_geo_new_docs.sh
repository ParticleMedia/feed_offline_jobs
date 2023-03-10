# !/bin/bash
set -x
set -e
set -u

bash run_local_geo_new_docs_first_cate.sh
bash run_local_geo_new_docs_second_cate.sh
bash run_local_geo_new_docs_channel.sh

