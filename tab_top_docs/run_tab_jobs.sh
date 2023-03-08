set -e
set -x

if [ -f "/home/services/miniconda3/etc/profile.d/conda.sh" ]; then
    . "/home/services/miniconda3/etc/profile.d/conda.sh"
else
    export PATH="/home/services/miniconda3/bin:$PATH"
fi

conda activate tab_mining

cd /home/services/feed_offline_jobs/tab_top_docs
mkdir -p logs/

py_file=$1
log_file=$2

python $py_file > $log_file 2>&1

conda deactivate