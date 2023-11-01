#!/bin/bash

# Reference: https://sharats.me/posts/shell-script-best-practices/

# Exit if a command fails
set -o errexit

# Throw error when accessing an unset variable
set -o nounset

# Enable debug mode with TRACE=1 ./bench.sh
if [[ "${TRACE-0}" == "1" ]]; then
    set -o xtrace
fi

# Change to directory of this script
cd "$(dirname "$0")"

###############################################
###########   WORKLOAD PARAMETERS   ###########
###############################################

keylen_arr=(64)
nkeys_arr=(50000000)
nqrys_arr=(5000000)
kdist_arr=("kuniform" "knormal")
qdist_arr=("quniform" "quniform")
minrange_arr=(2 2)
maxrange_arr=(32 512)
pqratio_arr=(0.0)
pnratio_arr=(0.0)

echo -e "Running RocksDB Benchmark"

###############################################
############   FILTER PARAMETERS   ############
###############################################

### Oasis
membudg_arr=(8 10 12 14 16)
block_sz=150

################################################################

REPO_DIR="$(pwd)"
ROCKS_DIR="$REPO_DIR/../.."
OASIS_DIR="$ROCKS_DIR/Oasis"
SOSD_DIR="$OASIS_DIR/benchmark/workloads/SOSD/"
WORKL_BIN="$ROCKS_DIR/build/bin/workload_gen"
EXP_BIN="$ROCKS_DIR/build/range_filter_exp/filter_experiment"

# Create result folder - if folder exists then exit
res_folder="$REPO_DIR/rocks_result/$1"
if [ ! -e "$res_folder" ]; then
    mkdir -p "$res_folder"
else
    echo "Result folder already exists! Please try again with a valid result folder name."
    exit -1
fi

# Create file that indexes the information of the generated experiment result files
index_file="$res_folder/index.txt"
touch $index_file

# Create result csv
res_csv="$res_folder/results.csv"
touch $res_csv

SCRATCH_DIR="$ROCKS_DIR/tmp/A"
DATA_DIR="$ROCKS_DIR/tmp/B"
# Create directories if they don't exist
mkdir -p "$DATA_DIR"
mkdir -p "$SCRATCH_DIR"

fetch_data() {

    path="$DATA_DIR/${nkeys// /_}/${nqrys// /_}/${minrange// /_}/${maxrange// /_}/${kdist// /_}/${qdist// /_}/${pqratio// /_}/${pnratio// /_}"

    if [ ! -e "$path/my_data" ]; then
        echo "Generating data in $path"
        mkdir -p "$path" && cd "$path"

        $WORKL_BIN "$SOSD_DIR" "$nkeys" "$nqrys" "$minrange" "$maxrange" "$kdist" "$qdist" "$pqratio" "$pnratio"
    else
        echo "Copying data from $path"
    fi

    cp -r "$path/my_data" $expdir/my_data
}

experiment() {
    cd "$expdir"
    touch ./experiment_result
    echo -e "### FILE INFO ###" >>./experiment_result
    echo -e "\tResult Folder:\t$res_folder" >>./experiment_result
    echo -e "\tFile Counter:\t$filecnt\n\n" >>./experiment_result

    echo -e "### BEGIN SYSTEM EXPERIMENT ###" >>./experiment_result

    echo -e "\n\n### BEGIN EXPERIMENT DESCRIPTION ###" >>./experiment_result
    echo -e "\tFilter Name:\t$filter" >>./experiment_result
    echo -e "\tNumber of Keys:\t$nkeys" >>./experiment_result
    echo -e "\tKey Length:\t$keylen" >>./experiment_result
    echo -e "\tNumber of Queries:\t$nqrys" >>./experiment_result
    echo -e "\tKey Distribution:\t$kdist" >>./experiment_result
    echo -e "\tQuery Distribution:\t$qdist" >>./experiment_result
    echo -e "\tMinimum Range Size:\t$minrange" >>./experiment_result
    echo -e "\tMaximum Range Size:\t$maxrange" >>./experiment_result
    echo -e "\tPoint / Range Query Ratio:\t$pqratio" >>./experiment_result
    echo -e "\tPositive / Negative Query ratio:\t$pnratio" >>./experiment_result

    fetch_data
    cd "$expdir"

    # Print experiment parameters to index file and result CSV
    out_file="$filter-$filecnt"
    echo -e "############# $out_file #############" >>$index_file
    echo -e "NKeys: $nkeys; NQueries: $nqrys; KLen: $keylen" >>$index_file
    echo -e "KDist: $kdist; QDist: $qdist" >>$index_file
    echo -e "MinRange: $minrange; MaxRange: $maxrange" >>$index_file
    echo -e "P-Q ratio: $pqratio; +/- Q ratio: $pnratio" >>$index_file

    config="$filter-${nkeys// /_}-${keylen// /_}-${nqrys// /_}-${minrange// /_}-${maxrange// /_}-${pqratio// /_}-${kdist// /_}-${qdist// /_}-${pnratio// /_}"
    if [ $filter = "Oasis" ]; then
        config+="-${membudg}-${block_sz}"
        echo -e "BPK: $membudg; BlockSZ: $block_sz" >>$index_file
    elif [ $filter = "OasisPlus" ]; then
        config+="-${membudg}-${block_sz}"
        echo -e "BPK: $membudg; BlockSZ: $block_sz" >>$index_file
    fi

    echo -ne "$filter,$nkeys,$nqrys,$keylen,$kdist,$qdist,$minrange,$maxrange,$pqratio,$pnratio,$membudg,$block_sz," >>$res_csv

    echo -e "\n" >>$index_file
    echo "Running $out_file: " $config

    if [ $filter = "Oasis" ]; then
        echo -e "\tLRF Bits-per-Key:\t$membudg" >>./experiment_result
        echo -e "\tLRF Elements-per-Block:\t$block_sz" >>./experiment_result
        echo -e "### END EXPERIMENT DESCRIPTION ###\n\n" >>./experiment_result
        $EXP_BIN "$filter" "$res_csv" "$membudg" "$block_sz" >>./experiment_result

    elif [ $filter = "OasisPlus" ]; then
        echo -e "\tLRF Bits-per-Key:\t$membudg" >>./experiment_result
        echo -e "\tLRF Elements-per-Block:\t$block_sz" >>./experiment_result
        echo -e "### END EXPERIMENT DESCRIPTION ###\n\n" >>./experiment_result
        $EXP_BIN "$filter" "$res_csv" "$membudg" "$block_sz" "$maxrange" >>./experiment_result
    fi

    # set -

    echo $out_file " - Complete "
    cp ./experiment_result "$res_folder/$filter/$out_file.txt"

    cp ./db/LOG LOG
    rm -rf ./db/
    rm -rf ./my_data/
    rm ./experiment_result
}

# Oasis Experiments
filter="Oasis"
file_cnt=1
mkdir "$res_folder/$filter"
for nkeys in "${nkeys_arr[@]}"; do
    for keylen in "${keylen_arr[@]}"; do
        for nqrys in "${nqrys_arr[@]}"; do
            for i in "${!kdist_arr[@]}"; do
                for j in "${!minrange_arr[@]}"; do
                    for pqratio in "${pqratio_arr[@]}"; do
                        for pnratio in "${pnratio_arr[@]}"; do
                            for membudg in "${membudg_arr[@]}"; do

                                kdist="${kdist_arr[$i]}"
                                qdist="${qdist_arr[$i]}"
                                minrange="${minrange_arr[$j]}"
                                maxrange="${maxrange_arr[$j]}"

                                printf -v filecnt "%03d" $file_cnt
                                expdir=$(mktemp -d $SCRATCH_DIR/$filter.XXXXXXX)
                                experiment
                                rm -rf $expdir
                                ((file_cnt++))
                            done
                        done
                    done
                done
            done
        done
    done
done

# OasisPlus Experiments
filter="OasisPlus"
file_cnt=1
mkdir "$res_folder/$filter"
for nkeys in "${nkeys_arr[@]}"; do
    for keylen in "${keylen_arr[@]}"; do
        for nqrys in "${nqrys_arr[@]}"; do
            for i in "${!kdist_arr[@]}"; do
                for j in "${!minrange_arr[@]}"; do
                    for pqratio in "${pqratio_arr[@]}"; do
                        for pnratio in "${pnratio_arr[@]}"; do
                            for membudg in "${membudg_arr[@]}"; do

                                kdist="${kdist_arr[$i]}"
                                qdist="${qdist_arr[$i]}"
                                minrange="${minrange_arr[$j]}"
                                maxrange="${maxrange_arr[$j]}"

                                printf -v filecnt "%03d" $file_cnt
                                expdir=$(mktemp -d $SCRATCH_DIR/$filter.XXXXXXX)
                                experiment
                                rm -rf $expdir
                                ((file_cnt++))
                            done
                        done
                    done
                done
            done
        done
    done
done
