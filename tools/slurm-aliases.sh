function summarize-user {
    check_time=$(date +%Y-%m-%d -d "$2")
    sacct -S $check_time -u $1 \
        --format=User,Account,Jobname,elapsed,ncpus,AllocTRES,NodeList,JobId,State,SystemCPU,UserCPU -p \
        | column -t -s '|';
}


function modquota {
    echo sacctmgr -i modify account $1 set GrpTRES=gres/gpu=$2 GrpTRESMin=gres/gpu=$3;
    sacctmgr -i modify account $1 set GrpTRES=gres/gpu=$2 GrpTRESMin=gres/gpu=$3;
}

function getquota {
    echo "Assoc Limits"
    echo "-----------------------"
    sacctmgr show assoc \
        User="" Account=$1 \
        format=Account,GrpTRES,GrpTRESMins -p \
            | column -t -s "|"
    printf "\nUsage: \n"
    echo "-----------------------"
    scontrol show assoc_mgr Account=$1 flags=assoc \
        |grep -m 1 'GrpTRESMins' ;
}

function gq { getquota $1;}

function group-quota {
    sudo repquota -gs /share1 \
        | sort -hk 3 \
        | column -t
}

function user-quota {
    sudo repquota -us /share1 \
        | sort -hk 3 \
        | column -t
}


function complain-about {
    scontrol show jobs -o \
        | grep $1 | grep "RUNNING" \
        | grep -e "UserId=[^ ]*" -e "TRES=[^ ]*" -o \
        | awk 'NR%2{printf "%s ",$0;next;}1' \
        | column -t;
}

function attach-cvit {
    USR=$1;
    set -x;
    sacctmgr -i create account parent=cvit name=$USR;
    sacctmgr -i add user $USR account=$USR;
    sacctmgr -i modify user where user=$USR set defaultaccount=$USR;
    sacctmgr -i modify Account $USR set GrpTRES=gres/gpu=1 GrpTRESMin=gres/gpu=600;
    set +x;
}
