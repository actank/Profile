#!/bin/bash


if [ -n $1 ]
then
    if [[ $1 == "long" ]]
        python dump_user_action.py -p $1
        python brand_preference.py -p $1
    then
    elif [[ $1 == "middle" ]]
    then
    elif [[ $1 == "short" ]]
    then
    fi
#
#    if [[ $1 == "dump" ]]
#    then
#        echo $1
#        python dump_user_action.py -p 
#    elif [[ $1 == "short_category_preference" ]]
#    then
#        echo $1
#    elif [[ $1 == "short_brand_preference" ]]
#    then
#        echo $1
#    elif [[ $1 == "help" || $# != 1 ]]
#    then
#        echo "usage: sh schedule.sh dump|category_preference|brand_preference"
#    fi
#
fi
