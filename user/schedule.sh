#!/bin/bash


if [ -n $1 ]
then
    if [[ $1 == "middle_dump" ]]
    then
        echo $1
        python dump_user_action.py
    elif [[ $1 == "short_category_preference" ]]
    then
        echo $1
    elif [[ $1 == "short_brand_preference" ]]
    then
        echo $1
    elif [[ $1 == "help" || $# != 1 ]]
    then
        echo "usage: sh schedule.sh middle_dump|category_preference|brand_preference"
    fi
fi
