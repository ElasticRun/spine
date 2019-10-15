#!/bin/sh

cd /home/frappe/frappe-bench/apps/$CI_PROJECT_NAME

echo "PULLING git pull prod-deployment $CI_COMMIT_REF_NAME"
PULL_OUTPUT=$(git pull prod-deployment $CI_COMMIT_REF_NAME)
echo $PULL_OUTPUT

ARE_JSONS_PRESENT=$(echo $PULL_OUTPUT | grep '.json')
echo "ARE_JSONS_PRESENT="
echo $ARE_JSONS_PRESENT

if [ "$ARE_JSONS_PRESENT" = "" ]
then
echo "Nothing to migrate"
else
cd ../..
bench migrate
fi
