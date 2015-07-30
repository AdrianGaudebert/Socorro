#!/bin/bash

# elasticsearch integration test for Socorro

source scripts/defaults

echo -n "INFO: setting up environment..."
. socorro-virtualenv/bin/activate >> setup.log 2>&1
if [ $? != 0 ]
then
  fatal 1 "could not activate virtualenv"
fi
export PYTHONPATH=.
echo " Done."

echo -n "INFO: running elasticsearch integration"
# XXXX Not even sure if this works
python socorro/integrationtest/test_elasticsearch_storage_app.py \
    --elasticsearch.elasticsearch_urls=$elasticsearch_urls \
    --elasticsearch.elasticsearch_index=socorro_integration_test_reports \
    --elasticsearch.elasticsearch_emails_index=socorro_integration_test \
    --elasticsearch.elasticsearch_default_index=socorro_integration_test
