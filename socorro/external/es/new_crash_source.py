# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime

from elasticsearch import helpers
from configman import Namespace, RequiredConfig, class_converter


class ESNewCrashSource(RequiredConfig):

    required_config = Namespace()
    required_config.elasticsearch = Namespace()
    required_config.elasticsearch.add_option(
        'elasticsearch_class',
        default='socorro.external.es.connection_context.ConnectionContext',
        from_string_converter=class_converter,
        reference_value_from='resource.elasticsearch',
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, name, quit_check_callback=None):
        self.config = config
        self.es_context = self.config.elasticsearch.elasticsearch_class(
            config=self.config.elasticsearch
        )

    def new_crashes(self, date, product, versions):
        next_day = date + datetime.timedelta(days=1)

        query = {
            'filter': {
                'bool': {
                    'must': [
                        {
                            'range': {
                                'processed_crash.date_processed': {
                                    'gte': date.isoformat(),
                                    'lte': next_day.isoformat(),
                                }
                            }
                        },
                        {
                            'term': {
                                'processed_crash.product': product.lower()
                            }
                        },
                        {
                            'terms': {
                                'processed_crash.version': [
                                    x.lower() for x in versions
                                ]
                            }
                        }
                    ]
                }
            }
        }

        es_index = date.strftime(self.config.elasticsearch.elasticsearch_index)
        es_doctype = self.config.elasticsearch.elasticsearch_doctype

        with self.es_context() as es_context:
            res = helpers.scan(
                es_context,
                scroll='1m',  # keep the "scroll" connection open for 1 minute.
                index=es_index,
                doc_type=es_doctype,
                fields=['crash_id'],
                query=query,
            )
            for hit in res:
                yield hit['fields']['crash_id'][0]
