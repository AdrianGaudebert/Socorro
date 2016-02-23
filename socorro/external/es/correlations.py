# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import json
import os

from configman import Namespace
from configman.converters import class_converter

from socorro.analysis.correlations.correlations_rule_base import (
    CorrelationsStorageBase,
)


HERE = os.path.dirname(os.path.abspath(__file__))


# XXX is there not one of these we can import from somewhere else?
def string_to_list(input_str):
    return [x.strip() for x in input_str.split(',') if x.strip()]


class Correlations(CorrelationsStorageBase):

    required_config = Namespace()

    # required_config.add_option(
    #     'transaction_executor_class',
    #     default="socorro.database.transaction_executor."
    #     "TransactionExecutorWithLimitedBackoff",
    #     doc='a class that will manage transactions',
    #     from_string_converter=class_converter,
    # )

    required_config.elasticsearch = Namespace()
    required_config.elasticsearch.add_option(
        'elasticsearch_class',
        default='socorro.external.es.connection_context.ConnectionContext',
        from_string_converter=class_converter,
        reference_value_from='resource.elasticsearch',
    )

    required_config.add_option(
        'index_creator',
        default='socorro.external.es.index_creator.IndexCreator',
        from_string_converter=class_converter,
        doc='a class that can create Elasticsearch indices',
    )
    required_config.add_option(
        'elasticsearch_correlations_index_settings',
        default='%s/mappings/correlations_index_settings.json' % HERE,
        doc='the file containing the mapping of the indexes receiving '
            'correlations data',
    )
    required_config.add_option(
        'elasticsearch_correlations_index',
        default='socorro_correlations_%Y%m',
        doc='the index that handles data about correlations',
    )

    required_config.add_option(
        'recognized_platforms',
        default='Windows NT, Linux, Mac OS X',
        doc='The kinds of platform names we recognize',
        from_string_converter=string_to_list,
    )

    def __init__(self, config):
        super(Correlations, self).__init__(config)
        self.config = config

        self.es_context = self.config.elasticsearch.elasticsearch_class(
            config=self.config.elasticsearch
        )

        self.indices_cache = set()

    def get_index_for_date(self, date):
        return date.strftime(self.config.elasticsearch_correlations_index)

    def create_correlations_index(self, es_index):
        """Create an index to store correlations. """
        if es_index not in self.indices_cache:
            settings_json = open(
                self.config.elasticsearch_correlations_index_settings
            ).read()
            es_settings = json.loads(settings_json)

            index_creator = self.config.index_creator(self.config)
            print "Create Index", es_index
            index_creator.create_index(es_index, es_settings)
            print "Created Index", es_index
            print
            self.indices_cache.add(es_index)

    def _prefix_to_datetime_date(self, prefix):
        yy = int(prefix[:4])
        mm = int(prefix[4:6])
        dd = int(prefix[6:8])
        return datetime.date(yy, mm, dd)


from pprint import pprint
class CoreCounts(Correlations):

    def store(
        self,
        counts_summary_structure,
        **kwargs
    ):

        # print "counts_summary_structure"
        # pprint(counts_summary_structure)


        date = self._prefix_to_datetime_date(kwargs['prefix'])
        index = self.get_index_for_date(date)
        self.create_correlations_index(index)

        notes = counts_summary_structure['notes']
        product = kwargs['key'].split('_')[0]
        version = kwargs['key'].split('_')[1]
        for platform in counts_summary_structure:
            if platform not in self.config.recognized_platforms:
                # print "%r is not a platform!" % (platform,)
                continue
            count = counts_summary_structure[platform]['count']
            signatures = counts_summary_structure[platform]['signatures']
            if not signatures:
                # print "NO SIGNATURES!"
                continue

            for signature, payload in signatures.items():
                # payload = signatures[signature]
                doc = {
                    'platform': platform,
                    'product': product,
                    'version': version,
                    'count': count,
                    'signature': signature,
                    'payload': payload,
                    'date': date,
                    'key': kwargs['name'],
                    'notes': notes,
                }
                pprint(doc)
                # self.docs.append(doc)
                self.es_context.index(
                    index=index,
                    # see correlations_index_settings.json
                    doc_type='correlations',
                    doc=doc,
                )

    def close(self):
        # XXX Consider, accumulate docs in self.store and here in the close
        # do a bulk save.
        print "Closing CoreCounts"


class InterestingModules(Correlations):

    def store(
        self,
        counts_summary_structure,
        **kwargs  # XXX unpack this here with what we actually need
    ):

        # ss=str(counts_summary_structure)
        # if 1:
        #     print "interesting counts_summary_structure"
        #     pprint(counts_summary_structure)
        #
        #     print "KWARGS"
        #     pprint(kwargs)

        date = self._prefix_to_datetime_date(kwargs['prefix'])
        index = self.get_index_for_date(date)
        self.create_correlations_index(index)

        notes = counts_summary_structure['notes']
        product = kwargs['key'].split('_')[0]
        version = kwargs['key'].split('_')[1]
        os_counters = counts_summary_structure['os_counters']
        for platform in os_counters:
            if not platform:
                continue
            if platform not in self.config.recognized_platforms:
                print "%r is not a platform!" % (platform,)
                continue
            count = os_counters[platform]['count']
            signatures = os_counters[platform]['signatures']
            for signature, payload in signatures.items():
                doc = {
                    'platform': platform,
                    'product': product,
                    'version': version,
                    'count': count,
                    'signature': signature,
                    'payload': payload,
                    'date': date,
                    'key': kwargs['name'],
                    'notes': notes,
                }
                print doc
                self.es_context.index(
                    index=index,
                    # see correlations_index_settings.json
                    doc_type='correlations',
                    doc=doc,
                )

    def close(self):
        # XXX Consider, accumulate docs in self.store and here in the close
        # do a bulk save.
        print "Closing InterestingModules"
