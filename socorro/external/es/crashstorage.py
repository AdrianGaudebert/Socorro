# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
from threading import Thread
from Queue import Queue
from contextlib import contextmanager

import elasticsearch
from elasticsearch import helpers

from socorro.external.crashstorage_base import CrashStorageBase
from socorro.external.es.index_creator import IndexCreator
from socorro.external.crashstorage_base import CrashIDNotFound
from socorrolib.lib import datetimeutil

from configman import Namespace
from configman.converters import class_converter


#==============================================================================
class ESCrashStorage(CrashStorageBase):
    """This sends processed crash reports to Elasticsearch."""

    required_config = Namespace()
    required_config.add_option(
        'transaction_executor_class',
        default="socorro.database.transaction_executor."
        "TransactionExecutorWithLimitedBackoff",
        doc='a class that will manage transactions',
        from_string_converter=class_converter,
    )

    required_config.elasticsearch = Namespace()
    required_config.elasticsearch.add_option(
        'elasticsearch_class',
        default='socorro.external.es.connection_context.ConnectionContext',
        from_string_converter=class_converter,
        reference_value_from='resource.elasticsearch',
    )

    # This cache reduces attempts to create indices, thus lowering overhead
    # each time a document is indexed.
    indices_cache = set()

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        """Init, you know.
        """

        super(ESCrashStorage, self).__init__(
            config,
            quit_check_callback
        )

        # Ok, it's sane, so let's continue.
        self.es_context = self.config.elasticsearch.elasticsearch_class(
            config=self.config.elasticsearch
        )

        self.transaction = config.transaction_executor_class(
            config,
            self.es_context,
            quit_check_callback
        )

    #--------------------------------------------------------------------------
    def get_index_for_crash(self, crash_date):
        """Return the submission URL for a crash; based on the submission URL
        from config and the date of the crash.
        If the index name contains a datetime pattern (ex. %Y%m%d) then the
        crash_date will be parsed and appended to the index name.
        """

        index = self.config.elasticsearch.elasticsearch_index

        if not index:
            return None
        elif '%' in index:
            # Note that crash_date must be a datetime object!
            index = crash_date.strftime(index)

        return index

    #--------------------------------------------------------------------------
    def save_raw_and_processed(self, raw_crash, dumps, processed_crash,
                               crash_id):
        """This is the only write mechanism that is actually employed in normal
        usage.
        """

        crash_document = {
            'crash_id': crash_id,
            'processed_crash': processed_crash,
            'raw_crash': raw_crash
        }

        self.transaction(
            self._submit_crash_to_elasticsearch,
            crash_document=crash_document
        )

    #--------------------------------------------------------------------------
    @staticmethod
    def reconstitute_datetimes(processed_crash):
        datetime_fields = [
            'submitted_timestamp',
            'date_processed',
            'client_crash_date',
            'started_datetime',
            'startedDateTime',
            'completed_datetime',
            'completeddatetime',
        ]
        for a_key in datetime_fields:
            try:
                processed_crash[a_key] = string_to_datetime(
                    processed_crash[a_key]
                )
            except KeyError:
                # not there? we don't care
                pass

    #--------------------------------------------------------------------------
    def _submit_crash_to_elasticsearch(self, connection, crash_document):
        """Submit a crash report to elasticsearch.
        """
        # Massage the crash such that the date_processed field is formatted
        # in the fashion of our established mapping.
        self.reconstitute_datetimes(crash_document['processed_crash'])

        # Obtain the index name.
        es_index = self.get_index_for_crash(
            crash_document['processed_crash']['date_processed']
        )
        es_doctype = self.config.elasticsearch.elasticsearch_doctype
        crash_id = crash_document['crash_id']

        # Attempt to create the index; it's OK if it already exists.
        if es_index not in self.indices_cache:
            index_creator = IndexCreator(config=self.config)
            index_creator.create_socorro_index(es_index)

        # Submit the crash for indexing.
        try:
            connection.index(
                index=es_index,
                doc_type=es_doctype,
                body=crash_document,
                id=crash_id
            )

        except elasticsearch.exceptions.ElasticsearchException as e:
            self.config.logger.critical(
                'Submission to Elasticsearch failed for %s (%s)',
                crash_id,
                e,
                exc_info=True
            )
            raise


#==============================================================================
class ESCrashBulkProcessedReadStorage(ESCrashStorage):
    # required_config =  Namespace()
    # required_config.add_option(
    #     'target_query',
    #     doc="some way of telling ES what to get",
    #     default='',
    # )
    # required_config.add_option(
    #     'bulk_limit',
    #     doc="max number of crashes at a time",
    #     default=1000,
    # )

    def __init__(self, config, quit_check_callback=None):
        super(ESCrashBulkProcessedReadStorage, self).__init__(config)
        # self.processed_crashes_cache = {}
        self.temporary_storage = Queue()

    #--------------------------------------------------------------------------
    def get_unredacted_processed(self, crash_id):
        try:
            return self.processed_crashes_cache[crash_id]
        except KeyError:
            raise CrashIDNotFound(crash_id)


    #--------------------------------------------------------------------------
    def _get_more_crashes(self):
        # fill self.processed_crashes_cache with the next set from
        # some query of ES.  self.processed_crashes_cache should be
        # a giant dict of processed crashes keyed by crash_id

        try:
            self.processed_crashes_cache = some_es_bulk_fetch_method(
                config.target_query,
                config.bulk_limit,
            )
            return True  # success
        except SomeNoResultsException:
            return False  # nothing left to fetch

    def dl_es_data(  # XXX rename
        self,
        es_index,
        es_doctype,
        date,
        product,
        versions,
        fields,
    ):
        """Return all documents of an index.
        See documentation:
         * https://www.elastic.co/guide/en/elasticsearch/reference/1.4/search-request-scroll.html
         * https://elasticsearch-py.readthedocs.org/en/1.2.0/helpers.html#elasticsearch.helpers.scan
        """
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
                                'processed_crash.product': product
                            }
                        },
                        {
                            'term': {
                                'processed_crash.version': versions
                            }
                        }
                    ]
                }
            }
        }
        print "QUERY"
        from pprint import pprint
        pprint(query)


        with self.es_context() as es_context:
            print repr(es_context)
            print es_context
            res = helpers.scan(
                es_context,
                scroll='1m',  # keep the "scroll" connection open for 1 minute.
                index=es_index,
                doc_type=es_doctype,
                # Don't use `fields=fields` because you'll get an
                # ElasticsearchIllegalArgumentException[field \
                # [processed_crash.json_dump] isn\'t a leaf field];
                # error.
                _source=fields,
                # fields=[
                #     'processed_crash.uuid',
                #     'processed_crash.product',
                #     'processed_crash.version',
                #     'processed_crash.json_dump',
                # ],
                query=query,
            )
            return res
            c=0
            for hit in res:
                print hit.keys()
                # print hit['fields']
                c+=1
                if c> 10:break
                raise Exception

    #--------------------------------------------------------------------------
    def new_crashes(self, date, product, versions, fields):
        print (date, product, versions)

        stream = self.dl_es_data(
            date.strftime('socorro%Y%W'),
            'crash_reports',
            date,
            product,
            versions,
            fields,
        )
        print "DATE", repr(date)
        print "PRODUCT", repr(product)
        print "VERSIONS", repr(versions)
        print "FIELDS", repr(fields)
        print "STREAM"
        print stream
        for each in stream:
            print (each['_source']['processed_crash']['product'], each['_source']['processed_crash']['version'])
            self.temporary_storage.put(
                each['_source']['processed_crash']
            )
            # there is no need to yield a real crash_id.  This class represents
            # a stream of crashes, not a Mapping of crash_ids to crashes.
            # the yield is just a trigger to tell the FTS app to fetch the
            # next item from the queue by calling the 'get_unredacted_processed'
            # method.
            yield 'next_processed_crash'

    #--------------------------------------------------------------------------
    def get_unredacted_processed(self, crash_id_ignored):
        """just pull the next item from the queue"""
        next_ = self.temporary_storage.get()
        print "NEXT_", next
        return next_


from socorrolib.lib.converters import change_default
from socorrolib.lib.datetimeutil import string_to_datetime
from socorro.external.crashstorage_base import Redactor


#==============================================================================
class ESCrashStorageRedactedSave(ESCrashStorage):
    required_config = Namespace()
    required_config.namespace('es_redactor')
    required_config.es_redactor.add_option(
        name="redactor_class",
        doc="the name of the class that implements a 'redact' method",
        default='socorro.external.crashstorage_base.Redactor',
        from_string_converter=class_converter,
    )
    required_config.es_redactor.forbidden_keys = change_default(
        Redactor,
        "forbidden_keys",
        "json_dump, "
        "upload_file_minidump_flash1.json_dump, "
        "upload_file_minidump_flash2.json_dump, "
        "upload_file_minidump_browser.json_dump"
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        """Init, you know.
        """
        super(ESCrashStorageRedactedSave, self).__init__(
            config,
            quit_check_callback
        )
        self.redactor = config.es_redactor.redactor_class(config.es_redactor)
        self.config.logger.warning(
            "beware, this crashstorage class is destructive to the "
            "processed crash - if you're using a polycrashstore you may "
            "find the modified processed crash saved to the other crashstores"
        )

    #--------------------------------------------------------------------------
    def save_raw_and_processed(self, raw_crash, dumps, processed_crash,
                               crash_id):
        """This is the only write mechanism that is actually employed in normal
        usage.
        """
        self.reconstitute_datetimes(processed_crash)
        self.redactor.redact(processed_crash)

        super(ESCrashStorageRedactedSave, self).save_raw_and_processed(
            raw_crash,
            dumps,
            processed_crash,
            crash_id
        )


#==============================================================================
class QueueWrapper(Queue):
    """this class allows a queue to be a standin for a connection to an
    external resource.  The queue then becomes compatible with the
    TransactionExecutor classes"""

    #--------------------------------------------------------------------------
    def commit(self):
        pass

    #--------------------------------------------------------------------------
    def rollback(self):
        pass

    #--------------------------------------------------------------------------
    def close(self):
        pass

    #--------------------------------------------------------------------------
    @contextmanager
    def __call__(self):
        yield self


#==============================================================================
class QueueContextSource(object):
    """this class allows a queue to be a standin for a connection to an
    external resource.  The queue then becomes compatible with the
    TransactionExecutor classes"""
#--------------------------------------------------------------------------
    def __init__(self, a_queue):
        self.queue = a_queue

#--------------------------------------------------------------------------
    @contextmanager
    def __call__(self):
        yield self.queue

#--------------------------------------------------------------------------
    operational_exceptions = ()
    conditional_exceptions = ()


#------------------------------------------------------------------------------
def _create_bulk_load_crashstore(base_class):

    #==========================================================================
    class ESBulkClassTemplate(base_class):
        required_config = Namespace()
        required_config.add_option(
            'items_per_bulk_load',
            default=500,
            doc="the number of crashes that triggers a flush to ES"
        )
        required_config.add_option(
            'maximum_queue_size',
            default=512,
            doc='the maximum size of the internal queue'
        )

        #----------------------------------------------------------------------
        def __init__(self, config, quit_check_callback=None):
            super(ESBulkClassTemplate, self).__init__(
                config,
                quit_check_callback
            )

            self.task_queue = QueueWrapper(config.maximum_queue_size)
            self.consuming_thread = Thread(
                name="ConsumingThread",
                target=self._consuming_thread_func
            )

            # overwrites original
            self.transaction = config.transaction_executor_class(
                config,
                QueueContextSource(self.task_queue),
                quit_check_callback
            )
            self.done = False
            self.consuming_thread.start()

        #----------------------------------------------------------------------
        def _submit_crash_to_elasticsearch(self, queue, crash_document):
            # Massage the crash such that the date_processed field is formatted
            # in the fashion of our established mapping.
            # First create a datetime object from the string in the crash
            # report.
            self.reconstitute_datetimes(crash_document['processed_crash'])

            # Obtain the index name.
            es_index = self.get_index_for_crash(
                crash_document['processed_crash']['date_processed']
            )
            es_doctype = self.config.elasticsearch.elasticsearch_doctype
            crash_id = crash_document['crash_id']

            # Attempt to create the index; it's OK if it already exists.
            if es_index not in self.indices_cache:
                index_creator = IndexCreator(config=self.config)
                index_creator.create_socorro_index(es_index)

            action = {
                '_index': es_index,
                '_type': es_doctype,
                '_id': crash_id,
                '_source': crash_document,
            }
            queue.put(action)

        #----------------------------------------------------------------------
        def _consumer_iter(self):
            while True:
                try:
                    crash_document = self.task_queue.get()
                except Exception:
                    self.config.logger.critical(
                        "Failure in ES Bulktask_queue",
                        exc_info=True
                    )
                    crash_document = None
                if crash_document is None:
                    self.done = True
                    break
                yield crash_document  # execute the task

        #----------------------------------------------------------------------
        def close(self):
            self.task_queue.put(None)
            self.consuming_thread.join()

        #----------------------------------------------------------------------
        def _consuming_thread_func(self):  # execute the bulk load
            with self.es_context() as es:
                try:
                    elasticsearch.helpers.bulk(
                        es,
                        self._consumer_iter(),
                        chunk_size=self.config.items_per_bulk_load
                    )
                except Exception:
                    self.config.logger.critical(
                        "Failure in ES elasticsearch.helpers.bulk",
                        exc_info=True
                    )

    return ESBulkClassTemplate


#==============================================================================
ESBulkCrashStorage = _create_bulk_load_crashstore(ESCrashStorage)


#==============================================================================
ESBulkCrashStorageRedactedSave = _create_bulk_load_crashstore(
    ESCrashStorageRedactedSave
)


#==============================================================================
ESCrashStorageNoStackwalkerOutput = ESCrashStorageRedactedSave
