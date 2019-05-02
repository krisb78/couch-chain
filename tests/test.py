# coding=utf-8

import unittest


from .processors.base import BaseChangesProcessorTestCase
from .processors.base import BaseDocChangesProcessorTestCase
from .processors.base import BaseDocWithSeqChangesProcessorTestCase
from .processors.couchdb import SimpleCouchdbChangesProcessorTestCase
from .processors.es import SimpleESChangesProcessorTestCase


if __name__ == '__main__':
    unittest.main()
