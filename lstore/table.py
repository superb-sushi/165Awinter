from lstore.index import Index
from time import time

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2
TIMESTAMP_COLUMN = 3

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # RID -> (page, slot) (Where is the record 'RID' located?)
        self.index = Index(self) # Which RIDs have a specific value on a specific column?
        self.merge_threshold_pages = 20  # The threshold to trigger a merge; functionaly implemented in Index Class
        self.page_ranges = []  # List of Page Ranges
        pass

    def __merge(self):
        print("merge is happening")
        pass