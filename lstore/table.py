from lstore.index import Index
from time import time
from lstore.page import BasePage, TailPage, PageRange

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
        self.num_columns = num_columns
        self.key = key

        self.page_ranges = []
        self.page_directory = {}   # RID -> (page_range, slot)

        self.index = Index(self)

        self.rid_counter = 0
        self.merge_threshold_pages = 20

    def allocate_rid(self):
        self.rid_counter += 1
        return self.rid_counter
    
    def get_active_page_range(self):
        if len(self.page_ranges) == 0 or not self.page_ranges[-1].has_capacity():
            self.page_ranges.append(PageRange(self.num_columns))

        return self.page_ranges[-1]

    def insert_base_record(self, columns):
        page_range = self.get_active_page_range()
        rid = self.allocate_rid()
        slot = page_range.insert_base_record(columns)
        self.page_directory[rid] = (page_range, slot)
        return rid

    def read_latest(self, rid, column_index):
        page_range, slot = self.page_directory[rid]
        return page_range.read_base(column_index, slot)
    
    def __merge(self):
        print("merge is happening")
        pass