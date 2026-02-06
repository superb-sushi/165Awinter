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
        self.merge_threshold_pages = 20 # The threshold to trigger a merge; functionally implemented in Index Class

    def allocate_rid(self):
        self.rid_counter += 1
        return self.rid_counter
    
    def get_active_page_range(self):
        if len(self.page_ranges) == 0 or not self.page_ranges[-1].has_capacity():
            self.page_ranges.append(PageRange(self.num_columns))

        return self.page_ranges[-1]

    def insert_record(self, *columns):
        rid = self.allocate_rid()
        curr_page_range = self.get_active_page_range()
        values = [
            rid,
            0,              # indirection col
            0,              # schema encoding
            int(time()),
            *columns
        ]
        
        slot = curr_page_range.insert_base_record(values)
        self.page_directory[rid] = (curr_page_range, slot)
        # self.index.insert(columns, rid)
        return rid
    
    def read_latest(self, rid, column_index):
        page_range, slot = self.page_directory[rid]
        return page_range.read_base(column_index, slot)
    
    def invalidate_record(self, rid):
        if rid not in self.page_directory:
            return False

        page_range, slot = self.page_directory[rid]

        try:
            indirection_page = page_range.get_base_page(INDIRECTION_COLUMN, slot)
            indirection_page.update(offset, -1)

            return True
        except Exception:
            return False

    def append_tail_record():
        pass

    def is_deleted(rid):
        if self.read_latest(rid, INDIRECTION_COLUMN) == -1:
            return True
        return False

    def __merge(self):
        print("merge is happening")
        pass