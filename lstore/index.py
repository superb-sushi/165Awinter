"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
METADATA_COLUMNS = 4 # number of metadata columns (rid, indirection, schema encoding, timestamp)

class ColumnIndex:
    def __init__(self):
        self.index = {}  # A simple dictionary that maps 'value' -> {list of RIDs}

    def add(self, value, rid):
        if value not in self.index:
            self.index[value] = set()
        self.index[value].add(rid)

    def remove(self, value, rid):
        if value in self.index:
            self.index[value].discard(rid)
            if not self.index[value]:
                del self.index[value]

    def get_all(self, value):
        return list(self.index.get(value, []))

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None] * (table.num_columns + METADATA_COLUMNS) # an array of lookup structures for EACH column of the table + 4 metadata columns (rid, indirection, schema encoding, timestamp)
        self.create_index(table.key + 4) # "Key column should be indexed by default"
        for i in range(METADATA_COLUMNS):
            self.create_index(i) # The 4 metadata columns should also be indexed by default"

    """
    # Returns the location of all records with the given `value` on column "column"
    """
    def locate(self, column: int, value):
        col_lookup = self.indices[column + METADATA_COLUMNS]
        return col_lookup.get_all(value) # returns list of corresponding RIDs

    """
    # Adds information about the record to the created columns
    """
    # are we separating column and the record, or putting them together?
    def insert(self, column, value, rid):
        col_lookup = self.indices[column + METADATA_COLUMNS]
        return col_lookup.add(value, rid)

    """
    # Deletes information about the record and the created columns
    """
    def delete(self, column, value, rid):
        col_lookup = self.indices[column + METADATA_COLUMNS]
        return col_lookup.delete(value, rid)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column: int):
        rids = []
        for i in range(begin, end + 1):
            rids = rids + self.locate(column + METADATA_COLUMNS, i)
        return rids # returns list of corresponding RIDs 

    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        self.indices[column_number] = ColumnIndex()

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        self.indices[column_number] = None

    def add(self, column, value, rid):
        if self.indices[column] is not None:
            self.indices[column].add(value, rid)
    
    def remove(self, column, value, rid):
        if self.indices[column] is not None:
            self.indices[column].remove(value, rid)
