from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids: 
                return False

            rid = rids[0]
            # self.table.invalidate_record(rid) # not yet implemented
            self.table.index.remove(self.table.key, primary_key, rid)
            return True
        except Exception: 
            return False
    #Delete record when a given primary key. 

        
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # schema_encoding = '0' * self.table.num_columns
        try: 
            if len(columns) != self.table.num_columns: 
                print("mismatch in # of cols")
                return False
            
            primary_key = columns[self.table.key]

            if primary_key is None:
                print("PK not provided")
                return False
        
            if self.table.index.locate(self.table.key, primary_key):
                print("PK already exists")
                return False
            
            rid = self.table.insert_base_record(columns)
            self.table.index.add(self.table.key, primary_key, rid)
            return True
            
        except Exception as e:
            print(e)
            return False
    #to insert a new record.
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        try: 
            rids = self.table.index.locate(search_key_index, search_key)
            results = []

            for rid in rids: 
                if self.table.is_deleted(rid):
                    continue
                
                record_columns = []
                for i, project in enumerate(projected_columns_index):
                    if project == 0: 
                        record_columns.append(None)
                    else: 
                        value = self.table.read_latest(rid, i)
                        record_columns.append(value)
                results.append(Record(rid, record_columns))
                print("HI!")
            return results
        except Exception: 
            print("Fail")
            return False
    #To select the most latest version of a matching record. 


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try: 
            rids = self.table.index.locate(search_key_index, search_key)
            results = []

            for rid in rids: 
                if self.table.is_deleted(rid):
                    continue
                record_columns = []
                for i, project in enumerate(projected_columns_index): 
                    if project == 0: 
                        record_columns.append(None)
                    else: 
                        value = self.table.read_version(rid, i, relative_version)
                        record_columns.append(value)
                results.append(Record(rid, record_columns))
            return results
        except Exception: 
            return False

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try: 
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids: 
                return False
            
            base_rid = rids[0]
            self.table.append_tail_record(base_rid, columns)

            # No need implementation for updates to PK of a record as of now
            # for i, value in enumerate(columns):
            #     if value is not None and i == self.table.key: 
            #         self.table.index.remove(primary_key, base_rid)
            #         self.table.index.add(value, base_rid)
            return True
        except Exception: 
            return False
    #To update a record by appending a tail record

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try: 
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids: 
                return False
            
            total = 0
            found = False
            for rid in rids: 
                if self.table.is_deleted(rid):
                     continue
                value = self.table.read_latest(rid, aggregate_column_index)
                total += value 
                found = True
            return total if found else False
        
        except Exception: 
            return False 

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try: 
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids: 
                return False
            
            total = 0
            found = False
            for rid in rids:
                if self.table.is_deleted(rid): 
                    continue
                value = self.table.read_version(rid, aggregate_column_index, relative_version)
                total += value 
                found = True
            return total if found else False
        except Exception: 
            return False

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
