
VALUE_SIZE = 8
PAGE_SIZE = 4096
MAX_RECORDS = PAGE_SIZE // VALUE_SIZE

class Page: #Physical part of memory

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.limit = MAX_RECORDS

    def has_capacity(self):
        return self.num_records < MAX_RECORDS

    def write(self, value):
        offset = self.num_records * VALUE_SIZE
        self.data[offset:offset+VALUE_SIZE] = value.to_bytes(VALUE_SIZE, 'little', signed=True)
        self.num_records += 1
        return self.num_records - 1   # return slot

    def update(self, slot, value):
        offset = slot * VALUE_SIZE
        self.data[offset:offset+VALUE_SIZE] = value.to_bytes(VALUE_SIZE, 'little', signed=True)
        return

    def read(self, slot):
        offset = slot * VALUE_SIZE
        value_bytes = self.data[offset:offset+VALUE_SIZE]
        return int.from_bytes(value_bytes, 'little', signed=True)


class BasePage(Page):
    def __init__(self):
        super().__init__()
        pass

class TailPage(Page):
    def __init__(self):
        super().__init__()
        pass

class PageRange:
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.base_pages = [[] for _ in range(num_columns)] # each col is represented by an Array of Pages
        self.tail_pages = [[] for _ in range(num_columns)] # each col is represented by an Array of Pages
        self.num_records = 0 # number of records in this page range, also used as slot for the next record to be inserted
        pass

    def has_capacity(self):
        if not self.base_pages or self.base_pages[-1].num_records >= self.limit:
            return True
        return False
    
    def read_base(self, column_index, slot): # given a column index and slot, read the value from the corresponding base page
        page_id = slot // MAX_RECORDS
        offset = slot % MAX_RECORDS
        page = self.base_pages[column_index][page_id]
        return page.read(offset)
    
    def get_base_page(self, column_index, slot):
        page_id = slot // MAX_RECORDS
        offset = slot % MAX_RECORDS
        page = self.base_pages[column_index][page_id]
        return page

    def insert_base_record(self, values):
        slot = self.num_records
        for col, value in enumerate(values):
            # Check if we need to create a new base page (I.e. no base pages at all, or last base page is at its limit)
            if len(self.base_pages[col]) == 0 or not self.base_pages[col][-1].has_capacity():
                self.base_pages[col].append(BasePage())

            page = self.base_pages[col][-1]
            page.write(value)

        self.num_records += 1
        return slot