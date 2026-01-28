class Bufferpool:

    def __init__(self):
        # rid -> (page_range_id, page_id, column_id)
        self._rid_map = {}

    def register(self, rid, page_range_id, page_id, column_id):
        """
        Register a mapping from `rid` to a physical location.
        If the rid already exists the mapping is overwritten.
        """
        self._rid_map[rid] = (page_range_id, page_id, column_id)

    def get_location(self, rid):
        """
        Return the location tuple for `rid` or None if unknown.
        """
        return self._rid_map.get(rid)

    def update_location(self, rid, page_range_id=None, page_id=None, column_id=None):
        """
        Update parts of an existing location. Returns True if updated.
        """
        if rid not in self._rid_map:
            return False

        pr, pid, col_id = self._rid_map[rid]
        pr = pr if page_range_id is None else page_range_id
        pid = pid if page_id is None else page_id
        col_id = col_id if column_id is None else column_id
        self._rid_map[rid] = (pr, pid, col_id)
        return True

    def remove(self, rid):
        """
        Remove mapping for `rid`. Returns True if removed.
        """
        return self._rid_map.pop(rid, None) is not None

    def clear(self):
        """
        Clear all mappings
        """
        self._rid_map.clear()

    def __len__(self):
        return len(self._rid_map)
