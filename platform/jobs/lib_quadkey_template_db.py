"""lib_quadkey_template_db.py — rebuilt from the 0818 class screenshot.

A quadkey prefix tree ("Quadkey Tree" slide): each node is one quadkey digit,
region names hang on the node where a region's quadkey ends. Looking up a
level-20 quadkey walks down the tree and collects every region it passes,
so one record can belong to several regions (e.g. 台北市 and 台北東區).

Shipped to executors with --py-files (like the course) or via PYTHONPATH.
"""


class Node:
    def __init__(self, digit):
        self.digit = digit
        self.region_names = []
        self.links = [None, None, None, None]

    def add_region(self, region_name):
        if region_name not in self.region_names:
            self.region_names.append(region_name)

    def add_node(self, digit):
        child = Node(digit)
        self.links[digit] = child
        return child


class QuadkeyTemplateDB:
    def __init__(self, csv_files_list):
        self.root = Node(-1)
        self.load_data(csv_files_list)

    def load_data(self, csv_files_list):
        for csv_file in csv_files_list:
            file_handle = open(csv_file, encoding="utf-8")
            is_header = True
            for line in file_handle:
                if is_header:
                    is_header = False
                    continue
                row = line.strip().split(",")
                region_name = row[0]
                quadkey = row[1]
                self.add_quadkey(quadkey, region_name)
            file_handle.close()

    def add_quadkey(self, quadkey, region_name):
        cur = self.root
        for i in quadkey:
            digit = int(i)
            if cur.links[digit] is None:
                cur = cur.add_node(digit)
            else:
                cur = cur.links[digit]
        cur.add_region(region_name)

    def lookup_regions(self, quadkey):
        found = []
        cur = self.root
        for i in quadkey or "":
            cur = cur.links[int(i)]
            if cur is None:
                break
            found.extend(cur.region_names)
        return found
