from json.encoder import JSONEncoder
from os import unlink, write

from core.dataset import *
from core.sync.sync_notion import NotionReader, NotionWriter
from core.sync.sync_types import *
from core.sync.sync_tsv import *
from core.sync.sync_json import *
import unittest
from os.path import dirname, exists, join, realpath
import json
from datetime import date, datetime
import asyncio
import copy
import time
import locale
import math

disable_expensive_tests = True

class NotionTest(unittest.TestCase):
    ''' Test Notion reader and writer. '''

    def setUp(self) -> None:
        path = join(dirname(realpath(__file__)), "config.json")
        with open(path, "r") as f:
            config = json.load(f)
        self.secret = config["test_notion_api"]
        self.test_page = config["test_notion_page_parent"]
        self.notion_db = config["test_notion_read_db"]

    def get_test_table(self, reader : NotionReader) -> TableSpec:
        tables = reader.get_tables()

        test_table = tables[0]

        for table in tables:
            if table.parameters["id"].replace('-','') == self.notion_db:
                test_table = table
                break
        
        return test_table

    def test_get_table(self):
        reader = NotionReader(self.secret)
        table = reader.get_table(self.notion_db)
        reader.set_table(table)
        dataset = reader.read_records_sync(10).records

        writer = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/notion_get_table.tsv"}, "notion_get_table"))
        writer.create_table_sync(dataset)

    def test_get_tables(self):
        reader = NotionReader(self.secret)
        reader.get_tables()

    def test_get_records_basic(self):
        reader = NotionReader(self.secret)
        tbl : TableSpec = self.get_test_table(reader)
        reader.set_table(tbl)
        it = reader.read_records_sync(100)
        ds = it.records
        ds.format = DataSetFormat(time_formats = ["%b %d, %Y %I:%M %p"]) # make it pretty-print dates - try commenting this out for default ISO dates
        while it.handle != None:
            it = reader.read_records_sync(100, it)
            ds.add_records(it.records.records)
        
        tsv = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/notion_read_all.tsv"}, "notion_write_all"))
        tsv.create_table_sync(ds)

    async def awaitable_read(self, reader):
        it = await reader.read_records(50)
        i = 1
        ds = it.records
        while it.handle != None:
            it = await reader.read_records(50, it)
            i += 1
            ds.add_records(it.records.records)
        return ds

    def test_find_records(self):
        reader = NotionReader(self.secret)

        tbl : TableSpec = self.get_test_table(reader)
        reader.set_table(tbl)

        ds = reader.find_records("Hanzi", "??????").records

        tsv = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/notion_find_test.tsv"}, "notion_find_test"))
        tsv.create_table_sync(ds)

        ids = reader.find_record_ids("Hanzi", "??????")

    def test_update_record(self):
        nw = NotionWriter(self.secret)
        nr = NotionReader(self.secret)
        tbl : TableSpec = self.get_test_table(nr)
        nr.set_table(tbl)
        nw.set_table(tbl)

        ds = nr.read_records_sync(10).records

        ds.records[0]["Examples & Usage"] = "EXAMPLE UPDATE. Hopefully it's visible..."
        ds.records[0]["Subtags"] = ["time-relative"]
        ds.records[0]["Last Created"] = datetime(1999,2,9,4,0,5)
        ds.records[0]["Tags"] = ["grammar","routine"]

        nw.update_record(ds.records[0], ds, "Hanzi", ds.records[0]["Hanzi"])

    def test_get_records_async(self):
        reader = NotionReader(self.secret)

        tbl : TableSpec = self.get_test_table(reader)

        reader.set_table(tbl)
        ds : DataSet = asyncio.run(self.awaitable_read(reader))
        ds.format = DataSetFormat(time_formats = ["%b %d, %Y %I:%M %p"]) # make it pretty-print dates - try commenting this out for default ISO dates
        tsv = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/notion_read_all_async.tsv"}, "notion_read_all_async"))
        tsv.create_table_sync(ds)

    def test_get_records_with_ids(self):
        reader = NotionReader(self.secret)
        tbl : TableSpec = self.get_test_table(reader)
        reader.set_table(tbl)

        ds : DataSet = reader.read_records_sync(10,include_ids=True).records

        tsv = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/notion_read_10_with_ids.tsv"}, "notion_read_10_with_ids"))
        tsv.create_table_sync(ds)

    def test_create_table(self):
        tsv = TsvReader(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_input/tsv_basic_test.tsv"}, "tsv_basic_test"))
        map_cols = {
            "id": DataColumn(COLUMN_TYPE.TEXT, "id"),
            "date": DataColumn(COLUMN_TYPE.DATE, "date"),
            "multiselect": DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            "select": DataColumn(COLUMN_TYPE.SELECT, "select"),
            "bad_data": DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        }
        dm = DataMap(map_cols, DataSetFormat(time_formats=["%b %d, %Y %I:%M %p"]))
        ds = tsv.read_records_sync(mapping=dm).records
        nw = NotionWriter(self.secret)
        parent_id = self.test_page
        table_sync = nw.create_table(ds,TableSpec(DATA_SOURCE.NOTION, {"parent_id": parent_id}, "API Test Sync"))
        table_async = nw.create_table(ds,TableSpec(DATA_SOURCE.NOTION, {"parent_id": parent_id}, "API Test Async"))
        nw.set_table(table_sync)
        asyncio.run(nw.write_records(ds))
        nw.set_table(table_async)
        nw.write_records_sync(ds)

    def test_complex_upload(self):

        if disable_expensive_tests:
            return

        tsv = TsvReader(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_input/chinese_sample.tsv"}, "chinese_sample"))
        map_cols = {
            "Tags": DataColumn(COLUMN_TYPE.MULTI_SELECT, "Tags"),
            "Last Created": DataColumn(COLUMN_TYPE.DATE, "Last Created"),
            "English": DataColumn(COLUMN_TYPE.TEXT, "English"),
            "Zhuyin": DataColumn(COLUMN_TYPE.TEXT, "Zhuyin"),
            "Subtags": DataColumn(COLUMN_TYPE.MULTI_SELECT, "Subtags"),
            "Examples & Usage": DataColumn(COLUMN_TYPE.TEXT, "Examples & Usage"),
            "Timestamp": DataColumn(COLUMN_TYPE.DATE, "Timestamp"),
            "Pinyin": DataColumn(COLUMN_TYPE.TEXT, "Pinyin"),
            "Hanzi": DataColumn(COLUMN_TYPE.TEXT, "Hanzi")
        }
        dm = DataMap(map_cols, DataSetFormat(time_formats=[]))
        ds = tsv.read_records_sync(mapping=dm).records
        nw = NotionWriter(self.secret)
        parent_id = self.test_page

        table_all = nw.create_table(ds,TableSpec(DATA_SOURCE.NOTION, {"parent_id": parent_id}, "API Test Chinese Upload - Full"))
        nw.set_table(table_all)

        page_size = 5
        current_page = 1
        total_pages = math.ceil(float(len(ds.records)) / float(page_size))
        print ("Writing page " + str(current_page) + " of " + str(total_pages)) 
        handle = nw.write_records_sync(ds, 5)

        while handle.params["last_written"] != -1:
            current_page += 1
            print ("Writing page " + str(current_page) + " of " + str(total_pages))
            handle = nw.write_records_sync(ds, 5, handle)

    def test_file_merge(self):
        tsv_in = TsvReader(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_input/chinese_sample.tsv"}, "chinese_sample"))
        map_cols = {
            "Tags": DataColumn(COLUMN_TYPE.MULTI_SELECT, "Tags"),
            "Last Created": DataColumn(COLUMN_TYPE.DATE, "Last Created"),
            "English": DataColumn(COLUMN_TYPE.TEXT, "English"),
            "Zhuyin": DataColumn(COLUMN_TYPE.TEXT, "Zhuyin"),
            "Subtags": DataColumn(COLUMN_TYPE.MULTI_SELECT, "Subtags"),
            "Examples & Usage": DataColumn(COLUMN_TYPE.TEXT, "Examples & Usage"),
            "Timestamp": DataColumn(COLUMN_TYPE.DATE, "Timestamp"),
            "Pinyin": DataColumn(COLUMN_TYPE.TEXT, "Pinyin"),
            "Hanzi": DataColumn(COLUMN_TYPE.TEXT, "Hanzi")
        }
        dm = DataMap(map_cols, DataSetFormat(time_formats=[]))
        ds_tsv = tsv_in.read_all_records_sync(1000)
        ds_tsv : DataSet = ds_tsv.remap(dm).op_returns["remapped_data"]

        nr = NotionReader(self.secret)
        nr.set_table(self.get_test_table(nr))
        ds_notion = nr.read_all_records_sync(100)

        ds_outer_merge = merge(ds_tsv,ds_notion,"Hanzi","Hanzi")
        ds_inner_merge = merge(ds_tsv,ds_notion,"Hanzi", "Hanzi", left_join=False, right_join=False)
        ds_left_merge = merge(ds_tsv,ds_notion,"Hanzi", "Hanzi", right_join=False, inner_join=False)
        ds_right_merge = merge(ds_tsv,ds_notion,"Hanzi", "Hanzi", left_join=False, inner_join=False)

        tsv_write = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/notion_outer_merge.tsv"}, "outer_merge_sample"))
        tsv_write.create_table_sync(ds_outer_merge)

        tsv_write = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/notion_left_merge.tsv"}, "left_merge_sample"))
        tsv_write.create_table_sync(ds_left_merge)

        tsv_write = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/notion_right_merge.tsv"}, "right_merge_sample"))
        tsv_write.create_table_sync(ds_right_merge)

        tsv_write = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/notion_inner_merge.tsv"}, "inner_merge_sample"))
        tsv_write.create_table_sync(ds_inner_merge)

    def test_update_columns(self):
        tsv_in = TsvReader(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_input/chinese_sample.tsv"}, "chinese_sample"))
        map_cols = {
            "Tags": DataColumn(COLUMN_TYPE.MULTI_SELECT, "Tags"),
            "Last Created": DataColumn(COLUMN_TYPE.DATE, "Last Created"),
            "English": DataColumn(COLUMN_TYPE.TEXT, "English"),
            "Zhuyin": DataColumn(COLUMN_TYPE.TEXT, "Zhuyin"),
            "Subtags": DataColumn(COLUMN_TYPE.MULTI_SELECT, "Subtags"),
            "Examples & Usage": DataColumn(COLUMN_TYPE.TEXT, "Examples & Usage"),
            "Timestamp": DataColumn(COLUMN_TYPE.DATE, "Timestamp"),
            "Pinyin": DataColumn(COLUMN_TYPE.TEXT, "Pinyin"),
            "Hanzi": DataColumn(COLUMN_TYPE.TEXT, "Hanzi")
        }
        dm = DataMap(map_cols, DataSetFormat(time_formats=[]))
        ds_tsv = tsv_in.read_all_records_sync(1000)
        ds_tsv : DataSet = ds_tsv.remap(dm).op_returns["remapped_data"]
        
        nw = NotionWriter(self.secret)
        parent_id = self.test_page

        table_10 = nw.create_table(ds_tsv,TableSpec(DATA_SOURCE.NOTION, {"parent_id": parent_id}, "API Test Update Columns"))
        nw.set_table(table_10)
        nw.write_records_sync(ds_tsv, 10)
        
        new_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "test_column_text"),
            DataColumn(COLUMN_TYPE.DATE, "test_column_date"),
            DataColumn(COLUMN_TYPE.SELECT, "test_column_select"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "test_column_multi_select")
        ]

        for col in new_cols:
            ds_tsv.add_column(col)

        ds_tsv.change_column_type("Timestamp", COLUMN_TYPE.TEXT)
        ds_tsv.change_column_type("Tags", COLUMN_TYPE.TEXT)

        col_list = ["test_column_text", "test_column_date", "test_column_select", "test_column_multi_select", "Timestamp", "Tags"]

        nw.update_columns(ds_tsv, col_list)


class TestTsvSync(unittest.TestCase):
    # def test_basic_read(self):
    #     reader = JsonReader({"file_path": "./test_input/json_basic_test.json"})
    #     ds : DataSet = asyncio.run( reader.read_records() )
    #     ds.change_column_type("date_added", COLUMN_TYPE.TEXT)
    #     write_out(ds.records,"./test_output/json_read_test.json")

    def test_basic_write(self):

        cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        ]
        records = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
                "bad_data": "xyz",
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
                "bad_data": "000 000 000"
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
                "bad_data": None
            },
            {
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
                "bad_data": None
            }
        ]

        ds = DataSet(cols, records)

        writer = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/tsv_basic_write.tsv"}, "basic_write"))
        asyncio.run( writer.create_table(ds) )

    def test_big_write(self):
        if disable_expensive_tests:
            return

        cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        ]
        record_template = {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
                "bad_data": "xyz",
        }
        ds = DataSet(cols)
        for id in range(10000):
            cur_record = copy.deepcopy(record_template)
            cur_record["id"] = str(id)
            cur_record["select"] = str(id)
            ds.add_record(cur_record)
        writer = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/tsv_big_write.tsv"}, "big_write"))
        asyncio.run( writer.create_table(ds) )

    def test_basic_read(self):
        map_cols = {
            "id": DataColumn(COLUMN_TYPE.TEXT, "id"),
            "date": DataColumn(COLUMN_TYPE.DATE, "date"),
            "multiselect": DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            "select": DataColumn(COLUMN_TYPE.SELECT, "select"),
            "bad_data": DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        }
        dm = DataMap(map_cols, DataSetFormat())
        reader = TsvReader(TableSpec(DATA_SOURCE.TSV,{"file_path": "./test_input/tsv_basic_test.tsv"},"read_test"))
        handle = asyncio.run( reader.read_records(mapping=dm) )
        ds = handle.records
        writer = TsvWriter(TableSpec(DATA_SOURCE.TSV,{"file_path": "./test_output/tsv_read_test.tsv"},"read_test"))
        asyncio.run( writer.create_table(ds) )

    def test_it_read_10(self):
        map_cols = {
            "id": DataColumn(COLUMN_TYPE.TEXT, "id"),
            "date": DataColumn(COLUMN_TYPE.DATE, "date"),
            "multiselect": DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            "select": DataColumn(COLUMN_TYPE.SELECT, "select"),
            "bad_data": DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        }
        dm = DataMap(map_cols, DataSetFormat())
        reader = TsvReader(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_input/tsv_big_read.tsv"}, "tsv_big_read"))
        handle = asyncio.run( reader.read_records(mapping=dm, limit=10) )
        ds = handle.records
        handle.close()
        writer = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/tsv_it_read_10_test.tsv"}, "tsv_big_read_test"))
        asyncio.run( writer.create_table(ds) )  

    def test_it_read_chunks(self):

        if disable_expensive_tests:
            return

        map_cols = {
            "id": DataColumn(COLUMN_TYPE.TEXT, "id"),
            "date": DataColumn(COLUMN_TYPE.DATE, "date"),
            "multiselect": DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            "select": DataColumn(COLUMN_TYPE.SELECT, "select"),
            "bad_data": DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        }
        dm = DataMap(map_cols, DataSetFormat())
        reader = TsvReader(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_input/tsv_big_read.tsv"}, "tsv_big_read"))
        
        with reader.read_records_sync(mapping=dm, limit=10) as cur_handle:
            # this approach ensures there are no file handles left hanging
            ds : DataSet = cur_handle.records
            while cur_handle.handle != None: 
                cur_handle = reader.read_records_sync(mapping=dm, limit=10, next_iterator=cur_handle)
                if cur_handle.handle != None:
                    ds.add_records(cur_handle.records.records)
            writer = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/tsv_it_read_all_test.tsv"}, "tsv_big_read_test"))
            asyncio.run( writer.create_table(ds) )

    def test_big_read(self):

        if disable_expensive_tests:
            return

        map_cols = {
            "id": DataColumn(COLUMN_TYPE.TEXT, "id"),
            "date": DataColumn(COLUMN_TYPE.DATE, "date"),
            "multiselect": DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            "select": DataColumn(COLUMN_TYPE.SELECT, "select"),
            "bad_data": DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        }
        dm = DataMap(map_cols, DataSetFormat())
        
        reader = TsvReader(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_input/tsv_big_read.tsv"}, "tsv_big_read"))
        handle = asyncio.run( reader.read_records(mapping=dm) )
        ds = handle.records
        writer = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/tsv_big_read_test.tsv"}, "tsv_big_read_test"))
        asyncio.run( writer.create_table(ds) )  
        # json_writer = JsonWriter(TableSpec(DATA_SOURCE.JSON, {"file_path": "./test_output/tsv_big_read_test_json.json"}, "tsv_big_read_test") )
        # asyncio.run(json_writer.create_table(ds))


class TestJsonSync(unittest.TestCase):

    def test_basic_read(self):
        reader = JsonReader(TableSpec(DATA_SOURCE.JSON, {"file_path": "./test_input/json_basic_test.json"}, "test"))
        ds : DataSet = asyncio.run( reader.read_records() ).records
        ds.change_column_type("date_added", COLUMN_TYPE.TEXT)
        # write_out(ds.records,"./test_output/json_read_test.json")

    def test_basic_write(self):

        cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        ]
        records = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
                "bad_data": "xyz",
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
                "bad_data": "000 000 000"
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
                "bad_data": None
            },
            {
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
                "bad_data": None
            }
        ]

        ds = DataSet(cols, records)

        writer = JsonWriter(TableSpec(DATA_SOURCE.JSON, {"file_path": "./test_output/json_basic_write.json"}, "json_basic_write"))
        asyncio.run( writer.create_table(ds) )

    def test_big_write(self):

        if disable_expensive_tests:
            return

        cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        ]
        record_template = {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
                "bad_data": "xyz",
        }
        ds = DataSet(cols)
        for id in range(10000):
            cur_record = copy.deepcopy(record_template)
            cur_record["id"] = str(id)
            cur_record["select"] = str(id)
            ds.add_record(cur_record)

        writer = JsonWriter(TableSpec(DATA_SOURCE.JSON, {"file_path": "./test_output/json_big_write.json"}, "test"))
        asyncio.run( writer.create_table(ds) )


class TestRemap(unittest.TestCase):
    def setUp(self) -> None:
        self.cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        ]
        self.records = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
                "bad_data": "xyz",
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
                "bad_data": "000 000 000"
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
                "bad_data": None
            },
            {
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
                "bad_data": None
            }
        ]

    def test_remap_drop_columns(self):
        mapping_1 = DataMap({ "id": DataColumn(COLUMN_TYPE.TEXT, "id"), "select": DataColumn(COLUMN_TYPE.SELECT, "select") }, DataSetFormat())
        result_1_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.SELECT, "select")
        ]
        result_1_records = [
            {
                "id": "0",
                "select": "0",
            },
            {
                "id": "1",
                "select": "1",
            },
            {
                "id": "2",
                "select": "2",
            },
            {
                "id": "3",
                "select": "3",
            }
        ]
        mapping_2 = DataMap({ "id": DataColumn(COLUMN_TYPE.TEXT, "id"), "date": DataColumn(COLUMN_TYPE.DATE, "date") }, DataSetFormat())
        result_2_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date")
        ]
        result_2_records = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1)
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2)
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3)
            },
            {
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 4)
            }
        ]
        ds = DataSet(self.cols, self.records)
        ds_op_1 = ds.remap(mapping_1)
        ds_op_2 = ds.remap(mapping_2)

        self.assertEqual(ds_op_1.status, OP_STATUS_CODE.OP_SUCCESS)
        self.assertEqual(ds_op_2.status, OP_STATUS_CODE.OP_SUCCESS)

        ds_remap_1 = ds_op_1.op_returns["remapped_data"]
        ds_remap_2 = ds_op_2.op_returns["remapped_data"]
        result_1 = DataSet(result_1_cols, result_1_records)
        result_2 = DataSet(result_2_cols, result_2_records)

        self.assertTrue( ds_remap_1.equivalent_to(result_1) )
        self.assertTrue( ds_remap_2.equivalent_to(result_2) )

    def test_remap_change_columns(self):
        mapping_1 = DataMap({ "id": DataColumn(COLUMN_TYPE.TEXT, "id"), "date": DataColumn(COLUMN_TYPE.DATE, "great_date"), "bad_data": DataColumn(COLUMN_TYPE.DATE, "bad_date") }, DataSetFormat())
        result_1_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "great_date"),
            DataColumn(COLUMN_TYPE.DATE, "bad_date")
        ]
        result_1_records = [
            {
                "id": "0",
                "great_date": datetime(1994, 3, 23, 12, 1),
                "bad_date": None,
            },
            {
                "id": "1",
                "great_date": datetime(1995, 3, 24, 12, 2),
                "bad_date": None
            },
            {
                "id": "2",
                "great_date": datetime(1996, 3, 25, 12, 3),
                "bad_date": None
            },
            {
                "id": "3",
                "great_date": datetime(1997, 3, 26, 12, 4),
                "bad_date": None
            }
        ]

        ds = DataSet(self.cols, self.records)

        # ds.drop_column("date")
        # write_out(ds.records,"./test_output/remap.json")

        # print(ds.column_names)
        ds_op_1 = ds.remap(mapping_1)

        self.assertEqual(ds_op_1.status, OP_STATUS_CODE.OP_SUCCESS)

        ds_remap_1 = ds_op_1.op_returns["remapped_data"]
        result_1 = DataSet(result_1_cols, result_1_records)

        self.assertTrue( ds_remap_1.equivalent_to(result_1) )

class TestComplexTransforms(unittest.TestCase):
    def setUp(self) -> None:
        self.source_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.TEXT, "bad_data"),
            DataColumn(COLUMN_TYPE.TEXT, "lose_this")
        ]
        self.source_records = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
                "bad_data": "xyz",
                "lose_this": None
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
                "bad_data": "000 000 000",
                "lose_this": None
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
                "bad_data": None,
                "lose_this": None
            },
            {
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
                "bad_data": None,
                "lose_this": None
            }
        ]
        self.destination_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.TEXT, "id_readable"),
            DataColumn(COLUMN_TYPE.TEXT, "all_nulls")
        ]
        self.destination_records = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
                "id_readable": "id: 0",
                "all_nulls": None
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
                "id_readable": "id: 1",
                "all_nulls": None
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
                "id_readable": "id: 2",
                "all_nulls": None
            },
            {
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
                "id_readable": "id: 3",
                "all_nulls": None
            }
        ]

        self.merged_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.DATE, "bad_date"),
            DataColumn(COLUMN_TYPE.TEXT, "id_readable"),
            DataColumn(COLUMN_TYPE.TEXT, "all_nulls")
        ]

        self.merged_records = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
                "id_readable": "id: 0",
                "bad_date": None,
                "all_nulls": None
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
                "id_readable": "id: 1",
                "bad_date": None,
                "all_nulls": None
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
                "id_readable": "id: 2",
                "bad_date": None,
                "all_nulls": None
            },
            {
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
                "id_readable": "id: 3",
                "bad_date": None,
                "all_nulls": None
            }
        ]

    def test_full_merge(self):
        ds_source = DataSet(self.source_cols,self.source_records)
        ds_dest = DataSet(self.destination_cols, self.destination_records)
        
        map_columns = {
            "id": DataColumn(COLUMN_TYPE.TEXT, "id"),
            "date": DataColumn(COLUMN_TYPE.DATE, "date"),
            "multiselect": DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            "select": DataColumn(COLUMN_TYPE.SELECT, "select"),
            "bad_data": DataColumn(COLUMN_TYPE.DATE, "bad_date") # we lose lose_this just to verify dropping columns works as intended
        }

        mapping = DataMap(map_columns, DataSetFormat())

        ds_remapped = ds_source.remap(mapping).op_returns["remapped_data"]

        ds_sample = DataSet(self.merged_cols, self.merged_records)
        ds_merged = merge(ds_remapped,ds_dest, left_key="id", right_key="id")

        self.assertTrue( ds_sample.equivalent_to(ds_merged) )



class TestDataMerges(unittest.TestCase):
    def setUp(self) -> None:
        self.cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select")
        ]
        self.left_records = [
            {
                "id": "merge_1",
                "multiselect": None,
                "select": None,
            },
            {
                "id": "merge_2",
                "multiselect": ['0','1','2','3','4'],
                "select": None,
            },
            {
                "id": "merge_2",
                "multiselect": None,
                "select": None,
            },
            {
                "id": 'merge_left_1',
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            }
        ]
        self.right_records = [
            {
                "id": "merge_1",
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "merge_2",
                "multiselect": None,
                "select": "0",
            },
            {
                "id": "merge_2",
                "multiselect": None,
                "select": "1",
            },
            {
                "id": 'merge_right_1',
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            }
        ]

    def test_inner_merge(self):

        merge_reference = [
            {
                "id": "merge_1",
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "merge_2",
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "merge_2",
                "multiselect": ['0','1','2','3','4'],
                "select": "1",
            },
            {
                "id": "merge_2",
                "multiselect": None,
                "select": "0",
            },
            {
                "id": "merge_2",
                "multiselect": None,
                "select": "1",
            }
        ]

        left = DataSet(self.cols, self.left_records)
        right = DataSet(self.cols, self.right_records)

        merged_manual = DataSet(self.cols)
        merged_manual.add_records(merge_reference)
        inner_merge = merge(left, right, "id", "id", left_join=False, right_join=False)

        # write_out(left.records,"./test_output/merge_left.json")
        # write_out(right.records,"./test_output/merge_right.json")

        self.assertTrue(inner_merge.equivalent_to(merged_manual))

    def test_merge_full_outer(self):
        merge_reference = [
            {
                "id": "merge_1",
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "merge_2",
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "merge_2",
                "multiselect": ['0','1','2','3','4'],
                "select": "1",
            },
            {
                "id": "merge_2",
                "multiselect": None,
                "select": "0",
            },
            {
                "id": "merge_2",
                "multiselect": None,
                "select": "1",
            },
            {
                "id": 'merge_left_1',
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": 'merge_right_1',
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            }
        ]

        left = DataSet(self.cols, self.left_records)
        right = DataSet(self.cols, self.right_records)

        merged_manual = DataSet(self.cols, merge_reference)
        outer_merge = merge(left, right, "id", "id")

        self.assertTrue( merged_manual.equivalent_to(outer_merge) )

    def test_left_merge(self):
        merge_reference = [
            {
                "id": 'merge_left_1',
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            }
        ]

        ds_left = DataSet(self.cols, self.left_records)
        ds_right = DataSet(self.cols, self.right_records)

        merged_manual = DataSet(self.cols, merge_reference)
        left_merge = merge(ds_left, ds_right, "id", "id", right_join=False, inner_join=False)

        self.assertTrue( merged_manual.equivalent_to(left_merge) )

    def test_right_merge(self):
        merge_reference = [
            {
                "id": 'merge_right_1',
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            }
        ]

        ds_left = DataSet(self.cols, self.left_records)
        ds_right = DataSet(self.cols, self.right_records)

        merged_manual = DataSet(self.cols, merge_reference)
        right_merge = merge(ds_left, ds_right, "id", "id", left_join=False, inner_join=False)

        self.assertTrue( merged_manual.equivalent_to(right_merge) )

class TestDataSet(unittest.TestCase):
    def setUp(self) -> None:
        locale.setlocale(locale.LC_ALL,"")
        # avoids errors with date formatting
        self.cols = [
            DataColumn(COLUMN_TYPE.TEXT, "title"),
            DataColumn(COLUMN_TYPE.TEXT, "description"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "tags")
        ]
        
        self.advanced_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "title"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.DATE, "date")
        ]
        self.records = [
            {
                "title": "record_1",
                "description": "this is the first record",
                "tags": ["0", "3", "5"]
            },
            {
                "title": "record_2",
                "description": "this is the second record",
                "tags": ["1", "2", "3"]
            },
            {
                "title": "too many fields",
                "description": "should cut down",
                "tags": ["5", "6", "7"],
                "extra1": "what is this",
                "extra2": "idk"
            },
            {
                "title": "too many fields",
                "description": "should cut down",
                "tags": ["5", "6", "7"],
            },
            {
                "title": "too few fields",
                "description": "should fill with Nones"
            },
            {
                "title": "too few fields",
                "description": "should fill with Nones",
                "tags": None
            },
            {
                "title": "merge_1",
                "description": None,
                "tags": None
            },
            {
                "title": "merge_2",
                "description": "This is merge_2.",
                "tags": ["test", "replace"]
            },
            {
                "title": None,
                "description": "Title should be merge_3.",
                "tags": None
            }
        ]
        self.merge_left_records = [
            {
                "title": "merge_1",
                "description": "This is merge_1 from left set.",
                "tags": ["0", "1"]
            },
            {
                "title": "merge_2",
                "description": "This is merge_2 from left set.",
                "tags": ["0", "2"]
            }
        ]

        self.type_change_records_text_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.TEXT, "date"),
            DataColumn(COLUMN_TYPE.TEXT, "multiselect"),
            DataColumn(COLUMN_TYPE.TEXT, "select")
        ]

        self.type_change_records_internal_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select")
        ]

        self.type_change_records_text = [
            {
                "id": "0",
                "date": "1994-03-23T12:01:00",
                "multiselect": "0,1,2,3,4",
                "select": "0",
            },
            {
                "id": "1",
                "date": "1995-03-24T12:02:00",
                "multiselect": "1,2,3,4,5",
                "select": "1",
            },
            {
                "id": "2",
                "date": "1996-03-25T12:03:00",
                "multiselect": "2,3,4,5,6",
                "select": "2",
            },
            {
                "id": "3",
                "date": "1997-03-26T12:04:00",
                "multiselect": "3,4,5,6,7",
                "select": "3",
            }
        ]

        self.type_change_records_internal = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
            },
            {
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
            }
        ]

    def test_get_column(self):
        ds = DataSet(self.cols)
        self.assertEqual( ds.get_column("title"), self.cols[0] )
        self.assertEqual( ds.get_column("description"), self.cols[1] )
        self.assertEqual( ds.get_column("tags"), self.cols[2] )
        self.assertRaises(ColumnError, ds.get_column, "snoog")

    def test_add_record(self):
        ds = DataSet(self.cols)
        ds.add_record( self.records[0] )
        ds2 = DataSet(self.cols)
        ds2.add_record( self.records[1] )
        ds.add_record( ds2.records[0] )
        self.assertNotEqual(ds2.records[0], ds.records[0])
        self.assertEqual(ds2.records[0], ds.records[1])
        ds.add_records( self.records[2:4] )
        self.assertEqual(ds.records[2], ds.records[3])
        ds.add_records( self.records[4:6] )
        self.assertEqual(ds.records[4], ds.records[5])

    def test_rename_column(self):
        ds = DataSet(self.cols)
        ds.add_records(self.records)
        self.assertEqual(ds._columns[0],DataColumn(COLUMN_TYPE.TEXT,"title"))
        ds.rename_column("title","name")
        self.assertEqual(ds._columns[0],DataColumn(COLUMN_TYPE.TEXT,"name"))
        cols2 = [
            DataColumn(COLUMN_TYPE.TEXT, "name"),
            DataColumn(COLUMN_TYPE.TEXT, "description"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "tags")
        ]
        ds2 = DataSet(cols2)
        renamed = {
                "name": "record_1",
                "description": "this is the first record",
                "tags": ["0", "3", "5"]
        }
        new_rec = ds2.add_record(renamed)
        self.assertEqual(new_rec, ds.records[0])

    def test_record_get_item(self):
        ds = DataSet(self.cols)
        ds.add_record(self.records[0])

    def test_append_records(self):
        ds = DataSet(self.cols)
        ds2 = DataSet(self.cols)
        ds3 = DataSet(self.cols) # benchmark for blunt append
        ds.add_records(self.records)
        ds2.add_records(self.records)
        ds3.add_records(self.records)
        ds3.add_records(self.records)

        append_no_ignore = append(ds, ds2, "title", "title", False)
        append_ignore = append(ds, ds2, "title", "title", True)

        self.assertEqual(append_no_ignore.records, ds3.records)
        # write_out(append_no_ignore.records,"./test_output/append_no_ignore.json")
        self.assertEqual(append_ignore.records, ds.records)
        # write_out(append_ignore.records,"./test_output/append_ignore.json")
        # the success of these tests might be down to implementation of dict
        # however it's probably fine, and avoids need to write a sort method


    def test_convert_types_correct(self):
        test_format = DataSetFormat(multiselect_delimiter=",")
        ds = DataSet(self.cols, format=test_format)

        # Select
        test_select_str = "select test"
        cur_out = ds.change_data_type("select test", COLUMN_TYPE.TEXT, COLUMN_TYPE.SELECT)
        self.assertEqual(test_select_str, cur_out) # no change as select validity is determined outside the scope of a dataset

        # Multiselect
        test_multiselect_str = "obj1,obj2,obj3,obj4,obj5"
        test_multiselect_list = ["obj1","obj2","obj3","obj4","obj5"]

        cur_out = ds.change_data_type(test_multiselect_str, COLUMN_TYPE.TEXT, COLUMN_TYPE.MULTI_SELECT)
        self.assertListEqual(test_multiselect_list, cur_out)

        cur_out = ds.change_data_type(test_multiselect_list, COLUMN_TYPE.MULTI_SELECT, COLUMN_TYPE.TEXT)
        self.assertEqual(test_multiselect_str, cur_out)

        # Dates

        test_date_str = "1994-03-23T12:01:00"
        test_date = datetime.fromisoformat(test_date_str)

        cur_out = ds.change_data_type("1994-03-23T12:01:00", COLUMN_TYPE.TEXT, COLUMN_TYPE.DATE)
        self.assertEqual(test_date, cur_out)

        cur_out = ds.change_data_type(test_date, COLUMN_TYPE.DATE, COLUMN_TYPE.TEXT)
        self.assertEqual(cur_out, test_date_str)

    def test_add_column(self):

        # self.cols = [
        #     DataColumn(COLUMN_TYPE.TEXT, "title"),
        #     DataColumn(COLUMN_TYPE.TEXT, "description"),
        #     DataColumn(COLUMN_TYPE.MULTI_SELECT, "tags")
        # ]

        # {
        #         "title": "record_1",
        #         "description": "this is the first record",
        #         "tags": ["0", "3", "5"]
        #     },
        #     {
        #         "title": "record_2",
        #         "description": "this is the second record",
        #         "tags": ["1", "2", "3"]
        #     }

        extra_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "title"),
            DataColumn(COLUMN_TYPE.TEXT, "description"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "tags"),
            DataColumn(COLUMN_TYPE.TEXT, "test_column")
        ]

        none_test = [
            {
                "title": "record_1",
                "description": "this is the first record",
                "tags": ["0", "3", "5"],
                "test_column": None
            },
            {
                "title": "record_2",
                "description": "this is the second record",
                "tags": ["1", "2", "3"],
                "test_column": None
            }]

        prefill_test = [
            {
                "title": "record_1",
                "description": "this is the first record",
                "tags": ["0", "3", "5"],
                "test_column": [0,1,2,3]
            },
            {
                "title": "record_2",
                "description": "this is the second record",
                "tags": ["1", "2", "3"],
                "test_column": [0,1,2,3]
            }]

        ds = DataSet(self.cols)
        ds.add_records(self.records[0:2])
        # write_out(ds.records,"./test_output/add_column_1.json")

        test_col = DataColumn(COLUMN_TYPE.TEXT,"test_column")
        ds.add_column(test_col)

        # write_out(ds.records,"./test_output/add_column_ds1.json")

        ds2 = DataSet(extra_cols, records=none_test)

        # write_out(ds2.records,"./test_output/add_column_ds2.json")

        self.assertEqual(ds.records[0], ds2.records[0])
        self.assertEqual(ds.records[1], ds2.records[1])


        ###

        ds = DataSet(self.cols)
        ds.add_records(self.records[0:2])

        test_col = DataColumn(COLUMN_TYPE.MULTI_SELECT,"test_column")
        ds.add_column(test_col, [0,1,2,3])

        ds2 = DataSet(extra_cols, records=prefill_test)
        self.assertEqual(ds.records[0], ds2.records[0])
        self.assertEqual(ds.records[1], ds2.records[1])

    def test_column_to_list(self):
        title_list = [ record["title"] for record in self.records ]
        ds = DataSet(self.cols)
        ds.add_records(self.records)
        title_column_list = ds.column_to_list("title")
        self.assertListEqual(title_list, title_column_list)

    def test_change_column_type(self):
        ds_text = DataSet(self.type_change_records_text_cols)
        ds_text.add_records(self.type_change_records_text)
    
        ds_internal = DataSet(self.type_change_records_internal_cols)
        ds_internal.add_records(self.type_change_records_internal)

        self.assertEqual( ds_text.column_to_list("id"), ds_internal.column_to_list("id") )
        self.assertNotEqual( ds_text.column_to_list("date"), ds_internal.column_to_list("date") )
        self.assertNotEqual( ds_text.column_to_list("multiselect"), ds_internal.column_to_list("multiselect") )
        self.assertEqual( ds_text.column_to_list("select"), ds_internal.column_to_list("select") ) # select is actually just text internally; should be changed?

        # text -> native type tests

        # new column tests
        ds_text.change_column_type("date", COLUMN_TYPE.DATE, "date_internal", inplace=False)
        ds_text.change_column_type("multiselect", COLUMN_TYPE.MULTI_SELECT, "multiselect_internal", inplace=False)
        ds_text.change_column_type("select", COLUMN_TYPE.SELECT, "select_internal", inplace=False)

        self.assertEqual( ds_text.column_to_list("id"), ds_internal.column_to_list("id") )
        self.assertEqual( ds_text.column_to_list("date_internal"), ds_internal.column_to_list("date") )
        self.assertEqual( ds_text.column_to_list("multiselect_internal"), ds_internal.column_to_list("multiselect") )
        self.assertEqual( ds_text.column_to_list("select_internal"), ds_internal.column_to_list("select") ) # select is actually just text internally

        # inplace column tests

        ds_text.change_column_type("date", COLUMN_TYPE.DATE)
        ds_text.change_column_type("multiselect", COLUMN_TYPE.MULTI_SELECT)
        ds_text.change_column_type("select", COLUMN_TYPE.SELECT)
        
        self.assertEqual( ds_text.column_to_list("id"), ds_internal.column_to_list("id") )
        self.assertEqual( ds_text.column_to_list("date"), ds_internal.column_to_list("date") )
        self.assertEqual( ds_text.column_to_list("multiselect"), ds_internal.column_to_list("multiselect") )
        self.assertEqual( ds_text.column_to_list("select"), ds_internal.column_to_list("select") )

        # native type -> text test

        ds_text_2 = DataSet(self.type_change_records_text_cols)
        ds_text_2.add_records(self.type_change_records_text)

        ds_internal.change_column_type("date", COLUMN_TYPE.TEXT, "date_text", inplace=False)
        ds_internal.change_column_type("multiselect", COLUMN_TYPE.TEXT, "multiselect_text", inplace=False)
        ds_internal.change_column_type("select", COLUMN_TYPE.TEXT, "select_text", inplace=False)

        self.assertEqual(ds_text_2.column_to_list("date"), ds_internal.column_to_list("date_text") )
        self.assertEqual(ds_text_2.column_to_list("multiselect"), ds_internal.column_to_list("multiselect_text") )
        self.assertEqual(ds_text_2.column_to_list("select"), ds_internal.column_to_list("select_text") )
        

        # failure cases

        with self.assertRaises(ColumnError) as ce:
            ds_internal.change_column_type("date", COLUMN_TYPE.MULTI_SELECT, "not_allowed")
            self.assertEquals(ce.error_code, COLUMN_ERROR_CODE.COLUMN_TYPE_INCOMPATIBLE)

    def test_convert_types_incorrect(self):
        ds_internal_2 = DataSet(self.type_change_records_internal_cols)
        ds_text_3 = DataSet(self.type_change_records_text_cols)

        text_list = [
        {
                "id": "0",
                "date": "1994-03-23T12:01:00",
                "multiselect": "0,1,2,3,4",
                "select": "0",
        },    
        {
                "id": "0",
                "date": "Mar 23, 1994", # incorrect format
                "multiselect": "0,1,2,3,4",
                "select": "0",
        }]

        ds_text_3.add_records(text_list)

        internal_list = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "0",
                "date": None,
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            }
        ]

        ds_internal_2.add_records(internal_list)

        report = ds_text_3.change_column_type("date", COLUMN_TYPE.DATE, "date_modified", inplace=False)

        self.assertEqual(report.non_critical_errors,1)
        self.assertEqual(ds_internal_2.column_to_list("date"),ds_text_3.column_to_list("date_modified"))

    def test_drop_column(self):

        cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select")
        ]

        sample = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "0",
                "date": None,
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            }
        ]

        sample_dropped = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4']
            },
            {
                "id": "0",
                "date": None,
                "multiselect": ['0','1','2','3','4']
            }
        ]

        ds = DataSet(cols)
        dropped_set = DataSet(cols[0:3])
        ds.add_records(sample)
        dropped_set.add_records(sample_dropped)

        self.assertRaises(ColumnError, ds.drop_column, "not_a_column")

        ds.drop_column("select")

        for i in range(len(ds.records)):
            self.assertEqual(ds.records[i],dropped_set.records[i])

    def test_equivalent_to_correct(self):
        ''' Check that equivalent_to function works when it should work. '''
        cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select") 
        ]
        records_1 = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
            },
            {
                "id": "2",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
            },
            {
                "id": "2",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
            }]
        records_disordered = [
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
            },
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "2",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
            },
            {
                "id": "2",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
            },
            ]
        
        ds = DataSet(cols,records_1)
        ds_disordered = DataSet(cols, records_disordered)
        self.assertTrue(ds.equivalent_to(ds_disordered, "id") ) # with designated key
        self.assertTrue(ds.equivalent_to(ds_disordered, "date") ) # with a different designated key
        self.assertTrue(ds.equivalent_to(ds_disordered, "select") ) # with a different designated key
        self.assertTrue(ds.equivalent_to(ds_disordered)) # without designated key

    def test_equivalent_to_different_columns(self):
        cols_1 = [DataColumn(COLUMN_TYPE.TEXT, "id"),
                DataColumn(COLUMN_TYPE.DATE, "date"),
                DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
                DataColumn(COLUMN_TYPE.SELECT, "select") ]
        cols_2 = [DataColumn(COLUMN_TYPE.TEXT, "id"),
                DataColumn(COLUMN_TYPE.DATE, "date"),
                DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect") ]
        ds1 = DataSet(cols_1)
        ds2 = DataSet(cols_2)
        self.assertFalse( ds1.equivalent_to(ds2) )

    def test_equivalent_to_empty_set(self):
        cols_1 = [DataColumn(COLUMN_TYPE.TEXT, "id"),
                DataColumn(COLUMN_TYPE.DATE, "date"),
                DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
                DataColumn(COLUMN_TYPE.SELECT, "select") ]
        cols_2 = [DataColumn(COLUMN_TYPE.TEXT, "id"),
                DataColumn(COLUMN_TYPE.DATE, "date"),
                DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
                DataColumn(COLUMN_TYPE.SELECT, "select") ]
        ds1 = DataSet(cols_1)
        ds2 = DataSet(cols_2)
        self.assertTrue( ds1.equivalent_to(ds2) )

    def test_equivalent_to_improper_key(self):
        cols_1 = [DataColumn(COLUMN_TYPE.TEXT, "id"),
                DataColumn(COLUMN_TYPE.DATE, "date"),
                DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
                DataColumn(COLUMN_TYPE.SELECT, "select") ]
        cols_2 = [DataColumn(COLUMN_TYPE.TEXT, "id"),
                DataColumn(COLUMN_TYPE.DATE, "date"),
                DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
                DataColumn(COLUMN_TYPE.SELECT, "select") ]
        ds1 = DataSet(cols_1)
        ds2 = DataSet(cols_2)
        self.assertRaises( ColumnError, ds1.equivalent_to, ds2, "not_a_column" )

    def test_equivalent_to_normal_failure(self):
        cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select") 
        ]
        records_1 = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
            },
            { # here has several differences from other dataset
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 5),
                "multiselect": ['3','4','5','6','5'],
                "select": "33",
            }]
        records_disordered = [
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
            },
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
            },
            {
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
            }]

        ds = DataSet(cols,records_1)
        ds_disordered = DataSet(cols, records_disordered)

        self.assertFalse(ds.equivalent_to(ds_disordered, "id") ) # with designated key
        self.assertFalse(ds.equivalent_to(ds_disordered, "date") ) # with a different designated key
        self.assertFalse(ds.equivalent_to(ds_disordered, "select") ) # with a different designated key
        self.assertFalse(ds.equivalent_to(ds_disordered)) # without designated key


if __name__ == '__main__':
    unittest.main()