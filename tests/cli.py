#!/usr/bin/env python

from core.sync.sync_notion import *
from core.sync.sync_tsv import *
from core.sync.sync_json import *
import argparse
import sys

def show_progress(prefix : str, current : int, total : int):
    if total == 0:
        return
    sys.stdout.write(f"\r{prefix} {current}/{total} ({ str(round((current/total) * 100, 1))}%)")
    sys.stdout.flush()

def _setup_input(input_type : str, input, secret : str) -> SourceReader:
    tr = None
    if input_type == "tsv":
        tr = TsvReader(TableSpec(DATA_SOURCE.TSV, {"file_path": input},"tsv_source"))
    elif input_type == "json":
        tr = JsonReader(TableSpec(DATA_SOURCE.JSON, {"file_path": input},"json_source"))
    elif input_type == "notion":
        if secret == None:
            sys.exit("Need a valid Notion integration key to read Notion databases.")
        tr = NotionReader(secret)
        tr.set_table(tr.get_table(input))
    return tr

def _setup_output(output_type : str, output : str, secret : str) -> SourceWriter:
    tw = None
    if output_type == "tsv":
        tw = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": output},"tsv_source"))
    elif output_type == "json":
        tw = JsonWriter(TableSpec(DATA_SOURCE.JSON, {"file_path": output},"json_source"))
    elif output_type == "notion":
        if secret == None:
            sys.exit("Need a valid Notion integration key to write Notion databases.")
        tw = NotionWriter(secret)
        tr = NotionReader(secret)
        tw.set_table(tr.get_table(output))
    return tw

def _report_callbacks(status:SyncStatus):
    if status.status == SYNC_STATUS_CODE.WRITING_SOURCE:
        show_progress("Writing records: ", status.records_synced, status.total_records)
    elif status.status == SYNC_STATUS_CODE.UPDATING_SOURCE:
        show_progress("Updating records: ", status.records_synced, status.total_records)


def read(parsed : argparse.Namespace):
    input = parsed.input
    limit = parsed.limit
    output = parsed.output

    tr = _setup_input(input[0], input[1], parsed.secret)

    if limit == None:
        ds : DataSet = tr.read_all_records_sync(100)
    else: 
        ds: DataSet = tr.read_records_sync(int(limit)).records

    if parsed.map != None:
        remap = setup_remap_info(ds, parsed.map)
        ds = ds.remap(map=DataMap(remap,ds.format)).op_returns["remapped_data"] # could be updated to allow custom delimiters and data formats

    if output == None:
        for record in ds.records:
            print(record.asdict())
    else:
        if output[0] == "json" or output[0] == "tsv": # no real write_records methods implemented yet
            tw : SourceWriter = _setup_output(output[0], output[1], parsed.secret)
            tw.create_table_sync(ds, None)
        elif output[0] == "notion":
            tw : NotionWriter = _setup_output(output[0], output[1], parsed.secret)
            handle = tw.write_records_sync(ds,1)
            cur_record = 1
            total_records = len(ds.records)
            show_progress("Writing records: ", cur_record, total_records)
            while not handle.done:
                handle = tw.write_records_sync(ds,1,handle)
                cur_record += 1
                show_progress("Writing records: ",cur_record, total_records)
            print()

str_to_type_map = {
    'date' : COLUMN_TYPE.DATE,
    'text' : COLUMN_TYPE.TEXT,
    'select' : COLUMN_TYPE.SELECT,
    'multiselect' : COLUMN_TYPE.MULTI_SELECT
}

def setup_remap_info(ds : DataSet, remaps : list): # it will be a list of lists
    source_columns = copy.deepcopy(ds.columns)
    remap_column_index = dict()
    for remap in remaps:
        if remap[1] not in str_to_type_map:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, remap[1] + " is not a valid data type. Valid data types are date, text, select, or multiselect.")
        remap_column_index[remap[0]] = DataColumn(str_to_type_map.get(remap[1]), remap[0])
    # algorithm will ignore non-matching remaps, which is fine for CLI purposes
    output = dict()
    for col in source_columns:
        if col.name in remap_column_index:
            col.type = remap_column_index[col.name].type
        output[col.name] = col
    return output

def update(parsed : argparse.Namespace):
    input = parsed.input
    output = parsed.output
    limit = parsed.limit

    if input == None or output == None:
        sys.exit("Need an input and an output to run an update.")
    # if input[0] != "json" or output[0] != "notion":
    #     sys.exit("Only support json -> Notion for now.")
    if parsed.primary_key == None:
        sys.exit("Need a primary key column to perform an update.")

    tr = _setup_input(input[0], input[1], parsed.secret)

    if limit == None:
        ds : DataSet = tr.read_all_records_sync(100)
    else: 
        ds: DataSet = tr.read_records_sync(int(limit)).records

    if parsed.map != None:
        remap = setup_remap_info(ds, parsed.map)
        ds = ds.remap(map=DataMap(remap,ds.format)).op_returns["remapped_data"]
        # could be updated to allow custom delimiters and data formats

    tw : NotionWriter = _setup_output(output[0], output[1], parsed.secret)

    tw.update_table(ds, parsed.primary_key, _report_callbacks)
    print()

parser = argparse.ArgumentParser("Interact with Notion Core via a CLI.")
parser.add_argument("method", choices=["update", "read"])
# Input and output are in same format
# e.g. -i notion abcd123 -o tsv ./output.tsv
parser.add_argument("-m", "--map", nargs=2,action="append", metavar=("column","type"))
parser.add_argument("-i", "--input", nargs=2)
parser.add_argument("-o", "--output", nargs=2)
parser.add_argument("--secret")
parser.add_argument("--limit")
parser.add_argument("--primary_key")

parsed = parser.parse_args(sys.argv[1:])

method_dict = {
    "update": update,
    "read": read
}

method_dict[parsed.method](parsed)