from turtle import update
from .sync_types import *
from ..dataset import COLUMN_TYPE, DataSet
import requests
import uuid
import json

select_color_list = ["orange", "yellow", "green", "blue", "purple", "pink", "red"]
notion_version = "2021-08-16"

def make_property_date(ds : DataSet, col_name: str, uvs : Dict[str, Set[str]]):
    return {"date": {}}

def make_property_text(ds : DataSet, col_name: str, uvs : Dict[str, Set[str]]):
    return {"rich_text": {}}

def make_property_title(ds : DataSet, col_name: str, uvs : Dict[str, Set[str]]):
    return {"title": {}}

def make_property_select(ds : DataSet, col_name: str, uvs : Dict[str, Set[str]]):
    unique_values = uvs[col_name]
    options = []
    for i, uv in enumerate(unique_values):
        if uv != "" and uv != None:
            options.append ({"name": uv, "id": str(uuid.uuid4()), "color": select_color_list[i % len(select_color_list)]})
    
    key = "multi_select"
    if ds.get_column(col_name).type == COLUMN_TYPE.SELECT:
        key = "select"

    return {key: {"options": options} }

def make_value_title(id : str, value : str):
    return {
        "title" : make_value_text(id, value)["rich_text"]
    }

def make_value_text(id : str, value : str):
    return {
        "rich_text": [
            {
                "plain_text": value,
                "annotations": make_annotation(),
                "type": "text",
                "text": {
                    "content": value
                }
            }
        ]
    }

def make_value_select(id : str, value : str):
    return {
        "select": {
            "name": value
        }
    }

def make_value_multiselect(id : str, value: List[str]):
    options = []
    for option in value:
        if option != "" and option != None:
            options.append( {"name": option} )
    return {
        "multi_select": options
    }

def make_value_date(id : str, value : datetime):
    if value == None:
        return None # looks silly, but should set value to empty
    return {
        "date": {
            "start": value.astimezone().replace(microsecond=0).isoformat()
        }
    }

def make_filter_text(property_name: str, value : str):
    return {
        "property": property_name,
        "text": {
            "equals": value
        }
    }

def make_filter_select(property_name: str, value : str):
    return { 
        "property": property_name,
        "select": {
            "equals": value
        }
    }

def make_filter_multiselect(property_name: str, value : list[str]):
    out_filter = { "and": [] }
    for v in value:
        out_filter["and"].append( { "property": property_name, "multiselect": { "contains": v } } )

def make_filter_date(property_name: str, value : datetime):
    return {
        "property": property_name,
        "date": {
            "equals": value.astimezone().replace(microsecond=0).isoformat()
        }
    }

def make_annotation(bold : bool = False, italic : bool = False, strikethrough: bool = False, underline : bool = False, code : bool = False, color : str = "default"):
        out = {
            "bold": bold, 
            "italic": italic, 
            "strikethrough": strikethrough,
            "underline": underline,
            "code": code,
            "color": color
        }
        return out

def get_notion_primary_key(result) -> str:
    for prop in result["properties"]:
        try:
            if result["properties"][prop]["type"] == "title":
                return prop
        except KeyError as ke:
            print ("Error: property " + prop + " has no type.")
            print (result["properties"][prop])

def get_notion_property_ids(result) -> Dict[str, str]:
    id_dict = {}
    for prop in result["properties"]:
        id_dict[prop] = result["properties"][prop]["id"]
    return id_dict

@dataclass
class NotionSyncHandle(SyncHandle):
    handle : str
    params : dict = field(default_factory=dict)

    def __init_subclass__(cls) -> None:
        return super().__init_subclass__()
    
    def __enter__(self):
        self.params["last_written"] = -1
        # No setup required right now.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class NotionWriter(SourceWriter):
    '''Write records to Notion.'''

    make_property_strategies = {
        COLUMN_TYPE.DATE: make_property_date,
        COLUMN_TYPE.TEXT: make_property_text,
        COLUMN_TYPE.SELECT: make_property_select,
        COLUMN_TYPE.MULTI_SELECT: make_property_select
    }

    make_value_strategies = {
        COLUMN_TYPE.DATE: make_value_date,
        COLUMN_TYPE.TEXT: make_value_text,
        COLUMN_TYPE.SELECT: make_value_select,
        COLUMN_TYPE.MULTI_SELECT: make_value_multiselect,
    }

    def __init__(self, api_key : str = None) -> None:
        self.api_key = None
        self.table = None
        if api_key != None:
            self.api_key = api_key

    def set_table(self, table : TableSpec):
        self.table = table

    def create_table(self, dataset: DataSet, spec: TableSpec, callback = None) -> TableSpec:
        # create a new page with a database in Notion
        if self.api_key == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "No API key found in Create Table for Notion.")
        parent_param = spec.parameters["parent_id"]
        if parent_param != None:
            parent = { "type": "page_id", "page_id": parent_param }
        else:
            parent = {"type": "workspace", "workspace": True}
        title = { "plain_text": spec.name, "annotations": make_annotation(), "type": "text", "text": {"content": spec.name} }
        data = { "parent": parent, "title": [title], "properties": NotionWriter.extract_properties(dataset)}
        res = requests.post("https://api.notion.com/v1/databases", json=data, auth=BearerAuth(self.api_key), headers={"Notion-Version": notion_version})
        json = res.json()
        if res.status_code != 200:
            raise SyncError(SYNC_ERROR_CODE.REQUEST_REJECTED, message=json)
        spec_out = copy.deepcopy(spec)
        spec_out.parameters["id"] = json["id"]
        spec_out.parameters["primary_key"] = get_notion_primary_key(json)
        spec_out.parameters["columns"] = get_notion_property_ids(json)
        return spec_out

    async def write_records(self, dataset : DataSet, limit: int = -1, next_iterator : NotionSyncHandle = None) -> NotionSyncHandle:
        return self._write_records(dataset, limit, next_iterator)

    def write_records_sync(self, dataset : DataSet, limit: int = -1, next_iterator : NotionSyncHandle = None) -> NotionSyncHandle:
        return self._write_records(dataset, limit, next_iterator)

    def _write_records(self, dataset : DataSet, limit: int = -1, next_iterator : NotionSyncHandle = None) -> NotionSyncHandle:
        results = []

        record_count = len(dataset.records)

        start_i : int = 0
        if next_iterator != None:
            last_written = next_iterator.params["last_written"]
            if last_written + 1 < record_count and last_written >= 0:
                start_i = last_written + 1

        end_i : int = start_i + limit
        
        if end_i > record_count or limit < 0:
            end_i = record_count
        
        last_written_out = 0
        for i in range(start_i, end_i):
            record = dataset.records[i]
            try:
                write_result = self._write_record(dataset, record)
                results.append(write_result)
                last_written_out = i
            except SyncError as err:
                print(err)

        done = False
        if last_written_out >= record_count - 1:
            done = True

        return NotionSyncHandle(results, DATA_SOURCE.NOTION, "", done, params={"last_written": last_written_out})            

    def _write_record(self, dataset : DataSet, record : DataRecord) -> dict:
        property_dict = {}

        if self.table == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "Table not set in NotionWriter._write_record.")

        for col in dataset.columns:
            col_id = self.table.parameters["columns"][col.name]
            if col.name == self.table.parameters["primary_key"]:
                property_dict[col.name] = make_value_title( col_id, record[col.name] )
            else:
                if record[col.name] != None:
                    property_dict[col.name] = NotionWriter.make_value_strategies[col.type]( col_id, record[col.name] )

        data = {
            "parent": {
                "type": "database_id",
                "database_id": self.table.parameters["id"]
            },
            "properties": property_dict
        }

        res = requests.post("https://api.notion.com/v1/pages", json=data, auth=BearerAuth(self.api_key), headers={"Notion-Version": notion_version})
        if res.status_code != 200:
            raise SyncError(SYNC_ERROR_CODE.REQUEST_REJECTED, res.json())
        return res.json()


    def update_table(self, left : DataSet, primary_key : str, loop_callback):
        nr = NotionReader(self.api_key)
        nr.set_table(self.table)

        # print("Starting to read records from dataset.")
        handle = nr.read_records_sync(100, include_ids=True)
        right = handle.records

        while not handle.done:
            handle = nr.read_records_sync(100, next_iterator=handle, include_ids=True)
            right.add_records(handle.records)

        lki = build_key_index(left,primary_key)

        full_outer_records = merge(left, right, primary_key, primary_key, overwrite=True)
        left_col_names = set(left.column_names)
        right_col_names = set(right.column_names)
        new_col_names = list(left_col_names.difference(right_col_names))

        # print("Found " + len(new_records.records) + " new records and " + len(update_records.records) + " records to update.")
    
        if ("notion_page_id" in new_col_names):
            new_col_names.remove("notion_page_id")

        # print("Updating columns.")
        self.update_columns(left, new_col_names, self.table.parameters["primary_key"])

        ncs = nr.get_columns()
        ncs.append( DataColumn(COLUMN_TYPE.TEXT, "notion_page_id") )
        notion_columns = nr.get_columns_as_dict()

        new_records = DataSet(ncs)
        update_records = DataSet(ncs)

        for record in full_outer_records.records:
            if record["notion_page_id"] == None:
                new_records.add_record(record)
            elif record[primary_key] in lki:
                update_records.add_record(record)

        new_records.drop_column("notion_page_id")

        # print("Writing " + str(len(new_records.records)) + " new records.")
        self.write_records_sync(new_records)

        # print ("Starting update of " + str(len(update_records.records)) + " records.")
        for record in update_records.records:
            self.update_record_by_id(record, update_records, record["notion_page_id"], notion_columns)

    def update_record_by_id(self, record : DataRecord, dataset : DataSet, id : str, notion_columns : dict[str,DataColumn] = None):
        if notion_columns == None:
            nr = NotionReader(self.api_key)
            nr.set_table(self.table)
            n_cols = nr.get_columns_as_dict()
        else:
            n_cols = notion_columns

        properties = {}

        for column in dataset.columns:
            if column.name not in n_cols:
                continue
            if column.type != n_cols[column.name].type:
                continue
            col_id = self.table.parameters["columns"][column.name]
            if (self.table.parameters["primary_key"] == column.name):
                properties[column.name] = make_value_title(col_id, record[column.name])
            else: 
                properties[column.name] = NotionWriter.make_value_strategies[column.type](col_id,record[column.name])

        data = {"properties": properties}

        res = requests.patch(f"https://api.notion.com/v1/pages/{id}", json=data, auth=BearerAuth(self.api_key), headers={"Notion-Version": notion_version})
        if res.status_code != 200:
                raise SyncError(SYNC_ERROR_CODE.REQUEST_REJECTED, res.json())

    def update_record(self, record : DataRecord, dataset : DataSet, key_column : str, key_val : object, update_all : bool = True):
        ''' Note: has complex behaviour where two records share the same key column. '''
        nr = NotionReader(self.api_key)
        nr.set_table(self.table)

        record_ids = nr.find_record_ids(key_column, key_val)
        if len(record_ids) == 0:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "Couldn't find record in NotionReader.update_record.")

        notion_columns = nr.get_columns_as_dict()

        properties = {}

        for column in dataset.columns:
            if column.name not in notion_columns:
                continue
            if column.type != notion_columns[column.name].type:
                continue
            col_id = self.table.parameters["columns"][column.name]
            if (self.table.parameters["primary_key"] == column.name):
                properties[column.name] = make_value_title(col_id, record[column.name])
            else: 
                properties[column.name] = NotionWriter.make_value_strategies[column.type](col_id,record[column.name])

        data = {"properties": properties}

        if update_all and len(record_ids) > 1:
            for id in record_ids:
                res = requests.patch(f"https://api.notion.com/v1/pages/{id}", json=data, auth=BearerAuth(self.api_key), headers={"Notion-Version": notion_version})
                if res.status_code != 200:
                    raise SyncError(SYNC_ERROR_CODE.REQUEST_REJECTED, res.json())
        else:
            id = record_ids[0]
            res = requests.patch(f"https://api.notion.com/v1/pages/{id}", json=data, auth=BearerAuth(self.api_key), headers={"Notion-Version": notion_version})
            if res.status_code != 200:
                    raise SyncError(SYNC_ERROR_CODE.REQUEST_REJECTED, res.json())

    def update_columns(self, dataset : DataSet, columns : list[str], title_column : str = None):
        ''' Update database table with columns from dataset. This both appends new columns and modifies existing columns. '''
        
        if self.table == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "Table not set in NotionWriter.append_columns")

        database_id = self.table.parameters["id"]

        property_object = NotionWriter.extract_properties(dataset, infer_title=False, title_column=title_column)

        column_names = set(columns)

        # filter properties not in the list
        data = dict()
        data["properties"] = dict()
        
        for k, v in property_object.items():
            if k in column_names:
                data["properties"][k] = v

        res = requests.patch("https://api.notion.com/v1/databases/" + database_id, json=data, auth=BearerAuth(self.api_key), headers={"Notion-Version": notion_version})
        
        if res.status_code != 200:
            raise SyncError(SYNC_ERROR_CODE.REQUEST_REJECTED, res.json())

        json = res.json()

        self.table.parameters["columns"] = get_notion_property_ids(json)

    @staticmethod
    def extract_properties(ds : DataSet, infer_title = True, title_column = None) -> dict:
        out = {}
        uniques = ds.get_uniques()
        title_created = False

        for column in ds.columns:
            if ((infer_title and not title_created) or (not infer_title and title_column == column.name)) and column.type == COLUMN_TYPE.TEXT:
                out[column.name] = make_property_title(ds, column.name, uniques)
                title_created = True
            else:
                out[column.name] = NotionWriter.make_property_strategies[column.type](ds, column.name, uniques)
        return out

class NotionReader(SourceReader):
    '''Read records from Notion.'''
    def __init__(self, api_key : str = None) -> None:
        self.api_key = None
        self.table = None
        if api_key != None:
            self.api_key = api_key
        self.type_map = {
            "title": COLUMN_TYPE.TEXT,
            "rich_text": COLUMN_TYPE.TEXT,
            "multi_select": COLUMN_TYPE.MULTI_SELECT,
            "date": COLUMN_TYPE.DATE,
            "created_time": COLUMN_TYPE.DATE
        }
        self.read_strategies = {
            'title': self._map_notion_text,
            'rich_text': self._map_notion_text,
            'multi_select': self._map_notion_multiselect,
            'date': self._map_notion_date,
            'created_time': self._map_notion_created_time
        }
        self.filter_strategies = {
            COLUMN_TYPE.TEXT: make_filter_text,
            COLUMN_TYPE.SELECT: make_filter_select,
            COLUMN_TYPE.MULTI_SELECT: make_filter_multiselect,
            COLUMN_TYPE.DATE: make_filter_date
        }

    def set_table(self, table : TableSpec):
        self.table = table

    def get_columns(self):
        if self.table == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "No table set when reading records from Notion.")
        return self.get_column_spec(self.api_key, self.table.parameters["id"])

    def get_columns_as_dict(self) -> dict[str,DataColumn]:
        columns = self.get_columns()
        out_dict = dict()
        for col in columns:
            out_dict[col.name] = col
        return out_dict

    def _read_records(self, limit : int = -1, next_iterator : NotionSyncHandle = None, mapping : DataMap = None, include_ids : bool = False) -> NotionSyncHandle:
        if self.table == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "No table set when reading records from Notion.")
        return self.get_records(self.api_key, self.table.parameters["id"], self.get_columns(), number=limit, iterator=next_iterator, include_ids = include_ids)

    async def read_records(self, limit: int = -1, next_iterator: NotionSyncHandle = None, mapping: DataMap = None, include_ids : bool = False) -> NotionSyncHandle:
        return self._read_records(limit = limit, next_iterator = next_iterator, mapping = mapping, include_ids = include_ids)

    def read_records_sync(self, limit: int = -1, next_iterator: NotionSyncHandle = None, mapping: DataMap = None, include_ids : bool = False) -> NotionSyncHandle:
        return self._read_records(limit = limit, next_iterator = next_iterator, mapping = mapping, include_ids = include_ids)

    def get_table_parents(self) -> "list[TableSpec]":
        ''' Gets possible parents for a table - i.e. pages that are available in Notion. '''
        if self.api_key == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "API key not set in NotionReader.")
        data = { "filter": {"property": "object", "value": "page"} }
        res = requests.post("https://api.notion.com/v1/search", json=data, auth=BearerAuth(self.api_key), headers={"Notion-Version": notion_version})
        json = res.json()
        return [self._parse_page_result(page) for page in json["results"]]

    def _parse_page_result(self, page) -> "List[TableSpec]":
        title = ""
        for prop in page["properties"].values():
            if prop["type"] == "title":
                if len(prop["title"]) > 0:
                    title = prop["title"][0]["plain_text"]
                break
        return TableSpec(DATA_SOURCE.NOTION, {"parent_id": page["id"]}, title)

    def get_tables(self) -> "list[TableSpec]":
        if self.api_key == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "API key not set in NotionReader.")
        return self.get_databases(self.api_key)

    def get_table(self, id:str) -> TableSpec:
        tables = self.get_tables()
        for table in tables:
            if table.parameters["id"].replace('-','') == id.replace('-',''): # normalise ids
                return table
        return None

    def get_databases(self, api_key : str):
        data = { "filter": {"property": "object", "value": "database"} }
        res = requests.post("https://api.notion.com/v1/search", json=data, auth=BearerAuth(api_key), headers={"Notion-Version": notion_version})
        json = res.json()
        return [self._parse_database_result(x) for x in json["results"]]

    def get_records(self, api_key: str, table_id: str, column_info: list, number=100, iterator : NotionSyncHandle = None, record_filter : dict = None, include_ids=False) -> NotionSyncHandle:
        json = self.query_database(api_key, table_id, number, iterator, record_filter)
        it = None
        done = False
        if json["has_more"]:
            it = json["next_cursor"]
        else:
            done = True
        records = [ self._map_record(record, column_info, include_ids) for record in json["results"] ]

        final_info = copy.deepcopy(column_info)
        if include_ids:
            final_info.append( DataColumn(COLUMN_TYPE.TEXT, "notion_page_id") )

        return NotionSyncHandle( DataSet(final_info, records), DATA_SOURCE.NOTION, handle=it, done=done )

    def find_records(self, key_col : str, key_val : object, include_ids : bool = False) -> NotionSyncHandle:
        col_info = self.get_columns()
        json = self._find_records_raw(key_col, key_val, col_info)
        records = [ self._map_record(record, col_info, include_ids) for record in json["results"]]
        return NotionSyncHandle( DataSet(col_info, records), DATA_SOURCE.NOTION, "", done=True )

    def find_record_ids(self, key_col : str, key_val : object) -> list[str]:
        col_info = self.get_columns()
        json = self._find_records_raw(key_col, key_val, col_info)
        out_ids : list[str] = []
        for page in json["results"]:
            out_ids.append(page["id"])
        return out_ids

    def _find_records_raw(self, key_col : str, key_val : object, columns : list[DataColumn]) -> dict:
        data_column = next( filter(lambda col : col.name == key_col, columns))
        if isinstance(key_val, str):
            key_val = key_val.strip()
        fil = self.filter_strategies[data_column.type](key_col, key_val)
        
        return self.query_database(self.api_key, self.table.parameters["id"], record_filter=fil)

    def query_database(self, api_key:str, table_id:str, number=100, iterator : NotionSyncHandle = None, record_filter : dict = None) -> dict:
        ''' A lower-level function which plugs directly into the Notion query api and provides the interface for the generic functions with Notion. '''
        url = f"https://api.notion.com/v1/databases/{table_id}/query"
        data = {"page_size": number}
        if iterator is not None: data["start_cursor"] = iterator.handle
        if record_filter is not None: data["filter"] = record_filter # deliberately not doing anything fancy as Notion's filter syntax is complex and better off just building queries that fit Notion
        res = requests.post(url, json=data, auth=BearerAuth(api_key), headers={"Notion-Version": notion_version})
        if res.status_code != 200:
            raise SyncError(SYNC_ERROR_CODE.REQUEST_REJECTED, res.json())
        return res.json()
        
    def get_record_types(self):
        pass

    def get_column_spec(self, api_key, id) -> List[DataColumn]:
        url = f"https://api.notion.com/v1/databases/{id}"
        res = requests.get(url, auth=BearerAuth(api_key), headers={"Notion-Version": notion_version})
        json = res.json()
        return [ self._map_column(k,json["properties"][k]) for k in json["properties"] ]

    def _parse_database_result(self, result) -> TableSpec:
        name = result["title"][0]["text"]["content"]
        params = {'name': name, 'id': result["id"], 'primary_key': get_notion_primary_key(result), 'columns': get_notion_property_ids(result)}
        return TableSpec(DATA_SOURCE.NOTION, parameters=params, name=name)

    def _map_column(self, column_name, column) -> DataColumn:
        if column["type"] in self.type_map:
            col_type = self.type_map[column["type"]]
        else:
            col_type = COLUMN_TYPE.TEXT
        return DataColumn(col_type, column_name)
    
    def _map_record(self, record:dict, columns:list, include_ids:bool=False):
        out_dict = {}
        column_names = [ col.name for col in columns ]
        for k in column_names:
            if k in record["properties"]:
                v = record["properties"][k]
                prop_type = v["type"]
                if prop_type in self.read_strategies:
                    # print (f"{prop_type} is in read strategies.")
                    out_dict[k] = self.read_strategies[prop_type](v)
                else: out_dict[k] = None # If we don't know how to handle the type, just return null.
            else:
                out_dict[k] = None
        if include_ids:
            out_dict["notion_page_id"] = record["id"]
        return out_dict

    def _map_notion_text(self, prop):
        # Database text properties might be of different types (title etc), so 
        # we get the type before looking for the content; database properties only
        # ever contain a single block, so we're safe to look at index [0]
        type = prop["type"] 
        if (len(prop[type]) > 0):
            return prop[type][0]["plain_text"]
        else: return ""

    def _map_notion_date(self, prop):
        if prop == None:
            return None
        elif prop["date"] == None:
            return None
        return datetime.fromisoformat(prop["date"]["start"])

    def _map_notion_multiselect(self, prop):
        return [ r["name"] for r in prop["multi_select"] ]

    def _map_notion_created_time(self, prop):
        return prop["created_time"]