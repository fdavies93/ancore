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

        return res.json()

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

    def set_table(self, table : TableSpec):
        self.table = table

    def get_columns(self):
        if self.table == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "No table set when reading records from Notion.")
        return self.get_column_spec(self.api_key, self.table.parameters["id"])

    def _read_records(self, limit : int = -1, next_iterator : NotionSyncHandle = None, mapping : DataMap = None) -> NotionSyncHandle:
        if self.table == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "No table set when reading records from Notion.")
        return self.get_records(self.api_key, self.table.parameters["id"], self.get_columns(), number=limit, iterator=next_iterator)

    async def read_records(self, limit: int = -1, next_iterator: NotionSyncHandle = None, mapping: DataMap = None) -> NotionSyncHandle:
        return self._read_records(limit = limit, next_iterator = next_iterator, mapping = mapping)

    def read_records_sync(self, limit: int = -1, next_iterator: NotionSyncHandle = None, mapping: DataMap = None) -> NotionSyncHandle:
        return self._read_records(limit = limit, next_iterator = next_iterator, mapping = mapping)

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

    def get_records(self, api_key: str, id: str, column_info: list, number=100, iterator : NotionSyncHandle = None) -> NotionSyncHandle:
        url = f"https://api.notion.com/v1/databases/{id}/query"
        data = {"page_size": number}
        if iterator is not None: data["start_cursor"] = iterator.handle
        res = requests.post(url, json=data, auth=BearerAuth(api_key), headers={"Notion-Version": notion_version})
        json = res.json()
        it = None
        done = False
        if json["has_more"]:
            it = json["next_cursor"]
        else:
            done = True
        records = [ self._map_record(record, column_info) for record in json["results"] ]
        return NotionSyncHandle( DataSet(column_info, records), DATA_SOURCE.NOTION, handle=it, done=done )

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
    
    def _map_record(self, record:dict, columns:list):
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