[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/fd93)
[![License: CC BY-NC-SA 4.0](https://img.shields.io/badge/License-CC_BY--NC--SA_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc-sa/4.0/)

# ⚓ AnCore

AnCore (Anchor Core or Analysis Core) is a pure Python ETL library which provides some useful data structures similar to Pandas' dataframes and a synchronisation layer to connect with different applications.

It also lets you convert between different types of data columns which are commonly found in apps - for example, between DateTime fields and strings.

Provides the data manipulation and synchronisation layers which [Anchor](https://github.com/fdavies93/anki-anchor) builds on.

Getting started is easy:

```
api_key = "YOUR_NOTION_API_KEY"
db_id = "YOUR_NOTION_DB_ID"

reader = NotionReader(api_key)
table = reader.get_table(db_id)
reader.set_table(table)
dataset = reader.read_records_sync(10).records

writer = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "FILE PATH HERE"}, "test_read"))
writer.create_table_sync(dataset)
```

## Currently Supported Sync Targets

* Notion
* JSON files
* TSV files

## How to Test Ancore

There are two different ways to test Ancore. You can either run automated tests (this takes some setup) or test via the CLI (this is more convenient and faster to write).

### Testing Via The CLI

There is a basic CLI in the tests folder which allows access to some of the key features in the library. At the moment you can:
* Update Notion databases from JSON or TSV files
* Read from Notion databases to JSON or TSV files

Make sure you put the CLI one folder *above* the core folder to ensure modules import correctly. The best way to try the CLI is to try it:

``` ./cli.py read -i notion YOUR_NOTION_DATABASE_ID -o tsv YOUR_FILE_PATH.tsv --secret YOUR_NOTION_API_KEY ```

If you get a merge error when trying to update records in Notion, you might need to remap columns to be the correct type (this is often true when updating from TSVs as TSVs do not hold metadata.) For example:

``` ./cli.py update -i tsv ./test_input/chinese_sample.tsv -o notion YOUR_DATABASE_TABLE -m "Last Created" date -m Subtags multiselect -m Tags multiselect -m Timestamp date --primary_key Hanzi --secret YOUR_NOTION_API_KEY ```

### Running Unit Tests

First, run setup.command to setup the appropriate Python module structure. *If the directory structure is incorrect most tests will fail.*

If you want to set up the folder structure manually, move everything into a subfolder named core and copy the contents of tests to the root folder.

Next, set up your config.json file with a valid Notion API key.

Finally, run the unit tests using [coverage](https://coverage.readthedocs.io/en/6.2/). You can use run_tests.command (zsh) or run_tests.sh (bash) if you want to keep the repository in line with unit test data or the simpler 

``` coverage run unit_test.py ```

if you just want to run the unit tests.

## FAQ

### Why Isn't Anki Listed As A Sync Target?

Unlike most sync targets, Anki cannot be interacted with headlessly. It's necessary to run some type of GUI to test Anki because it relies on the QT framework.

This makes testing Anki with the rest of the core somewhat inefficient and hacky, as you have to run a desktop and boot up the Anki app to test it. To avoid this, we've moved support for Anki synchronisation to the [Anchor](https://github.com/fdavies93/anki-anchor) project, which builds on this one.

### Why Didn't You Use Pandas?

The underlying code for Pandas is C-based. This means that a Pandas install needs to be recompiled for different operating systems and architectures.

AnCore was originally developed to work with Anki plugins, which are system-independent. To prevent end users from having to install and debug Pandas, we chose to create a basic Python implementation of the data analysis structures needed.

### A Note on Efficiency

As above, this library is built for portability rather than efficiency. Where the code can be made more efficient within the constraints of pure Python we've tried to do so, but certain operations available in other data analysis libraries (most notably memcpy and similar) cannot be replicated in Python.