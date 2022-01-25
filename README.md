[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/fd93)
[![License: CC BY-NC-SA 4.0](https://img.shields.io/badge/License-CC_BY--NC--SA_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc-sa/4.0/)

# ⚓ AnCore

AnCore (Anchor Core or Analysis Core) is a pure Python ETL library which provides some useful data structures similar to Pandas' dataframes and a synchronisation layer to connect with different applications.

It also lets you convert between different types of data columns which are commonly found in apps - for example, between DateTime fields and strings.

Provides the data manipulation and synchronisation layers which [Anchor](https://github.com/fdavies93/anki-anchor) builds on.

### Currently Supported Sync Targets

* Notion (nearly done)
* JSON files
* .TSV files

### How to Test Ancore

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