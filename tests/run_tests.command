#! /usr/bin/env zsh
cp ./unit_test.py core/tests/unit_test.py
cp -r ./test_input/* core/tests/test_input
coverage run unit_test.py