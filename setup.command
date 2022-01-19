#! /usr/bin/env zsh
setopt extendedglob
setopt dotglob
mkdir core
# move everything to subdirectory (to get proper Python module structure)
mv ^tests* ./core
cp -r ./tests/* ./
# unpack tests
mv ./tests ./core
# finally, move them back to main folder