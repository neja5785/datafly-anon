#! /bin/sh

for file in /path/to/files/*; do
    psql mytemplate1 -f "$file"
done
