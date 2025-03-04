#!/bin/bash

directory="$1"

# Find all files and sort them alphabetically
files=$(find "$directory" -type f | sort)

# Initialize an empty file to store the concatenated data
temp_file=$(mktemp)

# Concatenate all files incrementally
for file in $files; do
    cat "$file" >> "$temp_file"
done

# Compute the MD5 hash of the concatenated data
md5sum "$temp_file" | cut -d ' ' -f 1

# Remove the temporary file
rm "$temp_file"
