#!/bin/bash

echo "Downloading event_datafile_new.csv..."

curl -L -o data/event_datafile_new.csv \
  "https://video.udacity-data.com/topher/2019/January/5c4a7e3a_event-datafile-new/event-datafile-new.csv"

if [ $? -eq 0 ]; then
    echo "✓ Download completed successfully"
    echo "File size: $(du -h data/event_datafile_new.csv | cut -f1)"
    echo "Line count: $(wc -l < data/event_datafile_new.csv)"
else
    echo "✗ Download failed"
    exit 1
fi
