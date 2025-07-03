A simple CLI tool for stripping MBTiles of data not used by your map style.json.

```
TinyTiles is a CLI tool for minimizing MBTiles. It uses your style JSON to remove unused tile attributes and layers then compresses the tiles using Gzip.

Usage:
  tinytiles [mbtiles] [style.json] [flags]

Flags:
  -g, --gzipped                  The input data is gzipped.
  -h, --help                     help for tinytiles
  -a, --keep-attributes string   Write some Regex to specify which attributes should be kept no matter what.
  -l, --keep-layers string       Write some Regex to specify which layers should be kept no matter what.
  -o, --output string            The output file. (default "output.mbtiles")
```
