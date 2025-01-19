bleanser or 'backup cleanser' is a tool for cleaning old and redundant backups

## Installing

To install, run: `pip install bleanser`.

There are also extra install options. You can use none or multiple depending on your needs, i.e. `pip install bleanser[flavor1,flavor2]`:

- `bleanser[extra]` : some recommended but optional extras
- `bleanser[json]` : dependencies for JSON based modules
- `bleanser[xml]` : dependencies for XML based modules
- `bleanser[HPI]` : dependencies for [HPI](https://github.com/karlicoss/HPI) based modules

See `optional-dependencies` section in [pyproject.toml](pyproject.toml) if you're curious what libraries these extras pull.

## What bleanser does

In this context, backup typically means something like a GDPR export, an XML or JSON file which includes your data from some website/API, or a sqlite database from an application

<https://beepb00p.xyz/exobrain/projects/bleanser.html>

This is used to find 'redundant backups'. As an example, say you save your data to a JSON file by making API requests to some API service once a day. If your export of the data you exported today is a [superset](https://en.wikipedia.org/wiki/Subset) of the export yesterday, you know you can safely delete the old file and still have a complete backup of your data. This helps:

- save on disk space
- save of data access time; how long it takes to parse all your input files (see [data access layer](https://beepb00p.xyz/exports.html#dal))

This works for both [full](https://beepb00p.xyz/exports.html#full) (you're able to get all your data from a service) and [incremental](https://beepb00p.xyz/exports.html#incremental) exports.

This is especially relevant for incremental data exports, as they're harder to reason about. So, this handles the complex bits of diffing adjacent backups.

As an example of an incremental export, imagine the service you were using only gave you access to the latest 3 items in your history (a real example of this is the [github activity feed](https://github.com/karlicoss/ghexport))

| Day 1 | Day 2 | Day 3 |
| ----- | ----- | ----- |
| A     | B     | C     |
| B     | C     | D     |
| C     | D     | E     |

To parse this in your [data access layer](https://beepb00p.xyz/exports.html#dal), you could imagine something like this:

```python
events = set()
for file in inputs:
    for line in file:
        events.add(line)
# events is now {'A', 'B', 'C', 'D', 'E'}
```

You might notice that if you removed 'Day 2', you'd still have an accurate backup, and we'd still have all 5 items, but its not obvious you can remove it since none of these are supersets of each other.

`bleanser` is meant to solve this problem in a data agnostic way, so any export can be converted to a normalised representation, and those can be compared against each other to find redundant data

Sidenote: in particular this is describing how `--multiway` finds redundant files, see [`options.md`](./doc/options.md) for more info

## How it works

This has `Normaliser`s for different data sources (see [modules](src/bleanser/modules)), and generally follows a pattern like this:

```python
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from bleanser.core.processor import BaseNormaliser, unique_file_in_tempdir

class Normaliser(BaseNormaliser):

    @contextmanager
    def normalise(self, *, path: Path) -> Iterator[Path]:
        # if the input file was compressed, the "path" you receive here will be decompressed

        # a temporary file we write 'normalised' data to, that can be easily diffed/compared
        normalised = unique_file_in_tempdir(input_filepath=path, dir=self.tmp_dir)

        # some custom code here per-module that writes to 'normalised'

        yield normalised


# this script should be run as a module like
# python3 -m bleanser.modules.smscalls --glob ...
if __name__ == "__main__":
    Normaliser.main()
```

This is **always** acting on the data loaded into memory/temporary files, it is not modifying the files itself. Once it determines an input file can be pruned, it will warn you by default, and you can specify `--move` or `--remove` with the CLI (see below) to remove it.

There are particular normalisers for different filetypes, e.g. [`json`](./src/bleanser/core/modules/json.py), [`xml`](./src/bleanser/core/modules/xml.py), [`sqlite`](./src/bleanser/core/modules/sqlite.py) which might work if your data is especially basic, but typically this requires subclassing one of those and writing some custom code to 'cleanup' the data, so it can be properly compared/diffed.

### normalise

There are two ways you can think about `normalise` (creating a 'cleaned'/normalised representation of an input file) -- by specifying an 'upper' or 'lower' bound:

- upper: specify which data you want to drop, dumping everything else to `normalised`
- lower: specify which keys/data you want to keep, e.g. only returning a few keys which uniquely identify events in the data

As an example say you had a JSON export:

```json
[
  { "id": 5, "images": [{}], "href": "..." },
  { "id": 6, "images": [{}], "href": "..." },
  { "id": 7, "images": [{}], "href": "..." }
]
```

When comparing this, you could possibly:

1. Just write the `id` to the file. This is somewhat risky as you don't know if the `href` will always remain the same, so you may be losing data
2. Write the `id` and the `href`, by specifying those two keys you're interested in
3. Write the `id` and the `href`, by deleting the `images` key (this is different from 2!)

There is a trade-off to be made here. For especially noisy exports with lots of metadata that might change over time that you're not interested in, number 3 means every couple months you might have to check and add new keys to delete (as an example see [spotify](./src/bleanser/modules/spotify.py)). This could be seen as a positive as well, as it means when the schema for the API/data changes underneath you, you may notice it quicker

With option 2, you are more likely to remove redundant data files if additional metadata fields are added, and if you only really care about the `id` and `href` and you don't think the export format will change often, this is fine.

Option 3. is generally the safest, but most verbose/tedious, it makes sure you're not removing files that may possibly contain new fields you want to preserve/parse.

Ideally you meet somewhere in the middle, it depends a lot on the specific export data you're comparing.

As it can be a bit difficult to follow, generally this is doing something like:

- Decompress file if its a known compressed format into a `cleaned` file (`unpacked` in [`BaseNormaliser`](./src/bleanser/core/processor.py)), see [`kompress`](https://github.com/karlicoss/kompress/) for supported compression formats
- Creating a temporary file to write data to (`unique_file_in_tempdir` in [`BaseNormaliser`](./src/bleanser/core/processor.py))
- Parse the `cleaned` file into python objects (`JsonNormaliser`, `XmlNormaliser`, or something custom)
- Let the user `cleanup` the data to remove noisy keys/data (specific modules, e.g. [spotify](./src/bleanser/modules/spotify.py))
- Diff those against each other to find and/or remove files which dont contribute new data (module agnostic, run in `main`)

### Subclassing

For example, the JSON normaliser calls a `cleanup` function before it starts processing the data. If you wanted to remove the `images` key like shown above, you could do so like:

```python
from bleanser.core.modules.json import JsonNormaliser, delkeys, Json


class Normaliser(JsonNormaliser):
    # here, j is a dict, each file that this gets passed from the CLI call
    # below is pre-processed by the cleanup function
    def cleanup(self, j: Json) -> Json:
        delkeys(j, keys={
            'images',
        })

        return j


if __name__ == '__main__':
    Normaliser.main()
```

For common formats, the helper classes handle all the tedious bits like loading/parsing data, managing the temporary files. The `Normaliser.main` calls the CLI, which looks like this:

```
 $ python3 -m bleanser.core.modules.json prune --help
Usage: python -m bleanser.core.modules.json prune [OPTIONS] PATH

Options:
  --glob                 Treat the path as glob (in the glob.glob sense)
  --sort-by [size|name]  how to sort input files  [default: name]
  --dry                  Do not prune the input files, just print what would happen after pruning.
  --remove               Prune the input files by REMOVING them (be careful!)
  --move PATH            Prune the input files by MOVING them to the specified path. A bit safer than --remove mode.
  --yes                  Do not prompt before pruning files (useful for cron etc)
  --threads INTEGER      Number of threads (processes) to use. Without the flag won't use any, with the flag will try
                         using all available, can also take a specific value. Passed down to PoolExecutor.
  --from INTEGER
  --to INTEGER
  --multiway             force "multiway" cleanup
  --prune-dominated
  --help                 Show this message and exit.
```

You'd provide input paths/globs to this file, and possibly `--remove` or `--move /tmp/removed` to remove/move files

If you're not able to subclass one of the those, you might be able to subclass [extract](./src/bleanser/core/modules/extract.py), which lets you just yield any sort of string-afiable data, which is then used to diff/compare the input files. For example, if you only wanted to return the `id` and `href` in the JSON example above, you could just return a tuple:

```python
import json
from pathlib import Path
from typing import Iterator, Any

from bleanser.core.modules.extract import ExtractObjectsNormaliser


class Normaliser(ExtractObjectsNormaliser):
    def extract_objects(self, path: Path) -> Iterator[Any]:
        data = json.loads(path.read_text())
        for blob in data:
            yield (blob["id"], blob["href"])


if __name__ == "__main__":
    Normaliser.main()
```

Otherwise if you have some complex data source you need to handle yourself, you can override `do_normalise` and `unpacked` (how the data gets uncompressed/pre-processed) methods yourself, see handling the [discord zip files](https://github.com/purarue/bleanser/blob/master/src/bleanser_pura/modules/discord.py) as an example.
