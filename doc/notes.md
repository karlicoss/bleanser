Random notes which would be moved to proper documentation later.

# builtin modules

Good chances that you'll clean up a lot by using some builtin modules that are agnostic of a speficific data format, i.e.
- `bleanser.modules.binary`

   Considers files identical if they are byte-for-byte identical.
   
- `bleanser.core.modules.sqlite`
- `bleanser.core.modules.json`
- `bleanser.core.xml`


# using multiple threads

To speedup processing, you can use `--threads` option, it will spawn multiple threads to process files in parallel.

The data is split between threads in contiguous chunks, so the files at the chunks boundaries will be processed by different threads.
This means that after pruning and running the command again, you might see more files to prune.
If you really care about pruning every last file, you can run the command again without threads option to process everything in a single thread.


# error handling

If there is an error while processing a file, it will be logged and the file will be skipped.
The errored file will be considered as different from any other file, so it and its 'neighbours' won't be pruned.


# clean up process/writing new module/updating existing module
- generally, try to keep the module backwards compatible, i.e. ideally it should support all previous versions of the data format

process:

- run `-m <module> prune <path_to_data> --dry`
  - if necessary, use `--from` and `--to` options to limit the range of files to process
- if you're happy with the results, run it without `--dry` option
- if you think more files should be pruned, you want to look at the diffs between individual files to see if the diff is legit or due to some garbage data you want to ignore

- it's convenient to simply replace `prune` command with `diff`, it would dump a diff between the first two files in the range

  - it helps to use `--vim` option to open the diff in vimdiff
  - or alternatively, `--diff <difftool>`, for instance `--diff meld` (meld is a gui tool and detects inline diffs in a nicer way)


# running HPI modules
HPI modules are using a somewhat different strategy than 'normal' bleanser modules.

Typically, bleanser aims to only remove 'useless' data -- this is because

- it's safer since different people might have different opinions on what is 'useful'
- even if you know what data is useful, it can be tricky to aggregate it, this basically requires parsing/joining etc, so kind of outside the scope of bleanser

  For instance, in case of `twitter_android` module, it's actually quite hard to figure out what's our own username.

HPI modules, however, only compare HPI output, so only operate on 'useful' data that it extracted from the database.

It can still be useful to run them in case regular bleanser module struggles to remove some data.

To run modules in `hpi` namespace, in case you're running with `uv`, you can use

    uv run --with-editable /path/to/hpi -m bleanser.modules.hpi.<module> ...


# using multiple normalisers
This is convenient if you want to use HPI module and also cross-check it against a 'regular'/agnostic bleanser module. This helps to

- ensure that the 'agnostic' module is not removing data that the HPI module considers useful
- ensure that the 'agnostic' cleans up everything HPI considers useless

  This one might be not completely realistic at all times though.


    uv run --with-editable /code/hpi -m bleanser prune --normaliser bleanser.modules.twitter_android --normaliser bleanser.modules.hpi.twitter_android /path/to/data

You need to specify normalisers from the 'more agnostic' to the 'less agnostic'.

For actual pruning, the 'least agnostic/last' normaliser would be used.
