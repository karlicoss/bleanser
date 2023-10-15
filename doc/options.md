An explanation of the `--multiway`/`--prune-dominated` options, modified from [zulip chat](https://memex.zulipchat.com/#narrow/stream/279601-hpi/topic/bleanser/near/258276779)

Say you had a bunch of sqlite databases and mapped them onto text dumps using `normalise`. The idea is to figure out which dumps are redundant.

Say you've got dumps `C.sql` and `B.sql` -- and you diff them (like literally, [`diff`](https://man7.org/linux/man-pages/man1/diff.1.html))

You have the following cases

- they are exactly the same (`CmpResult.SAME`), so obviously it's safe to remove `A.sql`
- `B.sql` is a superset of `A.sql` (this is `CmpResult.DOMINATES`). In general it's safe to remove `A.sql` in this case, but cause I'm paranoid it's controlled by `delete_dominated`
- `B.sql` isn't a superset of `A.sql`, i.e. some items present in `A` are missing in `B`. (this is `CmpResult.DIFFERENT`). In this case you wanna keep both`A` and `B`. In practice this happens when there is some retention in the database (like with browser history)
- there is also a special value `CmpResult.ERROR`, which also means we want to keep both `A` and `B` (but it's nice to distinguish from `DIFFERENT`)

Now in the simplest case... you just go through all pairs of adjacent files, compute these `CmpResult`s, and end up with smth like this
I'll use `<` for 'dominated', `=` for 'same', `!=` for 'different':

`A < B < C != D = E < G != H != I != J < K != L < M < N`

So in principle, you only need to keep files `C, G, H, I, K, N` and it will still give you a complete set of data when you merge it

Alternatively, you keep `A, C, D, G, H, I, J, K, L, N` if the `delete_dominated` flag is `False`

This is called 'two-way' comparison, cause you just consider pairs of adjacent files, so it would be `MULTIWAY = False`

Multiway comparison; easier to show on an example

Say we've got these sets of items

```
{A B C} # 0
{B C D} # 1
{C D E} # 2
{X Y Z} # 3
```

If we do two-way comparisons, we'll keep them all because none of them fully contains the previous neighbour.

However you may notice that union of `0` and `2` completely contains `1`. This is what 'multiway' mode does -- trying to find 'pivot' elements which contain the sets 'between' them. <https://github.com/karlicoss/bleanser/blob/deae59f956ceb1131ed8f8f3666516f63ad82757/src/bleanser/core/common.py#L31-L41>
