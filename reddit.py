#!/usr/bin/env python3
from pathlib import Path

from jq_normaliser import JqNormaliser, Filter, pipe, jdel as d, jq_del_all


class RedditNormaliser(JqNormaliser):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs, logger_tag='reddit-normaliser', delete_dominated=False, keep_both=True) # type: ignore
        # TODO wonder if there are many dominated ?

    def cleanup(self) -> Filter:
        dq = []
        ignore_keys = [
            'icon_img',
            'icon_size',
            'icon_url',

            'thumbnail_height',

            'crosspost_parent_list',
            'primary_color',
            'archived',
            'suggested_sort',
            'over_18',
            'over18',
            'allow_videos',
            'comment_score_hide_mins',
            'wiki_enabled',
            'suggested_sort',
            'suggested_comment_sort',
            'header_img',
            'header_size',
            'has_menu_widget',
            'banner_background_color',
            'banner_background_image',
            'banner_img',
            'banner_size',

            'community_icon',
            'no_follow',
            'submission_type',
            'is_crosspostable',

            'link_flair_enabled',
            'link_flair_position',
            'link_flair_css_class',
            'link_flair_template_id',
            'link_flair_text',
            'link_flair_type',
            'link_flair_richtext',

            'post_hint',
            'is_robot_indexable',
            'content_categories',

            'parent_whitelist_status',
            'pwls',
            'whitelist_status',
            'wls',
            'show_media',
            'spoilers_enabled',
            'collapse_deleted_comments',
            'key_color',
            'can_assign_user_flair',
            'emojis_enabled',
            'author_patreon_flair',
            "author_flair_richtext",
            'author_flair_text',
            'author_flair_background_color',
            'author_flair_text_color',
            'author_flair_type',
            'author_flair_css_class',
            'author_flair_template_id',

            "original_content_tag_enabled",
            'emojis_custom_size',

            'gilded',
            'gid_1',
            'gid_2',
            'gid_3',
            'media_metadata',
            'can_assign_link_flair',
            'advertiser_category',
        ]
        dq.append(jq_del_all(*ignore_keys))
        sections = [
            'saved',
            'comments',
            'upvoted',
            'downvoted',
            'submissions',
        ]
        dq.extend([
            d(f'.{section}[] | (.preview, .body_html, .score, .ups, .description_html, .subreddit_type, .subreddit_subscribers, .selftext_html, .num_comments, .num_crossposts, .thumbnail, .created)') for section in sections
        ])
        dq.append(
            d('.multireddits[] | (.description_html, .created, .owner, .num_subscribers)')
        )
        # del_preview = lambda s: ddel(f'.{s} | .[]')
        # dq.extend(del_preview(s) for s in sections)
        # TODO shit, that's gonna remove lots of subreddits
        # I should also check that result contains reasonable favorites??
        # TODO not sure if it's necessary to sort.. | sort_by(.id) 
        # dq.append('.subreddits | map(del(.subscribers, .videostream_links_count, .description_html))') # ddel('(.subreddits) | .)') #  | del(.videostream_links_count) | del(.description_html)
        dq.extend([
            d('.subreddits[] | (.created, .subscribers, .description, .description_html, .videostream_links_count, .submit_text, .submit_text_html)'),
        ])
        return pipe(*dq)

    def extract(self) -> Filter:
        return pipe(
            # TODO FIXME this should be assertive on field existence

            # hmm, created changes all the time for some reason starting from 20181124201020
            # https://www.reddit.com/r/redditdev/comments/29991t/whats_the_difference_between_created_and_created/ciiuk24/
            # ok, it's broken

            '.comments     |= map({id, created_utc, body})',
            '.multireddits |= map({id, created_utc, name, subreddits: .subreddits | map_values(.display_name) })',
            '.saved        |= map({id, created_utc, body})',
            '.submissions  |= map({id, created_utc, title, selftext})',
            '.subreddits   |= map({id, created_utc, title, display_name, public_description, subreddit_type})',
            '.upvoted      |= map({id, created_utc, title, selftext})',
            '.downvoted    |= map({id, created_utc, title, selftext})',

            # '.comments     |= []',
            # '.multireddits |= []',
            # '.profile      |= {}',
            # '.saved        |= []',
            # '.submissions  |= []',
            # '.subreddits   |= []',
            # '.upvoted      |= []',
        )

# 2 styles of normalising:
# first is extracting stuff we expect to see. this is nicer and gives the idea if something actually changed
# second is cleaning up stuff that we don't need




def main():
    norm = RedditNormaliser()
    norm.main(glob='*.json.xz')


if __name__ == '__main__':
    main()
