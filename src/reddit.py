#!/usr/bin/env python3
from pathlib import Path

from jq_normaliser import JqNormaliser, Filter, pipe, jdel as d, Filter2
from jq_normaliser import CmpResult # eh, just to bring into scope for backup script

from kython.kjq import del_all_kjson


class RedditNormaliser(JqNormaliser):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs, logger_tag='reddit-normaliser', delete_dominated=False, keep_both=True) # type: ignore
        # TODO wonder if there are many dominated ?

    def cleanup(self) -> Filter:
        ignore_keys = (
            'allow_discovery',
            'event_start', 'event_end', 'event_is_live',
            'allowed_galleries',
            'top_awarded_type',
            'treatment_tags',
            # TODO 'edited'?
            'collapsed', 'collapsed_reason', # todo potentially interesting?
            'is_crosspostable_subreddit',
            'og_description', 'og_title',
            'pref_no_profanity', 'pref_geopopular', 'pref_top_karma_subreddits',
            'steward_report',
            'is_video',
            'rte_mode',
            'accept_chats',
            'accept_pms',
            'treatment_tags',
            'password_set',
            'allow_polls',
            'allow_chat_post_creation',
            'is_chat_post_feature_enabled',
            'linked_identities',
            'upvote_ratio',
            'icon_img',
            'icon_size',
            'icon_url',
            'icon_name',

            'thumbnail_height',

            'crosspost_parent_list',
            'primary_color',
            'archived',
            'suggested_sort',
            'over_18',
            'over18',
            'allow_videos',
            'allow_images',
            'allow_videogifs',

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
            'mobile_banner_image',

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
            'gildings',
            'gid_1',
            'gid_2',
            'gid_3',
            'media_metadata',
            'can_assign_link_flair',
            'advertiser_category',
            'can_gild',
            'user_reports',
            'author',
            'author_fullname',
            'report_reasons',
            'discussion_type',
            'allow_live_comments',
            'score_hidden',

            'submit_link_label',
            'submit_text_label',
            'header_title',
            # TODO reuse it in reddit backup script?

            'secure_media',
            'domain',

            'audience_target',
            'free_form_reports',

            'restrict_commenting',
            'restrict_posting',
            'show_media_preview',

            'is_favorited',
            'is_subscriber',

            'oembed',
            'media_embed',
            'secure_media_embed',
            'stickied',
            'owner_id',

            'all_awardings',

            'total_awards_received',

            'likes',
            'send_replies',
            'is_self',

            'url', # ugh. changed from www.reddit.... to link without reddit domain
            '_comments',

            "user_flair_richtext",
            "user_flair_template_id",
            "user_flair_type",
            "user_flair_text_color",
            "associated_award",

            'author_premium',
            'new',
            'awarders',
            'hide_score',
        )
        # TODO ugh, some issue with coins null vs 0??

        # NOTE this step took _really_ long.... e.g. 20 secs vs 0.5 sec for the rest of steps
        # dq.append(jq_del_all(*ignore_keys))

        dq = []
        dq.append('. + if has("inbox") then {} else {"inbox": []} end') # ugh. filling default value
        dq.append(d('.saved[].link_url')) # weird, changes for no reason sometimes...
        sections = [
            'saved',
            'comments',
            'upvoted',
            'downvoted',
            'submissions',
            'inbox',
        ]
        dq.extend([
            d(f'''.{section}[] | (
            .saved, .preview, .body_html, .score, .ups, .description_html, .subreddit_type, .subreddit_subscribers, .selftext_html, .num_comments, .num_crossposts, .thumbnail, .created, .media,
            .locked

            )''') for section in sections
        ])
        dq.append(
            d('.multireddits[] | (.description_html, .created, .owner, .num_subscribers)')
        )
        dq.append(
            d('''(.profile.subreddit, .subreddits[]) | (
              .disable_contributor_requests
            )''')
        )
        dq.append(
            d('''.profile | (
            .created,
            .has_mail,
            .inbox_count,
            .can_create_subreddit,
            .five_follower_send_message,
            .features,
            .has_gold_subscription,
            .has_stripe_subscription,
            .has_paypal_subscription,
            .has_subscribed_to_premium,
            .has_android_subscription,
            .has_ios_subscription,
            .next_coin_drip_date,
            .seen_premium_ftux,
            .seen_premium_adblock_modal,
            .in_redesign_beta,
            .gold_expiration,
            .is_gold
            )'''),
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
        return Filter2(
            jq=pipe(*dq),
            extra_del_all=ignore_keys,
        )

    def extract(self) -> Filter:
        return pipe(
            # TODO FIXME this should be assertive on field existence

            # TODO ehh. dunno about link/comment karma.. it's fuzzy anyway?
            # maybe try removing it once and see the difference

            # hmm, created changes all the time for some reason starting from 20181124201020
            # https://www.reddit.com/r/redditdev/comments/29991t/whats_the_difference_between_created_and_created/ciiuk24/
            # ok, it's broken
            '''.profile      |=     {
                id,
                created_utc,
                name,
                coins,
                comment_karma,
                link_karma,
                subreddit: .subreddit | {subscribers}
            }''',
            '.comments     |= map({id, created_utc, body})',
            '.multireddits |= map({id, created_utc, name, subreddits: .subreddits | map_values(.display_name) })',
            '.saved        |= map({id, created_utc, title, body,  selftext})',
            '.submissions  |= map({id, created_utc, title, selftext})',
            '.subreddits   |= map({id, created_utc, title, display_name, public_description, subreddit_type})',
            '.upvoted      |= map({id, created_utc, title, selftext})',
            '.downvoted    |= map({id, created_utc, title, selftext})',

            '. + if has("inbox") then {} else {"inbox": []} end', # ugh. filling default value

            '.inbox        |= map({id, created_utc, title, selftext, body})',
        )

# 2 styles of normalising:
# first is extracting stuff we expect to see. this is nicer and gives the idea if something actually changed
# second is cleaning up stuff that we don't need




def main():
    norm = RedditNormaliser()
    norm.main(glob='*.json.xz')


if __name__ == '__main__':
    main()
