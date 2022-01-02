#!/usr/bin/env python3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import json


from bleanser.core.json import delkeys
from bleanser.core.processor import BaseNormaliser
from bleanser.core.utils import Json


REDDIT_IGNORE_KEYS = {
    ## TODO hmm maybe do something about these
    ## might be useful to keep
    'subreddit_subscribers',
    'subscribers',
    'ups',
    'score',
    'num_comments',
    'upvote_ratio',
    ###

    ## TODO ??
    'pwls', # TODO what is it??
    'likes', # todo?
    'wls', # TODO ???
    ##

    '_comments',
    'accept_chats',
    'accept_pms',
    'advertiser_category',
    'all_awardings',
    'allow_chat_post_creation',
    'allow_discovery',
    'allow_images',
    'allow_live_comments',
    'allow_polls',
    'allow_videogifs',
    'allow_videos',
    'allowed_galleries',
    'archived',
    'associated_award',
    'audience_target',
    'author_flair_background_color',
    'author_flair_css_class',
    'author_flair_richtext',
    'author_flair_template_id',
    'author_flair_text',
    'author_flair_text_color',
    'author_flair_type',
    'author_patreon_flair',
    'author_premium',
    'awarders',
    'banner_background_color',
    'banner_background_image',
    'banner_img',
    'banner_size',
    'can_assign_link_flair',
    'can_assign_user_flair',
    'can_gild',
    'collapse_deleted_comments',
    'collapsed', 'collapsed_reason', # todo potentially interesting?
    'comment_score_hide_mins',
    'community_icon',
    'content_categories',
    'crosspost_parent_list',
    'dash_url',
    'discussion_type',
    'emojis_custom_size',
    'emojis_enabled',
    'event_start', 'event_end', 'event_is_live',
    'free_form_reports',
    'gid_1',
    'gid_2',
    'gid_3',
    'gilded',
    'gildings',
    'has_menu_widget',
    'header_img',
    'header_size',
    'header_title',
    'hide_score',
    'hls_url',
    'icon_img',
    'icon_name',
    'icon_size',
    'icon_url',
    'is_chat_post_feature_enabled',
    'is_crosspostable',
    'is_crosspostable_subreddit',
    'is_robot_indexable',
    'is_self',
    'is_video',
    'key_color',
    'link_flair_css_class',
    'link_flair_enabled',
    'link_flair_position',
    'link_flair_richtext',
    'link_flair_template_id',
    'link_flair_text',
    'link_flair_type',
    'linked_identities',
    'media_embed',
    'media_metadata',
    'mobile_banner_image',
    'new',
    'no_follow',
    'oembed',
    'og_description', 'og_title',
    'original_content_tag_enabled',
    'over18',
    'over_18',
    'owner_id',
    'parent_whitelist_status', # some ads thing
    'password_set',
    'post_hint',
    'post_hint',
    'pref_no_profanity', 'pref_geopopular', 'pref_top_karma_subreddits',
    'primary_color',
    'pwls',
    'report_reasons',
    'restrict_commenting',
    'restrict_posting',
    'rte_mode',
    'score_hidden',
    'secure_media',
    'secure_media_embed',
    'send_replies',
    'show_media',
    'show_media_preview',
    'spoilers_enabled',
    'steward_report',
    'stickied',
    'submission_type',
    'submit_link_label',
    'submit_text_label',
    'suggested_comment_sort',
    'suggested_sort',
    'suggested_sort',
    'thumbnail',
    'thumbnail_height',
    'thumbnail_width',
    'top_awarded_type',
    'total_awards_received',
    'treatment_tags',
    'treatment_tags',
    'user_flair_richtext',
    'user_flair_template_id',
    'user_flair_text_color',
    'user_flair_type',
    'user_reports',
    'videostream_links_count',
    'whitelist_status',
    'whitelist_status', # some ads thing
    'wiki_enabled',


    # TODO ??
    # 'likes',
    # 'url', # ugh. changed from www.reddit.... to link without reddit domain
    # 'is_favorited',
    # 'is_subscriber',
    # 'domain',
}


class Normaliser(BaseNormaliser):
    # filter out additions; keep the rest
    DIFF_FILTER =  '> '

    def cleanup(self, j: Json) -> Json:
        delkeys(j, keys=REDDIT_IGNORE_KEYS)
        return j

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        with self.unpacked(path=path, wdir=wdir) as upath:
            pass
        del path # just to prevent from using by accident

        j = json.loads(upath.read_text())
        j = self.cleanup(j)

        # todo copy paste from SqliteNormaliser
        jpath = upath.absolute().resolve()
        cleaned = wdir / Path(*jpath.parts[1:]) / (jpath.name + '-cleaned')
        cleaned.parent.mkdir(parents=True, exist_ok=True)

        with cleaned.open('w') as fo:
            if isinstance(j, list):
                j = {'<toplevel>': j} # meh

            assert isinstance(j, dict), j
            for k, v in j.items():
                if not isinstance(v, list):
                    # something like 'profile' data in hypothesis could be a dict
                    # something like 'notes' in rescuetime could be a scalar (str)
                    v = [v] # meh
                assert isinstance(v, list), (k, v)
                for i in v:
                    print(f'{k} ::: {json.dumps(i, sort_keys=True)}', file=fo)

        yield cleaned


if __name__ == '__main__':
    Normaliser.main()
