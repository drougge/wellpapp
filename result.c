#include "db.h"

void result_free(result_t *result) {
	if (result->posts) free(result->posts);
}

int result_add_post(result_t *result, post_t *post) {
	if (result->room == result->of_posts) {
		if (result->room == 0) {
			result->room = 64;
		} else {
			result->room *= 2;
		}
		post_t **p = realloc(result->posts, result->room * sizeof(post_t *));
		if (!p) return 1;
		result->posts = p;
	}
	result->posts[result->of_posts] = post;
	result->of_posts++;
	return 0;
}

int result_remove_tag(result_t *result, tag_t *tag, truth_t weak) {
	result_t new_result;
	uint32_t i;

	memset(&new_result, 0, sizeof(new_result));
	for (i = 0; i < result->of_posts; i++) {
		post_t *post = result->posts[i];
		if (!post_has_tag(post, tag, weak)) {
			if (result_add_post(&new_result, post)) return 1;
		}
	}
	result_free(result);
	*result = new_result;
	return 0;
}

int result_intersect(result_t *result, tag_t *tag, truth_t weak) {
	result_t new_result;
	memset(&new_result, 0, sizeof(new_result));
	if (result->of_posts) {
		uint32_t i;
		for (i = 0; i < result->of_posts; i++) {
			post_t *post = result->posts[i];
			if (post_has_tag(post, tag, weak)) {
				if (result_add_post(&new_result, post)) {
					return 1;
				}
			}
		}
	} else {
		tag_postlist_t *pl;
		pl = &tag->posts;
		while (pl) {
			uint32_t i;
			for (i = 0; i < TAG_POSTLIST_PER_NODE; i++) {
				if (pl->posts[i]) {
					int r = result_add_post(&new_result,
					                        pl->posts[i]);
					if (r) return 1;
				}
			}
			pl = pl->next;
		}
	}
	result_free(result);
	*result = new_result;
	return 0;
}
