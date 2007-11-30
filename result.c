#include "db.h"

void result_free(connection_t *conn, result_t *result) {
	if (result->posts) {
		c_free(conn, result->posts, result->room * sizeof(post_t *));
	}
}

int result_add_post(connection_t *conn, result_t *result, post_t *post) {
	if (result->room == result->of_posts) {
		unsigned int old_size;
		if (result->room == 0) {
			old_size = 0;
			result->room = 64;
		} else {
			old_size = result->room * sizeof(post_t *);
			result->room *= 2;
		}
		int r = c_realloc(conn, (void **)&result->posts, old_size,
		                  result->room * sizeof(post_t *));
		if (r) return 1;
	}
	result->posts[result->of_posts] = post;
	result->of_posts++;
	return 0;
}

int result_remove_tag(connection_t *conn, result_t *result,
                      tag_t *tag, truth_t weak) {
	result_t new_result;
	uint32_t i;

	memset(&new_result, 0, sizeof(new_result));
	for (i = 0; i < result->of_posts; i++) {
		post_t *post = result->posts[i];
		if (!post_has_tag(post, tag, weak)) {
			if (result_add_post(conn, &new_result, post)) return 1;
		}
	}
	result_free(conn, result);
	*result = new_result;
	return 0;
}

int result_intersect(connection_t *conn, result_t *result,
                     tag_t *tag, truth_t weak) {
	result_t new_result;
	memset(&new_result, 0, sizeof(new_result));
	if (result->of_posts) {
		uint32_t i;
		for (i = 0; i < result->of_posts; i++) {
			post_t *post = result->posts[i];
			if (post_has_tag(post, tag, weak)) {
				if (result_add_post(conn, &new_result, post)) {
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
					int r = result_add_post(conn, &new_result,
					                        pl->posts[i]);
					if (r) return 1;
				}
			}
			pl = pl->next;
		}
	}
	result_free(conn, result);
	*result = new_result;
	return 0;
}
