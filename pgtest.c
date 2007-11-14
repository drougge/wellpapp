#include "db.h"

#include <time.h>
#include <libpq-fe.h>

#define MAX_TAGS  409600
#define MAX_POSTS 204800

connection_t *logconn;

static void add_tag(const char *name, tag_t *tag) {
	tag->name = mm_strdup(name);
	if (rbtree_insert(tagtree, tag, rbtree_str2key(name))) {
		assert(0);
	}
	if (rbtree_insert(tagguidtree, tag, tag->guid.key)) {
		assert(0);
	}
}

static void add_tagalias(const char *name, tag_t *tag) {
	rbtree_key_t hash = rbtree_str2key(name);
	tagalias_t   *alias;
	alias = mm_alloc(sizeof(*alias));
	alias->name = mm_strdup(name);
	alias->tag  = tag;
	if (rbtree_insert(tagaliastree, alias, hash)) {
		assert(0);
	}
}

static time_t time_str2unix(const char *str) {
	struct tm tm;
	char *p;
	memset(&tm, 0, sizeof(tm));
	p = strptime(str, "%Y-%m-%d %H:%M:%S", &tm);
	assert(p && !*p);
	return mktime(&tm);
}

typedef struct {
	const char *ext;
	int        filetype;
} ext2ft_t;
static int ext2filetype(const char *ext) {
	int i;
	const ext2ft_t map[] = {
		{"GIF", FILETYPE_GIF},
		{"JPG", FILETYPE_JPEG},
		{"PNG", FILETYPE_PNG},
		{"Png", FILETYPE_PNG},
		{"bmp", FILETYPE_BMP},
		{"gif", FILETYPE_GIF},
		{"jpeg", FILETYPE_JPEG},
		{"jpg", FILETYPE_JPEG},
		{"png", FILETYPE_PNG},
		{"swf", FILETYPE_FLASH},
		{NULL, 0}
	};
//  1 avi html mp3 mp4 mpg pdf php?attachmentid=280808&d=1155260176 php?fn=147089 rar svg wmv zip
	for (i = 0; map[i].ext; i++) {
		if (!strcmp(map[i].ext, ext)) return map[i].filetype;
	}
	return -1;
}

static uint16_t danboorurating2rating(const char *dr) {
	int r;
	switch(*dr) {
		case 's': r = str2id("safe", rating_names); break;
		case 'q': r = str2id("questionable", rating_names); break;
		case 'e': r = str2id("explicit", rating_names); break;
		default:  r = str2id("unspecified", rating_names); break;
	}
	assert(r > 0);
	return r - 1;
}

/* Fix posts with broken width/height (bmp images) */
typedef struct {
	const char *md5str;
	uint16_t   width, height;
} broken_post_data_t;

broken_post_data_t broken_post_data[] = {
	{"3004609224859d8005b01290ba9a690a", 235, 512},
	{"2539ae3440d6a8cb15545c0c4de256cd", 235, 510},
	{"8e8a958f492a0ff1979c60dcd75acab9", 704, 480},
	{"af8fd64f5c04fed6d4e848a79f499d0f", 408, 488},
	{"cff7c95f30f27b1ddc2bebb25897fca7", 1024, 1280},
	{"9ad2dbd20caceeadd9a83280aba6de8d", 551, 358},
	{"ccd2a9fafa9b6f508f2a58a91b620d2a", 641, 361},
	{"816b3ed42624360b4c8f018909d9dc5e", 500, 600},
	{"c5c3c841d7d476cfc6b4f4324f3d2c31", 1280, 960},
	{"71e699b3f97023794a410a06c64a9f5e", 1075, 1518},
	{"450db53eec817966ced737fcc97e4c9e", 1600, 1200},
	{"a88435bd63f0d725a624c95d59f43a9f", 600, 424},
	{"c9a848207c71ea6e614ac2f46000ea05", 800, 600},
	{"00ac602f7f8d4796724ef76c3d04dbf4", 330, 409},
	{"53606453638108e4bb2b61774055fc76", 640, 480},
	{"f5f40a8ad68f1e319a6cbab49a488e1f", 1000, 1453},
	{"1f10ba1f4cfeba910a69c215804c0c1b", 1800, 1400},
	{"bbdc0e5f2c9375e55d7f962f815a3e83", 700, 900},
	{"96fb276c9ccbfe6249c3a3d31787bc77", 529, 693},
	{"59f146fb22f9793cd11d9dd05be87f57", 1024, 792},
	{"0a5c4ffec2c1b02a2beb6ca3fc5bf1a0", 656, 565},
	{"f4a15c7c53a3c0046151290299bc6d60", 577, 753},
	{"59e8d697dc040deb2fa471cdae14c62c", 544, 604},
	{"2377e156b933382ebf5b6f263cfb37f4", 804, 1206},
	{"719676762330f39a4757e07050f4b6be", 417, 658},
	{"292517122ab190eca55783c153cd9f97", 800, 533},
	{"a3126696e34943ccae390633ed74f093", 397, 497},
	{"02fb051ae6719d835548f45c516e29a1", 640, 480},
	{"8385bfa070012f606167c23aba3e2dae", 341, 698},
	{"a961bf9772ae9bad7bbcc71fdc1858c2", 800, 600},
	{"d3446631a5f8e3064690d699691d025a", 431, 600},
	{"deb8ad64a71a1e5b063f24649c9907bb", 1024, 768},
	{"65863778f8083827001d6a6d6ea8911f", 593, 273},
	{"0894316c238f4cda34dd7ce6e3cad2e4", 476, 750},
	{"bba8bca859f473b39d5f3fc83326bc92", 800, 600},
	{"da4ce0830ccd10d3e0f5cb7385c434a8", 100, 100},
	{"c938097aaeb441a1dae14c68c633eb03", 468, 722},
	{"23cfc4013da617969bc65de9d6bfe05f", 798, 596},
	{"f03a12056581b41004567f1c5ec33411", 800, 600},
	{"208a871a7949af26a03ac3c67cb6e3ca", 802, 800},
	{"98da893543466773dbdedc857e704821", 800, 600},
	{"0dd8dc188a6165559380c5605b5f192f", 611, 838},
	{"7d57b46157c426112ad58730591451af", 510, 564},
	{"01843e925fd3a56fff6c18b8c54b6ee5", 1280, 960},
	{"d636c80dd623e13f17793319ee9d78e3", 847, 1200},
	{"423f8a2505d9f587d31ecc3c617a2d82", 838, 1200},
	{"fd71dce3be1f73abde37e8dbd3be58ed", 1024, 768},
	{"655114a4bce3b171ed29fa61a14befc0", 463, 551},
	{"4b436075254d5d24bb2996c38eeafa96", 640, 480},
	{"ad81483c3fc0c429f297b02e622693a2", 800, 600},
	{"7c8c3dc05221c33d1e79cff5acb0f256", 787, 578},
	{"6d2aaa3cb1e103dda5597ccb1075cc4a", 1280, 960},
	{"dd55f7f40be2147bc8826165da2d961f", 433, 480},
	{"926f4377c9a4614bf9c8d4124e4422bf", 640, 480},
	{"1b0f25fe30a32958058bba2546e6e978", 388, 571},
	{"68b794fad34308682401f272141c8d72", 454, 575},
	{"019ee49680b8017870c6e0c5bd80001e", 640, 904},
	{"4227922de41d5806f8379f3512b54fb5", 742, 600},
	{"0de6e83c9e60e39059a70ea198d04fff", 640, 477},
	{"90b6421f4229784d23a67ea83ee4d286", 800, 600},
	{"1c3f01bd9b24970f69c0f91126f199ad", 360, 460},
	{"4f63343446d3a82386ecc378df892966", 600, 450},
	{"1d84c082cc26e1ebfad09245ab537fc9", 640, 480},
	{"0f5ed8e216cc655bafbdd90fa250921f", 1280, 960},
	{"48f841ff10e0fa6aa66d423f3dfc2bcb", 456, 718},
	{"e1929763891476f47ca564f5625be6ec", 160, 178},
	{"d4056255d1694a428df65d5bc25a2bcf", 800, 600},
	{"d68ddc762708d28f2aa172bf4511bee5", 540, 725},
	{"3c401f256e4bce52ca7a7031690110e4", 640, 480},
	{"146ed554149bb2367c39c8ed582ab001", 698, 481},
	{"6b39af7877649aa77b084477a2eadb47", 1230, 1784},
	{"1be4566e1723a6f8be4866f4beb2f256", 600, 450},
	{"1826110488e51883140167bf6c039fad", 643, 480},
	{"920e8791d2d3661d43ed5b19f7e67537", 1024, 768},
	{"07efc9edbe5c0a3dc09813d714579b22", 343, 480},
	{"31b98ccf74a2bd95fcc95bfb5f985b43", 1152, 864},
	{"0946641d5bf99e3d8de8a818a4c95a68", 1152, 864},
	{"65d0fa9b2ea01234d9386759e8bb8526", 375, 562},
	{"fbc403559ccd8e8ebee694132144ee07", 494, 750},
	{"afedf3793a2da6e7989793027127aece", 1000, 1471},
	{"4c38148b520ae70288e763474a877209", 640, 480},
	{"91944d0096a8810de855c7ee5ea72d15", 353, 479},
	{"d96c65f237b1eac368207f25764300a4", 740, 911},
	{"92ac334bc285d593cd19a25f41701d8f", 520, 578},
	{"ffe3a886f67039adfde2b818bb653319", 1600, 1200},
	{"09287c0910358685b0e4b18976749c97", 842, 595},
	{"d157c9878ab96c31f356b71e4cd9184f", 427, 640},
	{"1a2200c15bddf845dbd5d0d9b77d5465", 600, 450},
	{"cdfee247aa5e357f130141df448ab13d", 1280, 1024},
	{"7849af84b8096f4119f58bc547956145", 665, 962},
	{"f585c923e9d4140337b71a05f8c6234a", 632, 402},
	{"0bac3802870305f62db48e322c5c9046", 800, 600},
	{"812ab990b1ba4c080574438ef8f65e79", 800, 600},
	{"1bbf7ebccd1076a36252ee61f6211b30", 640, 480},
	{"0a8644894b0b7a1fb47abea69a6eed3f", 360, 590},
	{"47778d19f4a32a8d67260b0eaaee6dab", 800, 600},
	{"581faf99a3b0a52a951b6d9335f67540", 336, 420},
	{"a9f0a255f9ca789ee466ad5f6fac17a7", 640, 480},
	{"b7c6a43a8c6cf43a42520b05c09f3a3a", 800, 600},
	{"76b17546d3406da390f47943b768e985", 533, 400},
	{"4320ee3c21b4e57e5d37773abb4a2e91", 482, 700},
	{"543f3e21e9a48062c6ab2c57357b4e20", 900, 634},
	{NULL, 0, 0}
};

static void fix_broken_post(const char *md5str, post_t *post) {
	broken_post_data_t *data = broken_post_data;

	while (data->md5str) {
		if (!strcmp(data->md5str, md5str)) {
			post->width  = data->width;
			post->height = data->height;
			return;
		}
		data++;
	}
	assert(post->width && post->height);
}

static int dummy_error(connection_t *conn, const char *msg) {
	(void)conn;
	(void)msg;
	return 1;
}

static int populate_from_db(PGconn *conn) {
	PGresult *res = NULL;
	int r = 0;
	int cols, rows;
	int i;
	tag_t  **tags  = NULL;
	post_t **posts = NULL;
	uint16_t danboorutype2type[5];
	const char *danboorutype2type_str[] = {
		"unspecified",
		"artist",
		"ambiguous",
		"copyright",
		"character",
		NULL
	};

	for (i = 0; danboorutype2type_str[i]; i++) {
		int d2t = str2id(danboorutype2type_str[i], tagtype_names);
		assert(d2t > 0);
		danboorutype2type[i] = d2t - 1;
	}

	/* drougge/apa */
	r = prot_add(logconn, strdup("UNZHJvdWdnZQAA Cmkuser Cdelete PYXBh Cmodcap"));
	assert(!r);
	tags  = calloc(MAX_TAGS , sizeof(void *));
	posts = calloc(MAX_POSTS, sizeof(void *));
	res = PQexec(conn, "SELECT id, created_at, user_id, score, source, md5, width, height, file_ext, rating FROM posts");
	err(!res, 2);
	err(PQresultStatus(res) != PGRES_TUPLES_OK, 3);
	rows = PQntuples(res);
	cols = PQnfields(res);
	printf("posts: %d %d\n", rows, cols);
	for (i = 0; i < rows; i++) {
		post_t *post;
		char   *source;
		int    filetype;

		filetype = ext2filetype(PQgetvalue(res, i, 8));
		if (filetype < 0) continue; // @@
		post = mm_alloc(sizeof(*post));
		post->created  = time_str2unix(PQgetvalue(res, i, 1));
		post->score    = atol(PQgetvalue(res, i, 3));
		r = md5_str2md5(&post->md5, PQgetvalue(res, i, 5));
		assert(!r);
		post->width    = atol(PQgetvalue(res, i, 6));
		post->height   = atol(PQgetvalue(res, i, 7));
		post->rating   = danboorurating2rating(PQgetvalue(res, i, 9));
		post->filetype = filetype;
		source = PQgetvalue(res, i, 4);
		if (source && *source) {
			post->source = mm_strdup(source);
		} else {
			post->source = NULL;
		}
		posts[atol(PQgetvalue(res, i, 0))] = post;
		fix_broken_post(PQgetvalue(res, i, 5), post);
		if (rbtree_insert(posttree, post, *(rbtree_key_t *)post->md5.m)) {
			assert(0);
		}
	}
	PQclear(res);
	res = NULL;
	printf("whee..\n");
	/* 31316K 27228K här */

	res = PQexec(conn, "SELECT post_id, tag_id FROM posts_tags");
	err(!res, 2);
	err(PQresultStatus(res) != PGRES_TUPLES_OK, 3);
	rows = PQntuples(res);
	cols = PQnfields(res);
	printf("posts_tags: %d %d\n", rows, cols);
	for (i = 0; i < rows; i++) {
		tag_t    *tag;
		tag_id_t tag_id;

		tag_id = atol(PQgetvalue(res, i, 1));
		assert(tag_id < MAX_TAGS);
		tag = tags[tag_id];
		if (!tag) {
			tag = mm_alloc(sizeof(*tag));
			tag->guid = guid_gen_tag_guid();
			tags[tag_id]  = tag;
		}
if (!posts[atol(PQgetvalue(res, i, 0))]) {
printf("Tag %d on post %s has no post\n",tag_id, PQgetvalue(res, i, 0));
} else
		r = post_tag_add(posts[atol(PQgetvalue(res, i, 0))], tag, T_NO);
		if (r) {
			printf("WARN: post %s already tagged as %d?\n", PQgetvalue(res, i, 0), tag_id);
		}
	}
	PQclear(res);
	res = NULL;
	printf("whee..\n");
	/* 38912K 34268K här */

	res = PQexec(conn, "SELECT id, name, tag_type FROM tags");
	err(!res, 2);
	err(PQresultStatus(res) != PGRES_TUPLES_OK, 3);
	rows = PQntuples(res);
	cols = PQnfields(res);
	printf("tags: %d %d\n", rows, cols);
	for (i = 0; i < rows; i++) {
		tag_t    *tag;
		tag_id_t tag_id;

		tag_id = atol(PQgetvalue(res, i, 0));
		assert(tag_id < MAX_TAGS);
		tag = tags[tag_id];
		if (tag) {
			const char *name = PQgetvalue(res, i, 1);
			tag->type = danboorutype2type[atol(PQgetvalue(res, i, 2))];
			/* The aliases suggest this might be what it's supposed to be called. */
			if (!*name) name = "awesome_female";
			add_tag(name, tag);
		}
	}
	PQclear(res);
	res = NULL;
	printf("whee..\n");

	res = PQexec(conn, "SELECT a.name, t.name FROM tag_aliases a, tags t WHERE a.alias_id = t.id");
	err(!res, 2);
	err(PQresultStatus(res) != PGRES_TUPLES_OK, 3);
	rows = PQntuples(res);
	printf("tag_aliases: %d\n", rows);
	for (i = 0; i < rows; i++) {
		tag_t        *tag;
		const char   *name;

		name = PQgetvalue(res, i, 0);
		tag  = tag_find_name(PQgetvalue(res, i, 1));
		if (tag) {
			add_tagalias(name, tag);
		} else {
			printf("WARN: tag-alias '%s' has no tag\n", name);
		}
	}
	PQclear(res);
	res = NULL;
	printf("whee..\n");
err:
	if (res  ) PQclear(res);
	if (tags ) free(tags);
	if (posts) free(posts);
	return r;
}

int main(int argc, char **argv) {
	int          r = 0;
	user_t       loguser_;
	connection_t logconn_;
	int          mm_r;

	loguser_.name = "LOG-READER";
	loguser_.caps = ~0;
	logconn = &logconn_;
	memset(logconn, 0, sizeof(*logconn));
	logconn->user  = &loguser_;
	logconn->error = dummy_error;
	logconn->sock  = -1;
	logconn->flags = CONNFLAG_LOG;
	logconn->trans.conn = logconn;

	assert(argc == 2);
	db_read_cfg(argv[1]);
	printf("initing mm..\n");
	mm_r = mm_init();
	assert(mm_r == 1);
	printf("populating from db..\n");
	PGconn *conn = PQconnectdb("user=danbooru");
	err(!conn, 2);
	err(PQstatus(conn) != CONNECTION_OK, 2);
	err(populate_from_db(conn), 3);
	mm_print();
	/*
	printf("mapd   %p\nstackd %p\nheapd  %p.\n", (void *)posttree, (void *)&conn, (void *)malloc(4));
	*/
	printf("Cleaning up mm..\n");
	mm_cleanup();
err:
	return r;
}
