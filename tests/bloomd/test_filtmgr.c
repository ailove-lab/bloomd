#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>
#include "config.h"
#include "filter.h"
#include "filter_manager.h"

START_TEST(test_mgr_init_destroy)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, &mgr);
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_create_drop)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "foo1", NULL);
    fail_unless(res == 0);

    res = filtmgr_drop_filter(mgr, "foo1");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_list)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "bar1", NULL);
    fail_unless(res == 0);
    res = filtmgr_create_filter(mgr, "bar2", NULL);
    fail_unless(res == 0);

    bloom_filter_list_head *head;
    res = filtmgr_list_filters(mgr, &head);
    fail_unless(res == 0);

    int has_bar1 = 0;
    int has_bar2 = 0;

    bloom_filter_list *node = head->head;
    while (node) {
        if (strcmp(node->filter_name, "bar1") == 0)
            has_bar1 = 1;
        else if (strcmp(node->filter_name, "bar2") == 0)
            has_bar2 = 1;
        node = node->next;
    }
    fail_unless(has_bar1);
    fail_unless(has_bar2);

    res = filtmgr_drop_filter(mgr, "bar1");
    fail_unless(res == 0);
    res = filtmgr_drop_filter(mgr, "bar2");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_add_check_keys)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "zab1", NULL);
    fail_unless(res == 0);

    char *keys[] = {"hey","there","person"};
    char result[] = {0, 0, 0};
    res = filtmgr_set_keys(mgr, "zab1", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    for (int i=0;i<3;i++) result[i] = 0;
    res = filtmgr_check_keys(mgr, "zab1", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    res = filtmgr_drop_filter(mgr, "zab1");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_check_no_keys)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "zab2", NULL);
    fail_unless(res == 0);

    char *keys[] = {"hey","there","person"};
    char result[] = {1, 1, 1};
    res = filtmgr_check_keys(mgr, "zab2", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(!result[0]);
    fail_unless(!result[1]);
    fail_unless(!result[2]);

    res = filtmgr_drop_filter(mgr, "zab2");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST
