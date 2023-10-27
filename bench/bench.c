#include "bench.h"

static void _print_header(int count) {
    double index_size = (double)((double)(KSIZE + 8 + 1) * count) / 1048576.0;
    double data_size = (double)((double)(VSIZE + 4) * count) / 1048576.0;

    printf("Keys:\t\t%d bytes each\n", KSIZE);
    printf("Values: \t%d bytes each\n", VSIZE);
    printf("Entries:\t%d\n", count);
    printf("IndexSize:\t%.1f MB (estimated)\n", index_size);
    printf("DataSize:\t%.1f MB (estimated)\n", data_size);

    printf(LINE1);
}

static void _print_environment(void) {
    time_t now = time(NULL);

    printf("Date:\t\t%s", (char*)ctime(&now));

    int num_cpus = 0;
    char cpu_type[256] = {0};
    char cache_size[256] = {0};

    FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
    if(cpuinfo) {
        char line[1024] = {0};
        while(fgets(line, sizeof(line), cpuinfo) != NULL) {
            const char *sep = strchr(line, ':');
            if(sep == NULL || strlen(sep) < 10)
                continue;

            char key[1024] = {0};
            char val[1024] = {0};
            strncpy(key, line, sep - 1 - line);
            strncpy(val, sep + 1, strlen(sep) - 1);
            if(strcmp("model name", key) == 0) {
                num_cpus++;
                strcpy(cpu_type, val);
            }
            else if(strcmp("cache size", key) == 0)
                strncpy(cache_size, val + 1, strlen(val) - 1);
        }

        fclose(cpuinfo);
        printf("CPU:\t\t%d * %s", num_cpus, cpu_type);
        printf("CPUCache:\t%s\n", cache_size);
	}
}

/**
 * @func: usage
 * @brief Prints the sample usage
 */
static void usage(void) {
    fprintf(stderr, "Usage: db-bench <write | read | readwrite-2 | readwrite-x | readwrite-per> <count> <num_of_threads> <read_percentage>\n");
    exit(1);
}

/**
 * @func: print_banner_and_calculate_count
 * @brief Prints the banner of info and gets the integer form of count
 * @param argv -> The array of command line arguments
 * @return long -> count
 */
static long print_banner_and_calculate_count(char **argv) {
    long count = atoi(argv[2]);
    _print_header(count);
    _print_environment();
    return count;
}

/**
 * @func: generates_random_keys
 * @brief Checks for generation of random keys
 * @param argc -> The number of command line arguments
 * @param argv -> The array of command line arguments
 */
static int generates_random_keys(int argc, char **argv) {
#ifdef RANDOM_INSERTS
    return 1;
#else
    return 0;
#endif
}

int main(int argc,char** argv) {
    /**
     * @param count -> Number of insertions or retrievals
     * @param r -> A flag signaling whether the keys should be random or not
     */
    long int count;
    int r = 0;

    /* Randomize hash output */
    srand(time(NULL));

    if (argc < 3)
        usage();

    count = print_banner_and_calculate_count(argv);
    r = generates_random_keys(argc, argv);

    /* Options */
    if(!strcmp(argv[1], "write"))
        _write_test(count, r);
    else if(!strcmp(argv[1], "read"))
        _read_test(count, r);
    else if(!strcmp(argv[1], "readwrite-2"))
        _readwrite_test_2_threads(count, r);
    else if(!strcmp(argv[1], "readwrite-x")) {
        if (argc < 4)
            usage();
        _readwrite_test_X_threads(count, r, atoi(argv[3]), 50, 1);
    }
    else if(!strcmp(argv[1], "readwrite-per")) {
        if (argc < 5)
            usage();
        _readwrite_test_X_threads(count, r, atoi(argv[3]), atoi(argv[4]), 0);
    }
    else
        usage();

    return 0;
}
