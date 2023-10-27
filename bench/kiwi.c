#include <pthread.h>
#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"

#include "kiwi.h"

#define DATAS ("testdb")

static void _random_key(char *key,int length) {
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz0123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

/**
 * @struct read_data_t
 * @brief Data for read threads
 * @param db -> The DB pointer
 * @param count -> The number of elemets to read
 * @param r -> A flag signaling if we want read randoms or not
 * @param i -> The current index of the retrieve iteration
 * @param key -> A string that stores data taken from the sst
 * @param found -> The number of elements that were found
 */
struct read_data_t {
	DB *db;
	long count;
	long r;
	long i;
	char *key;
	long found;
	long num_of_threads;
	long curr_element;
	long thread_id;

#ifdef BACKGROUND_GET
	pthread_mutex_t self_lock;
#endif
};

/**
 * @struct write_data_t
 * @brief Data for write threads
 * @param db -> The DB pointer
 * @param count -> The number of elemets to insert
 * @param r -> A flag signaling if we want to write randoms or not
 * @param i -> The current index of the insert iteration
 * @param key -> A string used as a key to insert to the sst
 * @param val -> The actual value to store in the sst
 * @param sbuf
 */
struct write_data_t {
	DB *db;
	long count;
	long r;
	long i;
	char *key;
	char *val;
	char *sbuf;
	long num_of_threads;
	long curr_element;
	long thread_id;

#ifdef BACKGROUND_PUT
	pthread_mutex_t self_lock;
#endif
};

static void thread_safe_set_r(struct read_data_t *init, char *value, void *new_value) {
	#ifdef BACKGROUND_GET
		pthread_mutex_lock(&init->self_lock);
	#endif

	if(!strcmp(value, "db"))
		init->db = (DB*)new_value;
	else if(!strcmp(value, "count"))
		init->count = (long)new_value;
	else if(!strcmp(value, "r"))
		init->r = (long)new_value;
	else if(!strcmp(value, "key"))
		init->key = (char*)new_value;
	else if(!strcmp(value, "found"))
		init->found = (long)new_value;
	else if(!strcmp(value, "num_of_threads"))
		init->num_of_threads = (long)new_value;
	else if(!strcmp(value, "curr_element"))
		init->curr_element = (long)new_value;
	else if(!strcmp(value, "thread_id"))
		init->thread_id = (long)new_value;
	
	#ifdef BACKGROUND_GET
		pthread_mutex_unlock(&init->self_lock);
	#endif
}

static void thread_safe_set_w(struct write_data_t *init, char *value, void *new_value) {
	#ifdef BACKGROUND_PUT
		pthread_mutex_lock(&init->self_lock);
	#endif

	if(!strcmp(value, "db"))
		init->db = (DB*)new_value;
	else if(!strcmp(value, "count"))
		init->count = (long)new_value;
	else if(!strcmp(value, "r"))
		init->r = (long)new_value;
	else if(!strcmp(value, "key"))
		init->key = (char*)new_value;
	else if(!strcmp(value, "val"))
		init->val = (char*)new_value;
	else if(!strcmp(value, "sbuf"))
		init->sbuf = (char*)new_value;
	else if(!strcmp(value, "num_of_threads"))
		init->num_of_threads = (long)new_value;
	else if(!strcmp(value, "curr_element"))
		init->curr_element = (long)new_value;
	else if(!strcmp(value, "thread_id"))
		init->thread_id = (long)new_value;
	
	#ifdef BACKGROUND_PUT
		pthread_mutex_unlock(&init->self_lock);
	#endif
}

static void *thread_safe_get_r(struct read_data_t *init, char *value) {
	void *ret_value = NULL;

	#ifdef BACKGROUND_GET
		pthread_mutex_lock(&init->self_lock);
	#endif

	if(!strcmp(value, "db"))
		ret_value = init->db; 
	else if(!strcmp(value, "count"))
		ret_value = (void*)init->count;
	else if(!strcmp(value, "r"))
		ret_value = (void*)init->r;
	else if(!strcmp(value, "key"))
		ret_value = init->key; 
	else if(!strcmp(value, "found"))
		ret_value = (void*)init->found;
	else if(!strcmp(value, "num_of_threads"))
		ret_value = (void*)init->num_of_threads;
	else if(!strcmp(value, "curr_element"))
		ret_value = (void*)init->curr_element;
	else if(!strcmp(value, "thread_id"))
		ret_value = (void*)init->thread_id;
	
	#ifdef BACKGROUND_GET
		pthread_mutex_unlock(&init->self_lock);
	#endif

	return ret_value;
}

static void *thread_safe_get_w(struct write_data_t *init, char *value) {
	void *ret_value = NULL;

	#ifdef BACKGROUND_GET
		pthread_mutex_lock(&init->self_lock);
	#endif

	if(!strcmp(value, "db"))
		ret_value = init->db; 
	else if(!strcmp(value, "count"))
		ret_value = (void*)init->count;
	else if(!strcmp(value, "r"))
		ret_value = (void*)init->r;
	else if(!strcmp(value, "key"))
		ret_value = init->key;
	else if(!strcmp(value, "val"))
		ret_value = init->val;
	else if(!strcmp(value, "sbuf"))
		ret_value = init->sbuf;
	else if(!strcmp(value, "num_of_threads"))
		ret_value = (void*)init->num_of_threads;
	else if(!strcmp(value, "curr_element"))
		ret_value = (void*)init->curr_element;
	else if(!strcmp(value, "thread_id"))
		ret_value = (void*)init->thread_id;
	
	#ifdef BACKGROUND_GET
		pthread_mutex_unlock(&init->self_lock);
	#endif

	return ret_value;
}

/**
 * @func: get_read_initializers
 * @brief Get the read initializers struct objects
 * @param count -> The number of elements to read
 * @param r -> A flag signaling if we want to read randoms or not
 * @return struct read_data_t* -> A struct with all needed values initialzed
 */
static struct read_data_t *get_read_initializers(long int count, int r) {
	struct read_data_t *ret = (struct read_data_t*)malloc(sizeof(struct read_data_t));

	// char key[KSIZE + 1];
	/* TODO -> COMMENT ABOUT THIS #2 */
	char *key = (char*)malloc(sizeof(char) * (KSIZE + 1));

	ret->db = NULL;
	ret->count = count;
	ret->r = r;
	ret->i = 0;
	ret->key = key;
	ret->found = 0;
	ret->num_of_threads = 1;

	/* TODO -> SET INITIAL AS 0 */
	ret->thread_id = 0;

	return ret;
}

/**
 * @func: read_single_element
 * @brief Performs the GET evaluation once
 * @param init -> The initializer struct
 * @return int -> 1 if the current element is found else 0
 */
static int read_single_element(struct read_data_t *init) {
	int ret;
	Variant sk;
	Variant sv;

	memset(init->key, 0, KSIZE + 1);

	/* if you want to test random write, use the following */
	if (init->r)
		_random_key(init->key, KSIZE);
	else
		snprintf(init->key, KSIZE, "key-%ld", init->curr_element);
	fprintf(stderr, "%ld searching %s\n", init->curr_element, init->key);
	sk.length = KSIZE;
	sk.mem = init->key;

	ret = db_get(init->db, &sk, &sv);

	/* Return 1 if db_get is successful */
	if (ret)
		return 1;
	else
		INFO("not found key#%s", sk.mem);
	return 0;
}

/**
 * @func: execute_and_time_read
 * @brief Executes multiple read operations in a loop
 * @param init -> The initializer struct
 * @return double -> The total time counted for the loop
 */
static double execute_and_time_read(struct read_data_t *init) {
	long i;
	long found = 0;
	long long start,end;
	long buffer_size = init->count / init->num_of_threads;
	long curr_range = init->thread_id;

	start = get_ustime_sec();
		for(i = curr_range; i < (curr_range + 1) * buffer_size; i++) {
			init->curr_element = i;
			/* Increase the total ammount */
			found += read_single_element(init);
			
			if ((i % 10000) == 0) {
				fprintf(stderr,"random read finished %ld ops%30s\r", i, "");
				fflush(stderr);
			}
		}
	end = get_ustime_sec();

	/* Sets found for the final display */
	init->found = found;

	/* Return the total cost */
	return end - start;
}

/**
 * @func: display_read_costs
 * @brief Displays costs and time taken for the read operations
 * @param cost -> The total time taken for the loop to complete
 * @param count -> The number of elements that was read
 * @param found -> The number of elements found
 */
static void display_read_costs(double cost, long int count, long found) {
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%ld): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
}

/**
 * @func: get_write_initializers
 * @brief Get the write initializer objects struct
 * @param count -> The number of elements to insert
 * @param r -> A flag signaling if we want to insert randoms or not
 * @return struct write_data_t* -> A struct with all needed values initialized
 */
static struct write_data_t *get_write_initializers(long int count, int r) {
	struct write_data_t *ret = (struct write_data_t*)malloc(sizeof(struct write_data_t));

	// char key[KSIZE + 1];
	// char val[VSIZE + 1];
	// char sbuf[1024];
	/* TODO -> COMMENT ABOUT THIS #1 */
	char *key = (char*)malloc(sizeof(char) * (KSIZE + 1));
	char *val = (char*)malloc(sizeof(char) * (VSIZE + 1));
	char *sbuf = (char*)malloc(1024);

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	ret->db = NULL;
	ret->count = count;
	ret->r = r;
	ret->i = 0;
	ret->key = key;
	ret->key = val;
	ret->sbuf = sbuf;
	ret->num_of_threads = 1;
	/* TODO -> SET INITIAL AS 0 */
	ret->thread_id = 0;

	return ret;
}

/**
 * @func: write_single_element
 * @brief Performs a write operation once
 * @param init -> The initializer struct
 */
static void write_single_element(struct write_data_t *init) {
	Variant sk, sv;

	if (init->r)
		_random_key(init->key, KSIZE);
	else
		snprintf(init->key, KSIZE, "key-%ld", init->curr_element);

	fprintf(stderr, "%ld adding %s\n", init->curr_element, init->key);
	/* TODO -> BUS ERROR */
	// snprintf(init->val, VSIZE, "val-%ld", init->i);

	sk.length = KSIZE;
	sk.mem = init->key;
	sv.length = VSIZE;

	/* TODO -> COMMENT ABOUT THIS #3 */
	// sv.mem = init->val;
	char *sbmemstring = (char*)malloc(32);
	sbmemstring = "value";
	sv.mem = sbmemstring;

	db_add(init->db, &sk, &sv);
}

/**
 * @func: execute_and_time_write
 * @brief Executes multiple write operations in a loop
 * @param init -> The initializer struct
 * @return double -> The total time counted for the loop
 */
static double execute_and_time_write(struct write_data_t *init) {
	long i;
	long long start, end;
	long buffer_size = init->count / init->num_of_threads;
	long curr_range = init->thread_id;

	start = get_ustime_sec();
		for(i = curr_range; i < (curr_range + 1) * buffer_size; i++) {
			init->curr_element = i;
			write_single_element(init);

			if ((i % 10000) == 0) {
				fprintf(stderr,"random write finished %ld ops%30s\r", i, "");
				fflush(stderr);
			}
		}
	end = get_ustime_sec();

	return end - start;
}

/**
 * @func: display_write_costs
 * @brief Displays costs and time taken for the write operations
 * @param cost -> The total time taken for the loop to complete
 * @param count -> The number of elements that was write
 */
static void display_write_costs(double cost, long int count) {
	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}






/**
 * @func: _read_test
 * @brief Multiple reads test
 * @param count -> The number of elements that we want to read
 * @param r -> A flag indicating whether we want randoms or not
 */
void _read_test(long int count, int r) {
	/* Final cost */
	double cost;

	/* Grab a struct of all values we need */
	struct read_data_t *initializers = get_read_initializers(count, r);

	/* Open db and execute the writes */
	/* Makes sure opening and closing the db happens in 1 thread */
	initializers->db = db_open(DATAS);
		/* Gather the costs */
		cost = execute_and_time_read(initializers);
	db_close(initializers->db);

	display_read_costs(cost, count, initializers->found);
}

/**
 * @func: _write_test
 * @brief Multiple writes test
 * @param count -> The number of elements that we want to write
 * @param r -> A flag indicating whether we want randoms or not
 */
void _write_test(long int count, int r) {
	/* Final cost */
	double cost;

	/* Grab a struct of all values we need */
	struct write_data_t *initializers = get_write_initializers(count, r);

	/* Open db and execute the writes */
	/* Makes sure opening and closing the db happens in 1 thread */
	initializers->db = db_open(DATAS);
		cost = execute_and_time_write(initializers);
	db_close(initializers->db);

	display_write_costs(cost, count);
}

/**
 * @func: _readwrite_test_2_threads
 * @brief Allocates 2 threads, 1 for reading and 1 for writing
 * @param count -> The number of elements that we want to read
 * @param r -> A flag indicating whether we want randoms or not
 */
void _readwrite_test_2_threads(long int count, int r) {
	/* Gather costs seperately */
	double read_cost;
	double write_cost;
	
	/* Get initializers */
	struct read_data_t *read_initializers = get_read_initializers(count, r);
	struct write_data_t *write_initializers = get_write_initializers(count, r);

	/* Initialize threads */
	#ifdef BACKGROUND_GET
		pthread_mutex_init(&read_initializers->self_lock, NULL);
	#endif
	#ifdef BACKGROUND_PUT
		pthread_mutex_init(&write_initializers->self_lock, NULL);
	#endif

	/* Point to the same db, so that we avoid multiple databases */
	read_initializers->db = db_open(DATAS);
	write_initializers->db = read_initializers->db;

	/* Try to test for concurrent writes and reads on 2 threads */
	pthread_t t[2];
    pthread_create(&t[0], NULL, (void *(*)(void*))execute_and_time_write, (void*)write_initializers);
    pthread_create(&t[1], NULL, (void *(*)(void*))execute_and_time_read, (void*)read_initializers);

	pthread_join(t[0], (void*)&write_cost);
    pthread_join(t[1], (void*)&read_cost);

	/* Make sure we close only once AFTER joins */
	db_close(read_initializers->db);

	/* Display each of the costs */
	display_read_costs(read_cost, count, read_initializers->found);
	display_write_costs(write_cost, count);

	printf("Combined cost:%.3f(sec)\n", read_cost + write_cost);

	/* Real cost is calculated monitoring the termination of the longest thread */
	printf("Total cost:%.3f(sec)\n", (read_cost > write_cost) ? read_cost : write_cost);
}

void _readwrite_test_X_threads(long int count, int r, int num_of_threads, int read_percentage, int xx) {
	if(xx) num_of_threads *= 2;

	/* Number for iterators */
	long ri;
	long wi;

	/* Gather costs seperately */
	double read_cost;
	double write_cost;

	/* Get initializers */
	struct read_data_t *r_init = get_read_initializers(count, r);
	struct write_data_t *w_init = get_write_initializers(count, r);

	/* Initialize threads */
	#ifdef BACKGROUND_GET
		pthread_mutex_init(&r_init->self_lock, NULL);
	#endif
	#ifdef BACKGROUND_PUT
		pthread_mutex_init(&w_init->self_lock, NULL);
	#endif

	/* Offload the processes according to percentage */
	int num_of_read_threads = num_of_threads * read_percentage / 100;
	int num_of_write_threads = num_of_threads - num_of_read_threads;
	pthread_t *t_read = (pthread_t*)malloc(sizeof(pthread_t) * num_of_read_threads);
	pthread_t *t_write = (pthread_t*)malloc(sizeof(pthread_t) * num_of_write_threads);

	/* Set data for threads according to calculated percentage */
	r_init->num_of_threads = num_of_read_threads;
	w_init->num_of_threads = num_of_write_threads;

	/* Point to the same db, so that we avoid multiple databases */
	r_init->db = db_open(DATAS);
	w_init->db = r_init->db;

	/* Start the cocurrent processing */
	for(wi = 0; wi < num_of_write_threads; wi++) {
		thread_safe_set_w(w_init, "thread_id", (void*)wi);
		pthread_create(&t_write[wi], NULL, (void *(*)(void*))execute_and_time_write, (void*)w_init);
	}
	for(ri = 0; ri < num_of_read_threads; ri++) {
		thread_safe_set_r(r_init, "thread_id", (void*)ri);
		pthread_create(&t_read[ri], NULL, (void *(*)(void*))execute_and_time_read, (void*)r_init);
	}

	/* Join all operations */
	for(wi = 0; wi < num_of_write_threads; wi++)
		pthread_join(t_write[wi], (void*)&write_cost);
	for(ri = 0; ri < num_of_read_threads; ri++)
		pthread_join(t_read[ri], (void*)&read_cost);

	/* Make sure we close only once AFTER joins */
	db_close(r_init->db);



	/* Display each of the costs */
	printf("\n\nUSED - %ld threads for reading and %ld for writing\n", r_init->num_of_threads, w_init->num_of_threads);

	display_read_costs(read_cost, count, r_init->found);
	display_write_costs(write_cost, count);

	printf("Combined cost:%.3f(sec)\n", read_cost + write_cost);

	/* Real cost is calculated monitoring the termination of the longest thread */
	printf("Total cost:%.3f(sec)\n", (read_cost > write_cost) ? read_cost : write_cost);
}
