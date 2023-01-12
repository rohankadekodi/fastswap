#if !defined(_SSWAP_RDMA_H)
#define _SSWAP_RDMA_H

#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <linux/inet.h>
#include <linux/module.h>
#include <linux/list.h>
#include <linux/mm_types.h>
#include <linux/gfp.h>
#include <linux/pagemap.h>
#include <linux/spinlock.h>

enum qp_type {
  QP_READ_SYNC,
  QP_READ_ASYNC,
  QP_WRITE_SYNC
};

enum request_type {
	PAGE_FAULT,
	PAGE_EVICT
};

struct sswap_rdma_dev {
  struct ib_device *dev;
  struct ib_pd *pd;
};

struct sswap_ib_phys_buf {
	u64 addr;
	u64 size;
};

struct rdma_req {
  struct completion done;
  struct list_head list;
  struct ib_cqe cqe;
  u64 dma;
  struct page *page;
};

/*
 * These states are used to signal events between the completion handler
 * and the main client or server thread.
 *
 * Once CONNECTED, they cycle through RDMA_READ_ADV, RDMA_WRITE_ADV, 
 * and RDMA_WRITE_COMPLETE for each ping.
 */
enum test_state {
	IDLE = 1,
	CONNECT_REQUEST,
	ADDR_RESOLVED,
	ROUTE_RESOLVED,
	CONNECTED,
	RDMA_READ_ADV,
	RDMA_READ_COMPLETE,
	RDMA_WRITE_ADV,
	RDMA_WRITE_COMPLETE,
  	RDMA_REQUESTED,
  	RDMA_RECEIVED,
	RDMA_RESPONDED,
	ERROR
};

struct sswap_rdma_info {
	uint64_t buf;
	uint32_t rkey;
	uint32_t size;
	uint64_t remote_offset;
	enum request_type request_type;
};

struct sswap_pf_rdma_info {
	uint64_t buf;
	uint32_t rkey;
	uint32_t size;
	uint64_t remote_offset;
	enum request_type request_type;
};

struct sswap_evict_rdma_info {
	uint64_t buf;
	uint32_t rkey;
	uint32_t size;
	uint64_t remote_offset;
	enum request_type request_type;
	uint8_t page_content[4096];
};

/*
 * Control block struct.
 */
struct sswap_cb {
	int server;			/* 0 iff client */
	struct ib_cq *cq;
	struct ib_pd *pd;
	struct ib_qp *qp;
	spinlock_t s_lock;

	struct ib_mr *dma_mr;

	struct ib_fast_reg_page_list *page_list;
	int page_list_len;
	struct ib_reg_wr reg_mr_wr;
	struct ib_send_wr invalidate_wr;
	struct ib_mr *reg_mr;
	int server_invalidate;
	int read_inv;
	u8 key;

	struct ib_recv_wr rq_wr;	/* recv work request record */
	struct ib_sge recv_sgl;		/* recv single SGE */
	struct sswap_evict_rdma_info recv_buf __aligned(16);	/* malloc'd buffer */
	u64 recv_dma_addr;
	DEFINE_DMA_UNMAP_ADDR(recv_mapping);

	struct ib_send_wr sq_wr;	/* send work requrest record */
	struct ib_sge send_sgl;
	struct ib_send_wr sq_pf_wr;	/* send work requrest record */
	struct ib_sge send_pf_sgl;
	struct ib_send_wr sq_evict_wr;	/* send work requrest record */
	struct ib_sge send_evict_sgl;
	struct sswap_rdma_info send_buf __aligned(16); /* single send buf */
	struct sswap_pf_rdma_info send_pf_buf __aligned(16); /* single send buf */
	struct sswap_evict_rdma_info send_evict_buf __aligned(16); /* single send buf */
	u64 send_dma_addr;
	u64 send_pf_dma_addr;
	u64 send_evict_dma_addr;
	DEFINE_DMA_UNMAP_ADDR(send_mapping);
	DEFINE_DMA_UNMAP_ADDR(send_pf_mapping);
	DEFINE_DMA_UNMAP_ADDR(send_evict_mapping);

	struct ib_rdma_wr rdma_sq_wr;	/* rdma work request record */
	struct ib_sge rdma_sgl;		/* rdma single SGE */
	char *rdma_buf;			/* used as rdma sink */
	u64  rdma_dma_addr;
	DEFINE_DMA_UNMAP_ADDR(rdma_mapping);
	struct ib_mr *rdma_mr;

	uint32_t remote_rkey;		/* remote guys RKEY */
	uint64_t remote_addr;		/* remote guys TO */
	uint32_t remote_len;		/* remote guys LEN */

	char *start_buf;		/* rdma read src */
	u64  start_dma_addr;
	DEFINE_DMA_UNMAP_ADDR(start_mapping);
	struct ib_mr *start_mr;

	enum test_state state;		/* used for cond/signalling */
	wait_queue_head_t sem;

	uint16_t port;			/* dst port in NBO */
	u8 addr[16];			/* dst addr in NBO */
	char ip6_ndev_name[128];	/* IPv6 netdev name */
	char *addr_str;			/* dst addr string */
	uint8_t addr_type;		/* ADDR_FAMILY - IPv4/V6 */
	int verbose;			/* verbose logging */
	int count;			/* ping count */
	int size;			/* ping data size */
	int validate;			/* validate ping data */
	int wlat;			/* run wlat test */
	int rlat;			/* run rlat test */
	int bw;				/* run bw test */
	int duplex;			/* run bw full duplex test */
	int poll;			/* poll or block for rlat test */
	int txdepth;			/* SQ depth */
	int local_dma_lkey;		/* use 0 for lkey */
	int frtest;			/* reg test */
	int tos;			/* type of service */

	/* CM stuff */
	struct rdma_cm_id *cm_id;	/* connection on client side,*/
					/* listener on server side. */
	struct rdma_cm_id *child_cm_id;	/* connection on server side */
	struct list_head list;
	int cm_error;
	struct completion cm_done;
};

struct sswap_rdma_ctrl;

struct rdma_queue {
	struct ib_qp *qp;
	struct ib_cq *cq;
	spinlock_t cq_lock;
	enum qp_type qp_type;

	struct sswap_rdma_ctrl *ctrl;

	struct rdma_cm_id *cm_id;
	int cm_error;
	struct completion cm_done;

	atomic_t pending;
};

struct sswap_rdma_memregion {
    u64 baseaddr;
    u32 key;
};

struct sswap_rdma_ctrl {

	struct sswap_rdma_dev *rdev; // TODO: move this to queue
	struct rdma_queue *queues;
	struct sswap_rdma_memregion servermr;
	spinlock_t s_lock;

	struct sswap_cb *cbs;

	union {
		struct sockaddr addr;
		struct sockaddr_in addr_in;
	};

	union {
		struct sockaddr srcaddr;
		struct sockaddr_in srcaddr_in;
	};
};

struct rdma_queue *sswap_rdma_get_queue(unsigned int idx, enum qp_type type);
enum qp_type get_queue_type(unsigned int idx);
int sswap_rdma_read_async(struct page *page, u64 roffset);
int sswap_rdma_read_sync(struct page *page, u64 roffset);
int sswap_new_rdma_read_sync(struct page *page, u64 roffset);
int sswap_rdma_write(struct page *page, u64 roffset);
int sswap_new_rdma_write(struct page *page, u64 roffset);
int sswap_rdma_poll_load(int cpu);

enum timing_category {
	read_title_t,
	read_t,
	read_async_t,

	write_title_t,
	write_t,

	TIMING_NUM,
};

extern int measure_timing;

extern const char *Timingstring[TIMING_NUM];
extern u64 Timingstats[TIMING_NUM];
DECLARE_PER_CPU(u64[TIMING_NUM], Timingstats_percpu);
extern u64 Countstats[TIMING_NUM];
DECLARE_PER_CPU(u64[TIMING_NUM], Countstats_percpu);

typedef struct timespec timing_t;

#define INIT_TIMING(X) timing_t X = {0}

#define FASTSWAP_START_TIMING(name, start) \
	{if (measure_timing) getrawmonotonic(&start); }

#define FASTSWAP_END_TIMING(name, start) \
	{if (measure_timing) { \
		INIT_TIMING(end); \
		getrawmonotonic(&end); \
		__this_cpu_add(Timingstats_percpu[name], \
			(end.tv_sec - start.tv_sec) * 1000000000 + \
			(end.tv_nsec - start.tv_nsec)); \
	} \
	__this_cpu_add(Countstats_percpu[name], 1); \
	}

void fastswap_get_timing_stats(void);
void fastswap_print_timing_stats(void);
void fastswap_clear_timing_stats(void);

#endif
