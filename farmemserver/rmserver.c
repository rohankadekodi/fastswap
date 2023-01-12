#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <linux/types.h>
#include <semaphore.h>
#include <infiniband/verbs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <rdma/rsocket.h>
#include <infiniband/ib.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

const size_t BUFFER_SIZE = 1024 * 1024 * 1024 * 32l;
const size_t CB_BUFFER_SIZE = 1024 * 1024 * 4l;
const unsigned int NUM_PROCS = 4;
const unsigned int NUM_QUEUES_PER_PROC = 3;
const unsigned int NUM_QUEUES = NUM_PROCS * NUM_QUEUES_PER_PROC;
const size_t PAGE_SIZE = 4096;

void *far_memory;

struct device {
	struct ibv_pd *pd;
	struct ibv_context *verbs;
};

struct queue {
	struct ibv_qp *qp;
	struct ibv_cq *cq;
	struct rdma_cm_id *cm_id;
	struct ctrl *ctrl;
	enum {
		INIT,
		CONNECTED
	} state;
};

/*
 * Default max buffer size for IO...
 */
#define RMSERVER_BUFSIZE 64*1024
#define RMSERVER_SQ_DEPTH 16

/* Default string for print data and
 * minimum buffer size
 */
#define _stringify( _x ) # _x
#define stringify( _x ) _stringify( _x )

#define RMSERVER_MSG_FMT           "rdma-ping-%d: "
#define RMSERVER_MIN_BUFSIZE       sizeof(stringify(INT_MAX)) + sizeof(RMSERVER_MSG_FMT)

struct rmserver_rdma_info {
	__be64 buf;
	__be32 rkey;
	__be32 size;
	__be64 remote_offset;
	__be32 request_type;
	uint8_t data[PAGE_SIZE];
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
	RDMA_RESPONSE_STARTED,
	RDMA_RESPONSE_SENT,
	DISCONNECTED,
	ERROR
};

enum request_type {
	PAGE_FAULT,
	PAGE_EVICT
};

/*
 * Control block struct.
 */
struct rmserver_cb {
	int server;			/* 0 iff client */
	pthread_t cqthread;
	pthread_t test_thread;
	pthread_t persistent_server_thread;
	struct ibv_comp_channel *channel;
	struct ibv_cq *cq;
	struct ibv_pd *pd;
	struct ibv_qp *qp;

	struct ibv_recv_wr rq_wr;	/* recv work request record */
	struct ibv_sge recv_sgl;	/* recv single SGE */
	struct rmserver_rdma_info recv_buf;/* malloc'd buffer */
	struct ibv_mr *recv_mr;		/* MR associated with this buffer */

	struct ibv_send_wr sq_wr;	/* send work request record */
	struct ibv_sge send_sgl;
	struct rmserver_rdma_info send_buf;/* single send buf */
	struct ibv_mr *send_mr;

	struct ibv_send_wr rdma_sq_wr;	/* rdma work request record */
	struct ibv_sge rdma_sgl;	/* rdma single SGE */
	char *rdma_buf;			/* used as rdma sink */
	struct ibv_mr *rdma_mr;

	uint32_t remote_rkey;		/* remote guys RKEY */
	uint64_t remote_addr;		/* remote guys TO */
	uint32_t remote_len;		/* remote guys LEN */

	char *start_buf;		/* rdma read src */
	struct ibv_mr *start_mr;

	enum test_state req_state;		/* used for cond/signalling */
	sem_t sem;

	struct sockaddr_storage sin;
	struct sockaddr_storage ssource;
	__be16 port;			/* dst port in NBO */
	int verbose;			/* verbose logging */
	int self_create_qp;		/* Create QP not via cma */
	int count;			/* ping count */
	int size;			/* ping data size */
	int validate;			/* validate ping data */

	__be64 remote_offset;
	enum request_type request_type;

	/* CM stuff */
	pthread_t cmthread;
	struct rdma_event_channel *cm_channel;
	struct rdma_cm_id *cm_id;	/* connection on client side,*/
					/* listener on service side. */
	struct rdma_cm_id *child_cm_id;	/* connection on server side */

	enum {
    	INIT,
    	CONNECTED
  	} state;

	struct ctrl *ctrl;
	void *buffer;
	void *send_buffer;
	struct ibv_mr *mr_buffer;
	struct ibv_mr *mr_send_buffer;
	volatile int req_done;
};

struct ctrl {
	struct rmserver_cb *cbs;
	struct queue *queues;
	struct ibv_mr *mr_buffer;
	struct ibv_mr *mr_send_buffer;
	void *buffer;
	void *send_buffer;
	struct device *dev;

	struct ibv_comp_channel *comp_channel;
};

struct memregion {
  uint64_t baseaddr;
  uint32_t key;
};


static void die(const char *reason);

static int alloc_control();
static int on_connect_request(struct rdma_cm_id *id, struct rdma_conn_param *param);
static int on_connection(struct rmserver_cb *cb);
static int on_disconnect(struct rmserver_cb *cb);
static int on_event(struct rdma_cm_event *event);
static void destroy_device(struct ctrl *ctrl);

static struct ctrl *gctrl = NULL;
static unsigned int queue_ctr = 0;

// static int rmserver_cma_event_handler(struct rdma_cm_id *cma_id,
// 				    struct rdma_cm_event *event)
// {
// 	int ret = 0;
// 	struct rmserver_cb *cb = (struct rmserver_cb *)cma_id->context;

// 	switch (event->event) {
// 	case RDMA_CM_EVENT_ADDR_RESOLVED:
// 		cb->req_state = ADDR_RESOLVED;
// 		ret = rdma_resolve_route(cma_id, 2000);
// 		if (ret) {
// 			cb->req_state = ERROR;
// 			perror("rdma_resolve_route");
// 			sem_post(&cb->sem);
// 		}
// 		break;

// 	case RDMA_CM_EVENT_ROUTE_RESOLVED:
// 		cb->req_state = ROUTE_RESOLVED;
// 		sem_post(&cb->sem);
// 		break;

// 	case RDMA_CM_EVENT_CONNECT_REQUEST:
// 		cb->req_state = CONNECT_REQUEST;
// 		cb->child_cm_id = cma_id;
// 		sem_post(&cb->sem);
// 		break;

// 	case RDMA_CM_EVENT_CONNECT_RESPONSE:
// 		cb->req_state = CONNECTED;
// 		sem_post(&cb->sem);
// 		break;

// 	case RDMA_CM_EVENT_ESTABLISHED:
// 		sem_post(&cb->sem);
// 		break;

// 	case RDMA_CM_EVENT_ADDR_ERROR:
// 	case RDMA_CM_EVENT_ROUTE_ERROR:
// 	case RDMA_CM_EVENT_CONNECT_ERROR:
// 	case RDMA_CM_EVENT_UNREACHABLE:
// 	case RDMA_CM_EVENT_REJECTED:
// 		fprintf(stderr, "cma event %s, error %d\n",
// 			rdma_event_str(event->event), event->status);
// 		sem_post(&cb->sem);
// 		ret = -1;
// 		break;

// 	case RDMA_CM_EVENT_DISCONNECTED:
// 		fprintf(stderr, "%s DISCONNECT EVENT...\n",
// 			cb->server ? "server" : "client");
// 		cb->req_state = DISCONNECTED;
// 		sem_post(&cb->sem);
// 		break;

// 	case RDMA_CM_EVENT_DEVICE_REMOVAL:
// 		fprintf(stderr, "cma detected device removal!!!!\n");
// 		cb->req_state = ERROR;
// 		sem_post(&cb->sem);
// 		ret = -1;
// 		break;

// 	default:
// 		fprintf(stderr, "unhandled event: %s, ignoring\n",
// 			rdma_event_str(event->event));
// 		break;
// 	}

// 	return ret;
// }

static int server_recv(struct rmserver_cb *cb, struct ibv_wc *wc)
{
	if (wc->byte_len != sizeof(cb->recv_buf)) {
		fprintf(stderr, "Received bogus data, size %d\n", wc->byte_len);
		return -1;
	}

	cb->remote_addr = be64toh(cb->recv_buf.buf);
	cb->remote_rkey = be32toh(cb->recv_buf.rkey);
	cb->remote_len  = be32toh(cb->recv_buf.size);
	cb->remote_offset = be64toh(cb->recv_buf.remote_offset);
	cb->request_type = (enum request_type)be32toh(cb->recv_buf.request_type);

	cb->req_state = RDMA_RECEIVED;

	printf("server received request of remote offset = %llu\n", cb->remote_offset);

	return 0;
}

static int rmserver_cq_event_handler(struct rmserver_cb *cb)
{
	struct ibv_wc wc;
	struct ibv_recv_wr *bad_wr;
	int ret;
	int flushed = 0;

	while ((ret = ibv_poll_cq(cb->cq, 1, &wc)) == 1) {
		ret = 0;

		if (wc.status) {
			if (wc.status == IBV_WC_WR_FLUSH_ERR) {
				flushed = 1;
				continue;

			}
			fprintf(stderr,
				"cq completion failed status %d\n",
				wc.status);
			ret = -1;
			goto error;
		}

		switch (wc.opcode) {
		case IBV_WC_SEND:
			printf("cb = %p, send has been completed\n", cb);
			cb->req_state = RDMA_RESPONSE_SENT;
			sem_post(&cb->sem);
			break;

		case IBV_WC_RDMA_WRITE:
			cb->req_state = RDMA_WRITE_COMPLETE;
			sem_post(&cb->sem);
			break;

		case IBV_WC_RDMA_READ:
			cb->req_state = RDMA_READ_COMPLETE;
			sem_post(&cb->sem);
			break;

		case IBV_WC_RECV:
			printf("server received a message\n");
			ret = server_recv(cb, &wc);
			if (ret) {
				fprintf(stderr, "recv wc error: %d\n", ret);
				goto error;
			}

			ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
			if (ret) {
				fprintf(stderr, "post recv error: %d\n", ret);
				goto error;
			}
			sem_post(&cb->sem);
			break;

		default:
			ret = -1;
			goto error;
		}
	}
	if (ret) {
		fprintf(stderr, "poll error %d\n", ret);
		goto error;
	}
	return flushed;

error:
	cb->req_state = ERROR;
	sem_post(&cb->sem);
	return ret;
}

// static int rmserver_accept(struct rmserver_cb *cb)
// {
// 	int ret;

// 	ret = rdma_accept(cb->child_cm_id, NULL);
// 	if (ret) {
// 		perror("rdma_accept");
// 		return ret;
// 	}

// 	sem_wait(&cb->sem);
// 	if (cb->req_state == ERROR) {
// 		fprintf(stderr, "wait for CONNECTED state %d\n", cb->state);
// 		return -1;
// 	}
// 	return 0;
// }

// static int rmserver_disconnect(struct rmserver_cb *cb, struct rdma_cm_id *id)
// {
// 	struct ibv_qp_attr qp_attr = {};
// 	int err = 0;

// 	if (cb->self_create_qp) {
// 		qp_attr.qp_state = IBV_QPS_ERR;
// 		err = ibv_modify_qp(cb->qp, &qp_attr, IBV_QP_STATE);
// 		if (err)
// 			return err;
// 	}

// 	return rdma_disconnect(id);
// }

static void rmserver_setup_wr(struct rmserver_cb *cb)
{
	cb->recv_sgl.addr = (uint64_t) &cb->recv_buf;
	cb->recv_sgl.length = sizeof cb->recv_buf;
	cb->recv_sgl.lkey = cb->mr_buffer->lkey;
	cb->rq_wr.sg_list = &cb->recv_sgl;
	cb->rq_wr.num_sge = 1;
}

// static int rmserver_setup_buffers(struct rmserver_cb *cb)
// {
// 	int ret;

// 	cb->recv_mr = ibv_reg_mr(cb->pd, &cb->recv_buf, sizeof cb->recv_buf,
// 				 IBV_ACCESS_LOCAL_WRITE);
// 	if (!cb->recv_mr) {
// 		fprintf(stderr, "recv_buf reg_mr failed\n");
// 		return errno;
// 	}

// 	cb->send_mr = ibv_reg_mr(cb->pd, &cb->send_buf, sizeof cb->send_buf, 0);
// 	if (!cb->send_mr) {
// 		fprintf(stderr, "send_buf reg_mr failed\n");
// 		ret = errno;
// 		goto err1;
// 	}

// 	cb->rdma_buf = (char *)malloc(cb->size);
// 	if (!cb->rdma_buf) {
// 		fprintf(stderr, "rdma_buf malloc failed\n");
// 		ret = -ENOMEM;
// 		goto err2;
// 	}

// 	cb->rdma_mr = ibv_reg_mr(cb->pd, cb->rdma_buf, cb->size,
// 				 IBV_ACCESS_LOCAL_WRITE |
// 				 IBV_ACCESS_REMOTE_READ |
// 				 IBV_ACCESS_REMOTE_WRITE);
// 	if (!cb->rdma_mr) {
// 		fprintf(stderr, "rdma_buf reg_mr failed\n");
// 		ret = errno;
// 		goto err3;
// 	}

// 	rmserver_setup_wr(cb);
// 	return 0;

// err3:
// 	free(cb->start_buf);
// 	ibv_dereg_mr(cb->rdma_mr);
// 	free(cb->rdma_buf);
// err2:
// 	ibv_dereg_mr(cb->send_mr);
// err1:
// 	ibv_dereg_mr(cb->recv_mr);
// 	return ret;
// }

// static void rmserver_free_buffers(struct rmserver_cb *cb)
// {
// 	ibv_dereg_mr(cb->recv_mr);
// 	ibv_dereg_mr(cb->send_mr);
// 	ibv_dereg_mr(cb->rdma_mr);
// 	free(cb->rdma_buf);
// }

// static int rmserver_create_qp(struct rmserver_cb *cb)
// {
// 	struct ibv_qp_init_attr init_attr;
// 	struct rdma_cm_id *id;
// 	int ret;

// 	memset(&init_attr, 0, sizeof(init_attr));
// 	init_attr.cap.max_send_wr = RMSERVER_SQ_DEPTH;
// 	init_attr.cap.max_recv_wr = 2;
// 	init_attr.cap.max_recv_sge = 1;
// 	init_attr.cap.max_send_sge = 1;
// 	init_attr.qp_type = IBV_QPT_RC;
// 	init_attr.send_cq = cb->cq;
// 	init_attr.recv_cq = cb->cq;
// 	id = cb->child_cm_id;

// 	ret = rdma_create_qp(id, cb->pd, &init_attr);
// 	if (!ret)
// 		cb->qp = id->qp;
// 	else
// 		perror("rdma_create_qp");
// 	return ret;
// }

// static void rmserver_free_qp(struct rmserver_cb *cb)
// {
// 	ibv_destroy_qp(cb->qp);
// 	ibv_destroy_cq(cb->cq);
// 	ibv_destroy_comp_channel(cb->channel);
// 	ibv_dealloc_pd(cb->pd);
// }

static int rmserver_setup_qp(struct rmserver_cb *cb)
{
	struct ibv_qp_init_attr qp_attr = {};
	int ret = 0;

	cb->channel = ibv_create_comp_channel(cb->cm_id->verbs);
	if (!cb->channel) {
		fprintf(stderr, "ibv_create_comp_channel failed\n");
		ret = errno;
		return ret;
	}
	printf("created channel %p\n", cb->channel);

	cb->cq = ibv_create_cq(cb->cm_id->verbs, 10 * 2, cb,
					cb->channel, 0);
	if (!cb->cq) {
		fprintf(stderr, "ibv_create_cq failed\n");
		ret = errno;
		goto err2;
	}
	printf("created cq %p\n", cb->cq);

	ret = ibv_req_notify_cq(cb->cq, 0);
	if (ret) {
		fprintf(stderr, "ibv_create_cq failed\n");
		ret = errno;
		goto err3;
	}

	qp_attr.send_cq = cb->cq;
	qp_attr.recv_cq = cb->cq;
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.cap.max_send_wr = 10;
	qp_attr.cap.max_recv_wr = 10;
	qp_attr.cap.max_send_sge = 1;
	qp_attr.cap.max_recv_sge = 1;

	TEST_NZ(rdma_create_qp(cb->cm_id, cb->ctrl->dev->pd, &qp_attr));
	cb->qp = cb->cm_id->qp;

	return 0;

err3:
	ibv_destroy_cq(cb->cq);
err2:
	ibv_destroy_comp_channel(cb->channel);
	return ret;
}

// static void *cm_thread(void *arg)
// {
// 	struct rmserver_cb *cb = (struct rmserver_cb *)arg;
// 	struct rdma_cm_event *event;
// 	int ret;

// 	while (1) {
// 		ret = rdma_get_cm_event(cb->cm_channel, &event);
// 		if (ret) {
// 			perror("rdma_get_cm_event");
// 			exit(ret);
// 		}
// 		ret = rmserver_cma_event_handler(event->id, event);
// 		rdma_ack_cm_event(event);
// 		if (ret)
// 			exit(ret);
// 	}
// }

// static void *cq_thread(void *arg)
// {
// 	struct rmserver_cb *cb = (struct rmserver_cb *)arg;
// 	struct ibv_cq *ev_cq;
// 	void *ev_ctx;
// 	int ret;
	
// 	while (1) {	
// 		pthread_testcancel();

// 		printf("waiting for event in %p\n", cb);
// 		ret = ibv_get_cq_event(cb->channel, &ev_cq, &ev_ctx);
// 		if (ret) {
// 			fprintf(stderr, "Failed to get cq event!\n");
// 			pthread_exit(NULL);
// 		}
// 		printf("Got cq event in %p\n", cb);

// 		if (ev_cq != cb->cq) {
// 			fprintf(stderr, "Unknown CQ!\n");
// 			pthread_exit(NULL);
// 		}
// 		ret = ibv_req_notify_cq(cb->cq, 0);
// 		if (ret) {
// 			fprintf(stderr, "Failed to set notify!\n");
// 			pthread_exit(NULL);
// 		}

// 		printf("calling rmserver_cq_event_handler\n");
// 		ret = rmserver_cq_event_handler(cb);
// 		ibv_ack_cq_events(cb->cq, 1);
// 		if (ret)
// 			pthread_exit(NULL);
// 	}
// }

static void rmserver_format_send(struct rmserver_cb *cb, char *buf, struct ibv_mr *mr, uint64_t roffset, enum request_type req)
{
	struct rmserver_rdma_info *info = (struct rmserver_rdma_info *)&cb->send_buf;

	info->buf = htobe64((uint64_t) (unsigned long) buf);
	info->rkey = htobe32(0);
	info->size = htobe32(cb->size);	
	info->remote_offset = htobe64(cb->remote_offset);
	info->request_type = htobe32(cb->request_type);
}

void* rmserver_test_server(void *arg)
{
	struct ibv_ah *ah[256];
	memset(ah, 0, 256 * sizeof(uintptr_t));

	struct ibv_send_wr wr, *bad_wr;
	struct ibv_sge sge;
	int ret;
	struct rmserver_cb *cb = (struct rmserver_cb *)arg;
	uint64_t rdma_size = 0;
	struct ibv_wc wc;

	while (1) {
		/* Wait for client's Start STAG/TO/Len */
		printf("%s: waiting for request at cb = %p\n", __FUNCTION__, cb);
		// sem_wait(&cb->sem);
		while (cb->req_state != RDMA_RECEIVED) {
			rmserver_cq_event_handler(cb);
			// fprintf(stderr, "wait for RDMA_READ_ADV state %d\n",
			// 		cb->state);
			// ret = -1;
			// break;
		}

		printf("RDMA has been received\n");

		rmserver_format_send(cb, cb->start_buf, cb->start_mr, cb->remote_offset, cb->request_type);
		if (cb->request_type == PAGE_FAULT) {
			printf("Received page fault\n");
			memcpy((void*)((uint64_t)&(cb->send_buf.data)), (void*)((uint64_t)(far_memory) + cb->remote_offset), PAGE_SIZE);
			rdma_size = sizeof(struct rmserver_rdma_info) + PAGE_SIZE;
		} else if (cb->request_type == PAGE_EVICT) {
			printf("Received page eviction\n");
			memcpy((void*)(((uint64_t)far_memory) + cb->remote_offset), (void*)((uint64_t)&(cb->recv_buf.data)), PAGE_SIZE);
			rdma_size = sizeof(struct rmserver_rdma_info);
		}

		wr.opcode = IBV_WR_SEND;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		wr.send_flags = IBV_SEND_SIGNALED;

		sge.addr = (uint64_t) &cb->send_buf;
		sge.length = rdma_size;
		sge.lkey = cb->mr_send_buffer->lkey;

		cb->req_state = RDMA_RESPONSE_STARTED;

		/* Tell client to continue */
		ret = ibv_post_send(cb->qp, &wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}
		printf("sent reponse successfully\n");

		while ((ret = ibv_poll_cq(cb->cq, 1, &wc) == 0));
		if (ret < 0) {
			printf("poll error %d\n", ret);
			return NULL;
		}
		if (wc.status) {
			printf("send completing error %d\n", wc.status);
			return NULL;
		}

		printf("SENDING HAS BEEN COMPLETED\n");

		// sem_wait(&cb->sem);
		// if (cb->req_state != RDMA_RESPONSE_SENT) {
		// 	fprintf(stderr, "wait for RDMA_RESPONSE_SENT state %d\n",
		// 		cb->state);
		// 	ret = -1;
		// 	break;
		// }
		// printf("server received send complete\n");
	}

	return (cb->req_state == DISCONNECTED) ? 0 : 0;
}

// static int rmserver_bind_server(struct rmserver_cb *cb)
// {
// 	int ret;

// 	if (cb->sin.ss_family == AF_INET)
// 		((struct sockaddr_in *) &cb->sin)->sin_port = cb->port;
// 	else
// 		((struct sockaddr_in6 *) &cb->sin)->sin6_port = cb->port;

// 	ret = rdma_bind_addr(cb->cm_id, (struct sockaddr *) &cb->sin);
// 	if (ret) {
// 		perror("rdma_bind_addr");
// 		return ret;
// 	}

// 	ret = rdma_listen(cb->cm_id, 3);
// 	if (ret) {
// 		perror("rdma_listen");
// 		return ret;
// 	}

// 	return 0;
// }

// static int rmserver_run_server(struct rmserver_cb *cb)
// {
// 	int ret;

  	// far_memory = (void*)malloc(BUFFER_SIZE);

	// ret = rmserver_bind_server(cb);
	// if (ret)
	// 	return ret;

	// sem_wait(&cb->sem);
	// if (cb->state != CONNECT_REQUEST) {
	// 	fprintf(stderr, "wait for CONNECT_REQUEST state %d\n",
	// 		cb->state);
	// 	return -1;
	// }

	// ret = rmserver_setup_qp(cb, cb->child_cm_id);
	// if (ret) {
	// 	fprintf(stderr, "setup_qp failed: %d\n", ret);
	// 	return ret;
	// }

	// ret = rmserver_setup_buffers(cb);
	// if (ret) {
	// 	fprintf(stderr, "rping_setup_buffers failed: %d\n", ret);
	// 	goto err1;
	// }

	// ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	// if (ret) {
	// 	fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
	// 	goto err2;
	// }

	// ret = pthread_create(&cb->cqthread, NULL, cq_thread, cb);
	// if (ret) {
	// 	perror("pthread_create");
	// 	goto err2;
	// }

	// ret = rmserver_accept(cb);
	// if (ret) {
	// 	fprintf(stderr, "connect error %d\n", ret);
	// 	goto err2;
	// }

// 	ret = rmserver_test_server(cb);
// 	if (ret) {
// 		fprintf(stderr, "rping server failed: %d\n", ret);
// 		goto err3;
// 	}

// 	ret = 0;
// err3:
// 	rmserver_disconnect(cb, cb->child_cm_id);
// 	pthread_join(cb->cqthread, NULL);
// 	rdma_destroy_id(cb->child_cm_id);
// 	//rmserver_free_buffers(cb);
// 	rmserver_free_qp(cb);

// 	return ret;
// }

// static int get_addr(char *dst, struct sockaddr *addr)
// {
// 	struct addrinfo *res;
// 	int ret;

// 	ret = getaddrinfo(dst, NULL, NULL, &res);
// 	if (ret) {
// 		printf("getaddrinfo failed (%s) - invalid hostname or IP address\n", gai_strerror(ret));
// 		return ret;
// 	}

// 	if (res->ai_family == PF_INET)
// 		memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
// 	else if (res->ai_family == PF_INET6)
// 		memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in6));
// 	else
// 		ret = -1;
	
// 	freeaddrinfo(res);
// 	return ret;
// }

int main(int argc, char **argv)
{
	if (argc != 2) {
		die("Need to specify a port number to listen");
	}

	struct sockaddr_in addr = {};
	struct rdma_cm_event *event = NULL;
	struct rdma_event_channel *ec = NULL;
	struct rdma_cm_id *listener = NULL;
	uint16_t port = 0;
	int ret = 0;
	struct ibv_recv_wr *bad_wr;

	// memset(cb, 0, sizeof(*cb));
	// cb->server = 1;
	// cb->state = IDLE;
	// cb->size = 64;
	// cb->sin.ss_family = PF_INET;
	// cb->port = htobe16(7174);
	// sem_init(&cb->sem, 0, 0);

	// opterr = 0;

	// get_addr(argv[1], (struct sockaddr *) &cb->sin);

	far_memory = (void*)malloc(BUFFER_SIZE);

	TEST_NZ(alloc_control());
	
	// cb->cm_channel = create_first_event_channel();
	// if (!cb->cm_channel) {
	// 	ret = errno;
	// 	goto out;
	// }

	// ret = rdma_create_id(cb->cm_channel, &cb->cm_id, cb, RDMA_PS_TCP);
	// if (ret) {
	// 	perror("rdma_create_id");
	// 	goto out2;
	// }

	// ret = pthread_create(&cb->cmthread, NULL, cm_thread, cb);
	// if (ret) {
	// 	perror("pthread_create");
	// 	goto out2;
	// }

	addr.sin_family = AF_INET;
	addr.sin_port = htons(atoi(argv[1]));

	TEST_Z(ec = rdma_create_event_channel());
	TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
	TEST_NZ(rdma_listen(listener, NUM_QUEUES + 1));
	port = ntohs(rdma_get_src_port(listener));
	printf("listening on port %d.\n", port);

	for (unsigned i = 0; i < NUM_QUEUES; i++) {
		printf("waiting for queue connection: %d\n", i);
		struct rmserver_cb *cb = &gctrl->cbs[i];

		// handle connection requests
		while (rdma_get_cm_event(ec, &event) == 0) {
			struct rdma_cm_event event_copy;

			memcpy(&event_copy, event, sizeof(*event));
			rdma_ack_cm_event(event);

			printf("received event to connect. Calling on_event()\n");
			if (on_event(&event_copy) || cb->state == rmserver_cb::CONNECTED)
				break;
		}
	}

	printf("done connecting all queues\n");

	for (unsigned int i = 0; i < NUM_QUEUES; ++i) {
		printf("Starting test server: %d\n", i);
		struct rmserver_cb *cb = &gctrl->cbs[i];

		ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
		}
		
		// ret = pthread_create(&cb->cqthread, NULL, cq_thread, cb);
		// if (ret) {
		// 	perror("pthread_create");
		// }

		ret = pthread_create(&cb->test_thread, NULL, rmserver_test_server, cb);
		if (ret) {
			perror("pthread create");
		}
	}


	// handle disconnects, etc.
	while (rdma_get_cm_event(ec, &event) == 0) {
		struct rdma_cm_event event_copy;

		memcpy(&event_copy, event, sizeof(*event));
		rdma_ack_cm_event(event);

		if (on_event(&event_copy))
			break;
	}

	rdma_destroy_event_channel(ec);
	rdma_destroy_id(listener);
	destroy_device(gctrl);
	return 0;






	addr.sin_family = AF_INET;
	addr.sin_port = htons(atoi(argv[1]));

	TEST_NZ(alloc_control());

	TEST_Z(ec = rdma_create_event_channel());
	TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
	TEST_NZ(rdma_listen(listener, NUM_QUEUES + 1));
	port = ntohs(rdma_get_src_port(listener));
	printf("listening on port %d.\n", port);

	for (unsigned int i = 0; i < NUM_QUEUES; ++i) {
		printf("waiting for queue connection: %d\n", i);
		struct queue *q = &gctrl->queues[i];

		// handle connection requests
		while (rdma_get_cm_event(ec, &event) == 0) {
			struct rdma_cm_event event_copy;

			memcpy(&event_copy, event, sizeof(*event));
			rdma_ack_cm_event(event);

			if (on_event(&event_copy) || q->state == queue::CONNECTED)
				break;
		}
	}

	printf("done connecting all queues\n");

	// handle disconnects, etc.
	while (rdma_get_cm_event(ec, &event) == 0) {
		struct rdma_cm_event event_copy;

		memcpy(&event_copy, event, sizeof(*event));
		rdma_ack_cm_event(event);

		if (on_event(&event_copy))
			break;
	}

	rdma_destroy_event_channel(ec);
	rdma_destroy_id(listener);
	destroy_device(gctrl);
	return 0;
}

void die(const char *reason)
{
	fprintf(stderr, "%s - errno: %d\n", reason, errno);
	exit(EXIT_FAILURE);
}

int alloc_control()
{
	gctrl = (struct ctrl *) malloc(sizeof(struct ctrl));
	TEST_Z(gctrl);
	memset(gctrl, 0, sizeof(struct ctrl));

	gctrl->cbs = (struct rmserver_cb *) malloc(sizeof(struct rmserver_cb) * NUM_QUEUES);
	gctrl->queues = (struct queue *) malloc(sizeof(struct queue) * NUM_QUEUES);
	TEST_Z(gctrl->cbs);
	TEST_Z(gctrl->queues);
	memset(gctrl->cbs, 0, sizeof(struct rmserver_cb) * NUM_QUEUES);
	memset(gctrl->queues, 0, sizeof(struct queue) * NUM_QUEUES);
	for (unsigned int i = 0; i < NUM_QUEUES; ++i) {
		gctrl->queues[i].ctrl = gctrl;
		gctrl->queues[i].state = queue::INIT;
		gctrl->cbs[i].ctrl = gctrl;
		gctrl->cbs[i].state = rmserver_cb::INIT;
	}
	return 0;
}

static device *get_device(struct rmserver_cb *cb)
{
	struct device *dev = NULL;

	if (!cb->ctrl->dev) {
		dev = (struct device *) malloc(sizeof(*dev));
		TEST_Z(dev);
		dev->verbs = cb->cm_id->verbs;
		TEST_Z(dev->verbs);
		dev->pd = ibv_alloc_pd(dev->verbs);
		TEST_Z(dev->pd);
		cb->ctrl->dev = dev;
	}

	TEST_Z(cb->mr_buffer = ibv_reg_mr(
		cb->ctrl->dev->pd,
		&cb->recv_buf,
		sizeof cb->recv_buf,
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ));

	printf("registered memory region of %zu bytes for recv\n", sizeof cb->recv_buf);

	TEST_Z(cb->mr_send_buffer = ibv_reg_mr(cb->ctrl->dev->pd, &cb->send_buf,
										sizeof cb->send_buf, 0));

	printf("registered memory region of %zu bytes for send\n", sizeof cb->send_buf);
	return cb->ctrl->dev;
}

static void destroy_device(struct ctrl *ctrl)
{
	TEST_Z(ctrl->dev);

	ibv_dereg_mr(ctrl->mr_buffer);
	free(ctrl->buffer);
	ibv_dealloc_pd(ctrl->dev->pd);
	free(ctrl->dev);
	ctrl->dev = NULL;
}

// static void create_qp(struct queue *q)
// {
//   struct ibv_qp_init_attr qp_attr = {};

//   qp_attr.send_cq = q->cq;
//   qp_attr.recv_cq = q->cq;
//   qp_attr.qp_type = IBV_QPT_RC;
//   qp_attr.cap.max_send_wr = 10;
//   qp_attr.cap.max_recv_wr = 10;
//   qp_attr.cap.max_send_sge = 1;
//   qp_attr.cap.max_recv_sge = 1;

//   TEST_NZ(rdma_create_qp(q->cm_id, q->ctrl->dev->pd, &qp_attr));
//   q->qp = q->cm_id->qp;
// }

int on_connect_request(struct rdma_cm_id *id, struct rdma_conn_param *param)
{
	struct rdma_conn_param cm_params = {};
	struct ibv_device_attr attrs = {};
	struct queue *q = &gctrl->queues[queue_ctr];
	struct rmserver_cb *cb = &gctrl->cbs[queue_ctr++];

	TEST_Z(cb->state == rmserver_cb::INIT);
	printf("%s\n", __FUNCTION__);

	//id->context = q;
	id->context = cb;
	q->cm_id = id;
	cb->cm_id = id;

	struct device *dev = get_device(cb);
	printf("%s: get_device() returned %p\n", __FUNCTION__, dev);

	// if (!(gctrl->cbs[0].rdma_buf)) {
	// 	rmserver_setup_buffers(&gctrl->cbs[0]);
	// }
	int ret = rmserver_setup_qp(cb);
	TEST_NZ(ret);
	printf("rmserver_setup_qp() returned %d\n", ret);

	TEST_NZ(ibv_query_device(dev->verbs, &attrs));

	printf("attrs: max_qp=%d, max_qp_wr=%d, max_cq=%d max_cqe=%d \
			max_qp_rd_atom=%d, max_qp_init_rd_atom=%d\n", attrs.max_qp,
			attrs.max_qp_wr, attrs.max_cq, attrs.max_cqe,
          	attrs.max_qp_rd_atom, attrs.max_qp_init_rd_atom);

	printf("ctrl attrs: initiator_depth=%d responder_resources=%d\n",
      		param->initiator_depth, param->responder_resources);

	cm_params.initiator_depth = param->initiator_depth;

	cm_params.responder_resources = param->responder_resources;
  	cm_params.rnr_retry_count = param->rnr_retry_count;
  	cm_params.flow_control = param->flow_control;

  	TEST_NZ(rdma_accept(cb->cm_id, &cm_params));

  	return 0;

	// ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	// if (ret) {
	// 	fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
	// }

	// ret = pthread_create(&cb->cqthread, NULL, cq_thread, cb);
	// if (ret) {
	// 	perror("pthread_create");
	// }

	// ret = rmserver_accept(cb);
	// if (ret) {
	// 	fprintf(stderr, "connect error %d\n", ret);
	// }

	// ret = pthread_create(&cb->test_thread, NULL, rmserver_test_server, cb);
	// if (ret) {
	// 	perror("pthread create");
	// }

	// return 0;
}

int on_connection(struct rmserver_cb *cb)
{
	printf("%s\n", __FUNCTION__);
	//struct ctrl *ctrl = q->ctrl;

	TEST_Z(cb->state == rmserver_cb::INIT);

//   if (q == &ctrl->queues[0]) {
//     struct ibv_send_wr wr = {};
//     struct ibv_send_wr *bad_wr = NULL;
//     struct ibv_sge sge = {};
//     struct memregion servermr = {};

//     printf("connected. sending memory region info.\n");
//     printf("MR key=%u base vaddr=%p\n", ctrl->mr_buffer->rkey, ctrl->mr_buffer->addr);

//     servermr.baseaddr = (uint64_t) ctrl->mr_buffer->addr;
//     servermr.key  = ctrl->mr_buffer->rkey;

//     wr.opcode = IBV_WR_SEND;
//     wr.sg_list = &sge;
//     wr.num_sge = 1;
//     wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;

//     sge.addr = (uint64_t) &servermr;
//     sge.length = sizeof(servermr);

//     TEST_NZ(ibv_post_send(q->qp, &wr, &bad_wr));

//     // TODO: poll here
//   }

	// q->state = queue::CONNECTED;

	rmserver_setup_wr(cb);
	printf("Connection successful\n");
	cb->state = rmserver_cb::CONNECTED;

	return 0;
}

int on_disconnect(struct rmserver_cb *cb)
{
	printf("%s\n", __FUNCTION__);

	if (cb->state == rmserver_cb::CONNECTED) {
		cb->state = rmserver_cb::INIT;
		rdma_destroy_qp(cb->cm_id);
		rdma_destroy_id(cb->cm_id);
	}

	return 0;
}

int on_event(struct rdma_cm_event *event)
{
	printf("%s\n", __FUNCTION__);
	//struct queue *q = (struct queue *) event->id->context;
	struct rmserver_cb *cb = (struct rmserver_cb *) event->id->context;

	switch (event->event) {
		case RDMA_CM_EVENT_CONNECT_REQUEST:
			return on_connect_request(event->id, &event->param.conn);
		case RDMA_CM_EVENT_ESTABLISHED:
			return on_connection(cb);
		case RDMA_CM_EVENT_DISCONNECTED:
			on_disconnect(cb);
		return 1;
		default:
			printf("unknown event: %s\n", rdma_event_str(event->event));
		return 1;
	}
}

