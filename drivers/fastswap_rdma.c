#define pr_fmt(fmt) KBUILD_MODNAME ": " fmt

#include "fastswap_rdma.h"
#include <linux/slab.h>
#include <linux/cpumask.h>
#include <linux/kthread.h>
#include <linux/sched.h>
#include <linux/delay.h>
#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <linux/pci.h>
#include <linux/device.h>
#include <linux/string.h>
#include <linux/inet.h>

static struct sswap_rdma_ctrl *gctrl;
static int serverport;
static int numqueues;
static int numcpus;
static int numprocs;
static char serverip[INET_ADDRSTRLEN];
static char clientip[INET_ADDRSTRLEN];
static struct kmem_cache *req_cache;
module_param_named(sport, serverport, int, 0644);
//module_param_named(nq, numqueues, int, 0644);
module_param_named(np, numprocs, int, 0644);
module_param_string(sip, serverip, INET_ADDRSTRLEN, 0644);
module_param_string(cip, clientip, INET_ADDRSTRLEN, 0644);

// TODO: destroy ctrl

#define CONNECTION_TIMEOUT_MS 60000
#define QP_QUEUE_DEPTH 256
/* we don't really use recv wrs, so any small number should do */
#define QP_MAX_RECV_WR 4
/* we mainly do send wrs */
#define QP_MAX_SEND_WR	(4096)
#define CQ_NUM_CQES	(QP_MAX_SEND_WR)
#define POLL_BATCH_HIGH (QP_MAX_SEND_WR / 4)

#define htonll(x) cpu_to_be64((x))
#define ntohll(x) cpu_to_be64((x))


static void sswap_rdma_addone(struct ib_device *dev)
{
  pr_info("sswap_rdma_addone() = %s\n", dev->name);
}

static void sswap_rdma_removeone(struct ib_device *ib_device, void *client_data)
{
  pr_info("sswap_rdma_removeone()\n");
}

static struct ib_client sswap_rdma_ib_client = {
  .name   = "sswap_rdma",
  .add    = sswap_rdma_addone,
  .remove = sswap_rdma_removeone
};

static void sswap_rdma_qp_event(struct ib_event *e, void *c)
{
  pr_info("sswap_rdma_qp_event\n");
}

static int sswap_rdma_create_qp(struct rdma_queue *queue)
{
  struct sswap_rdma_dev *rdev = queue->ctrl->rdev;
  struct ib_qp_init_attr init_attr;
  int ret;

  pr_info("start: %s\n", __FUNCTION__);

  memset(&init_attr, 0, sizeof(init_attr));
  //init_attr.event_handler = sswap_rdma_qp_event;
  init_attr.cap.max_send_wr = QP_MAX_SEND_WR;
  init_attr.cap.max_recv_wr = QP_MAX_RECV_WR;
  init_attr.cap.max_recv_sge = 1;
  init_attr.cap.max_send_sge = 1;
  //init_attr.sq_sig_type = IB_SIGNAL_REQ_WR;
  init_attr.qp_type = IB_QPT_UD;
  init_attr.send_cq = queue->cq;
  init_attr.recv_cq = queue->cq;
  /* just to check if we are compiling against the right headers */
  init_attr.create_flags = IB_QP_EXP_CREATE_ATOMIC_BE_REPLY & 0;

  ret = rdma_create_qp(queue->cm_id, rdev->pd, &init_attr);
  if (ret) {
    pr_err("rdma_create_qp failed: %d\n", ret);
    return ret;
  }

  queue->qp = queue->cm_id->qp;
  return ret;
}

static void sswap_rdma_destroy_queue_ib(struct rdma_queue *q)
{
  struct sswap_rdma_dev *rdev;
  struct ib_device *ibdev;

  pr_info("start: %s\n", __FUNCTION__);

  rdev = q->ctrl->rdev;
  ibdev = rdev->dev;
  //rdma_destroy_qp(q->ctrl->cm_id);
  ib_free_cq(q->cq);
}

static int sswap_setup_buffers(struct sswap_cb *cb, struct sswap_rdma_dev *rdev)
{
	int ret;

	cb->recv_dma_addr = ib_dma_map_single(rdev->pd->device,
					      &cb->recv_buf,
					      sizeof(cb->recv_buf), DMA_BIDIRECTIONAL);
	dma_unmap_addr_set(cb, recv_mapping, cb->recv_dma_addr);

	cb->send_pf_dma_addr = ib_dma_map_single(rdev->pd->device,
					      &cb->send_pf_buf, sizeof(cb->send_pf_buf),
					      DMA_BIDIRECTIONAL);
	dma_unmap_addr_set(cb, send_pf_mapping, cb->send_pf_dma_addr);

	cb->send_evict_dma_addr = ib_dma_map_single(rdev->pd->device,
					      &cb->send_evict_buf, sizeof(cb->send_evict_buf),
					      DMA_BIDIRECTIONAL);
	dma_unmap_addr_set(cb, send_evict_mapping, cb->send_evict_dma_addr);

	cb->rdma_buf = ib_dma_alloc_coherent(rdev->pd->device, cb->size,
					     &cb->rdma_dma_addr,
					     GFP_KERNEL);
	dma_unmap_addr_set(cb, rdma_mapping, cb->rdma_dma_addr);

	cb->page_list_len = (((cb->size - 1) & PAGE_MASK) + PAGE_SIZE)
		>> PAGE_SHIFT;
	cb->reg_mr = ib_alloc_mr(rdev->pd,  IB_MR_TYPE_MEM_REG,
				 cb->page_list_len);
	if (IS_ERR(cb->reg_mr)) {
		ret = PTR_ERR(cb->reg_mr);
		goto bail;
	}

	cb->start_buf = ib_dma_alloc_coherent(rdev->pd->device, cb->size,
					      &cb->start_dma_addr,
					      GFP_KERNEL);
	dma_unmap_addr_set(cb, start_mapping, cb->start_dma_addr);

	return 0;
 bail:
	if (cb->reg_mr && !IS_ERR(cb->reg_mr))
		ib_dereg_mr(cb->reg_mr);
	if (cb->rdma_mr && !IS_ERR(cb->rdma_mr))
		ib_dereg_mr(cb->rdma_mr);
	if (cb->dma_mr && !IS_ERR(cb->dma_mr))
		ib_dereg_mr(cb->dma_mr);
	if (cb->rdma_buf) {
		ib_dma_free_coherent(rdev->pd->device, cb->size, cb->rdma_buf,
				     cb->rdma_dma_addr);
	}
	if (cb->start_buf) {
		ib_dma_free_coherent(rdev->pd->device, cb->size, cb->start_buf,
				     cb->start_dma_addr);
	}
	return ret;
}


static struct sswap_rdma_dev *sswap_rdma_get_device(struct sswap_cb *cb)
{
	struct sswap_rdma_dev *rdev = NULL;
	int ret = 0;

	if (!gctrl->rdev) {
		rdev = kzalloc(sizeof(*rdev), GFP_KERNEL);
		if (!rdev) {
			pr_err("no memory\n");
			goto out_err;
		}

		rdev->dev = cb->cm_id->device;

		pr_info("selecting device %s\n", rdev->dev->name);

		rdev->pd = ib_alloc_pd(rdev->dev, 0);
		if (IS_ERR(rdev->pd)) {
			pr_err("ib_alloc_pd\n");
			goto out_free_dev;
		}
		pr_info("rdev->pd after ib_alloc_pd = %p\n", rdev->pd);

		if (!(rdev->dev->attrs.device_cap_flags &
			IB_DEVICE_MEM_MGT_EXTENSIONS)) {
			pr_err("memory registrations not supported\n");
			goto out_free_pd;
		}
		pr_info("Memory registrations are supported\n");

		gctrl->rdev = rdev;
	}

	cb->pd = gctrl->rdev->pd;

	ret = sswap_setup_buffers(cb, gctrl->rdev);
	if (ret) {
		pr_info("sswap_setup_buffers() failed with ret value %d\n", ret);
	}
	pr_info("sswap_setup_buffers() returned %d\n", ret);

	return gctrl->rdev;

out_free_pd:
	ib_dealloc_pd(gctrl->rdev->pd);
out_free_dev:
	kfree(gctrl->rdev);
out_err:
	return NULL;
}

static int sswap_create_qp(struct sswap_cb *cb)
{
	struct sswap_rdma_dev *rdev = gctrl->rdev;
	struct ib_qp_init_attr init_attr;
	int ret;

  	pr_info("start: %s\n", __FUNCTION__);

	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.event_handler = sswap_rdma_qp_event;
	init_attr.cap.max_send_wr = QP_MAX_SEND_WR;
	init_attr.cap.max_recv_wr = QP_MAX_RECV_WR;
	init_attr.cap.max_recv_sge = 1;
	init_attr.cap.max_send_sge = 1;
	init_attr.sq_sig_type = IB_SIGNAL_REQ_WR;
	init_attr.qp_type = IB_QPT_RC;
	init_attr.send_cq = cb->cq;
	init_attr.recv_cq = cb->cq;
	init_attr.sq_sig_type = IB_SIGNAL_REQ_WR;

	ret = rdma_create_qp(cb->cm_id, rdev->pd, &init_attr);
	if (ret) {
		pr_err("rdma_create_qp failed: %d\n", ret);
		return ret;
	}

	cb->qp = cb->cm_id->qp;
	return ret;
}

static int sswap_setup_qp(struct sswap_cb *cb, struct rdma_cm_id *cm_id)
{
	int ret;
	struct ib_cq_init_attr attr = {0};
	struct ib_device *ibdev = gctrl->rdev->dev;
	int comp_vector = 0;

	// cb->pd = ib_alloc_pd(cm_id->device);
	// if (IS_ERR(cb->pd)) {
	// 	pr_info("ib_alloc_pd() failed\n");
	// 	ret = PTR_ERR(cb->pd);
	// 	goto err1;
	// }
	// pr_info("created pd %p. cm_id->device = %p\n", cb->pd, cm_id->device);

	// attr.cqe = cb->txdepth * 2;
	// attr.comp_vector = 0;
	cb->cq = ib_alloc_cq(ibdev, cb, CQ_NUM_CQES, comp_vector, IB_POLL_DIRECT);
	if (IS_ERR(cb->cq)) {
		pr_info("ib_create_cq() failed\n");
		ret = PTR_ERR(cb->cq);
		goto err1;
	}
	pr_info("created cq %p\n", cb->cq);

	ret = sswap_create_qp(cb);
	if (ret) {
		pr_info("sswap_create_qp() failed\n");
		goto err2;
	}
	pr_info("created qp %p\n", cb->qp);
	return 0;

err1:
	ib_destroy_cq(cb->cq);
err2:
	ib_dealloc_pd(cb->pd);
	return ret;
}


static int sswap_rdma_addr_resolved(struct sswap_cb *cb)
{
	int ret;
  	struct sswap_rdma_dev *rdev = NULL;

  	pr_info("start: %s\n", __FUNCTION__);

	rdev = sswap_rdma_get_device(cb);
	if (!rdev) {
		pr_err("no device found\n");
		return -ENODEV;
	}
	pr_info("sswap_rdma_get_device() returned rdev = %p\n", rdev);

	/*
	ret = sswap_rdma_create_queue_ib(q);
	if (ret) {
		return ret;
	}
	*/

	ret = sswap_setup_qp(cb, cb->cm_id);
	if (ret) {
		pr_info("setup_qp() failed\n");
	}
	pr_info("sswap_setup_qp() returned %d\n", ret);

  	ret = rdma_resolve_route(cb->cm_id, CONNECTION_TIMEOUT_MS);
	if (ret) {
		pr_err("rdma_resolve_route failed\n");
		//sswap_rdma_destroy_queue_ib(cb);
	}
	pr_info("rdma_resolve_route() returned %d\n", ret);

	return 0;
}

static int reg_supported(struct ib_device *dev)
{
	u64 needed_flags = IB_DEVICE_MEM_MGT_EXTENSIONS;

	if ((dev->attrs.device_cap_flags & needed_flags) != needed_flags) {
		pr_info(
			"Fastreg not supported - device_cap_flags 0x%llx\n",
			(unsigned long long)dev->attrs.device_cap_flags);
		return 0;
	}
	pr_info("Fastreg supported - device_cap_flags 0x%llx\n",
		(unsigned long long)dev->attrs.device_cap_flags);
	return 1;
}

static void fill_sockaddr(struct sockaddr_storage *sin, struct sswap_cb *cb)
{
	memset(sin, 0, sizeof(*sin));

	if (cb->addr_type == AF_INET) {
		struct sockaddr_in *sin4 = (struct sockaddr_in *)sin;
		sin4->sin_family = AF_INET;
		memcpy((void *)&sin4->sin_addr.s_addr, cb->addr, 4);
		sin4->sin_port = cb->port;
	} else if (cb->addr_type == AF_INET6) {
		struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)sin;
		sin6->sin6_family = AF_INET6;
		memcpy((void *)&sin6->sin6_addr, cb->addr, 16);
		sin6->sin6_port = cb->port;
	}
}

static int sswap_rdma_route_resolved(struct sswap_cb *cb,
				     struct rdma_conn_param *conn_params)
{
	struct rdma_conn_param param = {};
	int ret;

	// memset(&conn_param, 0, sizeof(param));
	param.qp_num = cb->qp->qp_num;
	param.flow_control = 1;
	param.responder_resources = 16;
	param.initiator_depth = 16;
	param.retry_count = 7;
	param.rnr_retry_count = 7;
	param.private_data = NULL;
	param.private_data_len = 0;

	pr_info("%s: max_qp_rd_atom=%d max_qp_init_rd_atom=%d\n",
		__FUNCTION__, gctrl->rdev->dev->attrs.max_qp_rd_atom,
		gctrl->rdev->dev->attrs.max_qp_init_rd_atom);

	ret = rdma_connect(cb->cm_id, &param);
	if (ret) {
		pr_info("rdma_connect error %d\n", ret);
		return ret;
	}
	pr_info("%s: rdma_connect() returned %d\n", __FUNCTION__, ret);	

	// wait_event_interruptible(cb->sem, cb->state >= CONNECTED);
	// if (cb->state == ERROR) {
	// 	pr_info("wait for CONNECTED state %d\n", cb->state);
	// 	return -1;
	// }

	// pr_info("rdma_connect successful\n");
	return 0;
}

static int sswap_rdma_conn_established(struct rdma_queue *q)
{
  pr_info("connection established\n");
  return 0;
}

static int sswap_rdma_cm_handler(struct rdma_cm_id *cm_id,
    struct rdma_cm_event *ev)
{
	struct sswap_cb *cb = cm_id->context;
	int ret = 0;

	pr_info("cm_handler msg: %s (%d) status %d id %p\n", rdma_event_msg(ev->event),
		ev->event, ev->status, cm_id);

	switch (ev->event) {
	case RDMA_CM_EVENT_ADDR_RESOLVED:
			cb->state = ADDR_RESOLVED;
			ret = sswap_rdma_addr_resolved(cb);
			// ret = rdma_resolve_route(cm_id, 2000);
			if (ret) {
				pr_info("sswap_rdma_addr_resolved() error %d\n", ret);
				// wake_up_interruptible(&cb->sem);
			}
			// wake_up_interruptible(&cb->sem);
			pr_info("%s: Address has been resolved\n", __FUNCTION__);
			break;
	case RDMA_CM_EVENT_ROUTE_RESOLVED:
			cb->state = ROUTE_RESOLVED;
			ret = sswap_rdma_route_resolved(cb, &ev->param.conn);
			// wake_up_interruptible(&cb->sem);
			break;
	case RDMA_CM_EVENT_CONNECT_REQUEST:
			cb->state = CONNECT_REQUEST;
			cb->child_cm_id = cm_id;
			// wake_up_interruptible(&cb->sem);
			break;
	case RDMA_CM_EVENT_ESTABLISHED:
			pr_info("ESTABLISHED\n");
			cb->state = CONNECTED;
			//queue->cm_error = sswap_rdma_conn_established(queue);
			/* complete cm_done regardless of success/failure */
			//complete(&queue->cm_done);
			wake_up_interruptible(&cb->sem);
			break;
	case RDMA_CM_EVENT_REJECTED:
			cb->state = ERROR;
			pr_err("connection rejected\n");
			wake_up_interruptible(&cb->sem);
			break;
	case RDMA_CM_EVENT_ADDR_ERROR:
	case RDMA_CM_EVENT_ROUTE_ERROR:
	case RDMA_CM_EVENT_CONNECT_ERROR:
	case RDMA_CM_EVENT_UNREACHABLE:
			pr_err("CM error event %d\n", ev->event);
			ret = -ECONNRESET;
			cb->state = ERROR;
			wake_up_interruptible(&cb->sem);
			break;
	case RDMA_CM_EVENT_DISCONNECTED:
	case RDMA_CM_EVENT_ADDR_CHANGE:
	case RDMA_CM_EVENT_TIMEWAIT_EXIT:
			pr_err("CM connection closed %d\n", ev->event);
			wake_up_interruptible(&cb->sem);
			break;
	case RDMA_CM_EVENT_DEVICE_REMOVAL:
			/* device removal is handled via the ib_client API */
			wake_up_interruptible(&cb->sem);
			break;
	default:
			pr_err("CM unexpected event: %d\n", ev->event);
			wake_up_interruptible(&cb->sem);
			break;
	}

	//if (cm_error) {
		//queue->cm_error = cm_error;
		//complete(&queue->cm_done);
	//}

	return 0;
}

inline static int sswap_rdma_wait_for_cm(struct rdma_queue *queue)
{
  wait_for_completion_interruptible_timeout(&queue->cm_done,
    msecs_to_jiffies(CONNECTION_TIMEOUT_MS) + 1);
  return queue->cm_error;
}

inline static int sswap_new_rdma_wait_for_cm(struct sswap_cb *cb)
{
  wait_for_completion_interruptible_timeout(&cb->cm_done,
    msecs_to_jiffies(CONNECTION_TIMEOUT_MS) + 1);
  return cb->cm_error;
}

static int sswap_bind_client(struct sswap_cb *cb)
{
	struct sockaddr_storage sin;
	int ret;

	cb->addr_str = serverip;
	in4_pton(serverip, -1, cb->addr, -1, NULL);
	cb->addr_type = AF_INET;
	cb->port = serverport;

	// fill_sockaddr(&sin, cb);

	// ret = rdma_resolve_addr(cb->cm_id, NULL, (struct sockaddr *)&sin, 2000);
	// if (ret) {
	// 	pr_info("rdma_resolve_addr() failed\n");
	// 	return ret;
	// }

	ret = rdma_resolve_addr(cb->cm_id, &gctrl->srcaddr, &gctrl->addr, CONNECTION_TIMEOUT_MS);
	if (ret) {
		pr_info("rdma_resolve_addr() failed\n");
		return ret;
	}

	wait_event_interruptible(cb->sem, cb->state >= CONNECTED);
	if (cb->state != CONNECTED) {
		pr_info( 
		       "addr/route resolution did not resolve: state %d\n",
		       cb->state);
		return -EINTR;
	}

	if (!reg_supported(cb->cm_id->device))
		return -EINVAL;

	pr_info("rdma_resolve_addr - rdma_resolve_route successful\n");

	return 0;
}

static int client_recv(struct sswap_cb *cb, struct ib_wc *wc)
{
	pr_info("Client received data.\n");
	if (wc->byte_len < sizeof(cb->recv_buf)) {
		pr_info("Received bogus data, size %d\n", wc->byte_len);
		return -1;
	}

	pr_info("client_recv() remote_offset = %llu\n", ntohll(cb->recv_buf.remote_offset));

	cb->state = RDMA_RECEIVED;

	return 0;
}

static void sswap_cq_event_handler(struct ib_cq *cq, void *ctx)
{
	struct sswap_cb *cb = ctx;
	struct ib_wc wc;
	struct ib_recv_wr *bad_wr;
	int ret;

	BUG_ON(cb->cq != cq);
	if (cb->state == ERROR) {
		pr_info("cq completion in ERROR state\n");
		return;
	}
	while ((ret = ib_poll_cq(cb->cq, 1, &wc)) == 1) {
		if (wc.status) {
			if (wc.status == IB_WC_WR_FLUSH_ERR) {
				continue;
			} else {
				pr_info("cq completion failed with "
					"wr_id %Lx status %d opcode %d vender_err %x\n",
					wc.wr_id, wc.status, wc.opcode, wc.vendor_err);
				goto error;
			}
		}
		ret = 0;

		switch (wc.opcode) {
		case IB_WC_SEND:
			//cb->stats.send_bytes += cb->send_sgl.length;
			//cb->stats.send_msgs++;
			break;

		case IB_WC_RDMA_WRITE:
			//cb->stats.write_bytes += cb->rdma_sq_wr.sg_list->length;
			//cb->stats.write_msgs++;
			cb->state = RDMA_WRITE_COMPLETE;
			wake_up_interruptible(&cb->sem);
			break;

		case IB_WC_RDMA_READ:
			//cb->stats.read_bytes += cb->rdma_sq_wr.sg_list->length;
			//cb->stats.read_msgs++;
			cb->state = RDMA_READ_COMPLETE;
			wake_up_interruptible(&cb->sem);
			break;

		case IB_WC_RECV:
			//cb->stats.recv_bytes += sizeof(cb->recv_buf);
			//cb->stats.recv_msgs++;
			ret = client_recv(cb, &wc);
			if (ret) {
				pr_info("recv wc error: %d\n", ret);
				goto error;
			}

			ret = ib_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
			if (ret) {
				pr_info("post recv error: %d\n",
				       ret);
				goto error;
			}
			wake_up_interruptible(&cb->sem);
			break;

		default:
			pr_info(
			       "%s:%d Unexpected opcode %d, Shutting down\n",
			       __func__, __LINE__, wc.opcode);
			goto error;
		}
	}
	if (ret) {
		pr_info("poll error %d\n", ret);
		goto error;
	}
	return;

 error:
	cb->state = ERROR;
	wake_up_interruptible(&cb->sem);
}

/*
 * return the (possibly rebound) rkey for the rdma buffer.
 * FASTREG mode: invalidate and rebind via fastreg wr.
 * MW mode: rebind the MW.
 * other modes: just return the mr rkey.
 */
static u32 sswap_rdma_rkey(struct sswap_cb *cb, u64 buf, int post_inv)
{
	u32 rkey = 0xffffffff;

	if (buf == (u64)cb->start_dma_addr)
		rkey = cb->start_mr->rkey;
	else
		rkey = cb->rdma_mr->rkey;

	return rkey;
}

static void sswap_page_fault_format_send(struct sswap_cb *cb, u64 buf, u64 roffset)
{
	struct sswap_pf_rdma_info *info = &cb->send_pf_buf;
	u32 rkey = 0;

	pr_info("%s: setting rdma rkey\n", __FUNCTION__);
	// rkey = sswap_rdma_rkey(cb, buf, !cb->server_invalidate);
	info->buf = htonll(buf);
	info->rkey = htonl(rkey);
	info->size = htonl(cb->size);
  	info->remote_offset = htonll(roffset);
  	info->request_type = PAGE_FAULT;
}

static void sswap_page_evict_format_send(struct sswap_cb *cb, u64 buf, u64 roffset, struct page *page)
{
	struct sswap_evict_rdma_info *info = &cb->send_evict_buf;
	u32 rkey = 0;

	pr_info("%s: setting remote offset %lu\n", __FUNCTION__, roffset);
	// rkey = sswap_rdma_rkey(cb, buf, !cb->server_invalidate);
	info->buf = htonll(buf);
	info->rkey = htonl(rkey);
	info->size = htonl(cb->size);
  	info->remote_offset = htonll(roffset);
  	info->request_type = htonl(PAGE_EVICT);
	pr_info("%s: copying the page content from the page address to the send_evict_buf(), of size %lu\n", __FUNCTION__, PAGE_SIZE);
  	memcpy((void*)(info->page_content), page_address(page), PAGE_SIZE);
	pr_info("%s: memcpy was successful\n", __FUNCTION__);
}

inline struct sswap_cb *sswap_rdma_get_cb(unsigned int cpuid,
					       enum qp_type type)
{
  BUG_ON(gctrl == NULL);

  switch (type) {
    case QP_READ_SYNC:
      return &gctrl->cbs[cpuid];
    case QP_READ_ASYNC:
      return &gctrl->cbs[cpuid + numcpus];
    case QP_WRITE_SYNC:
      return &gctrl->cbs[cpuid + numcpus * 2];
    default:
      BUG();
  };
}

int sswap_new_rdma_read_sync(struct page *page, u64 roffset)
{
	struct ib_send_wr *bad_wr;
	struct ib_wc wc;
	int ret;
	struct sswap_cb *cb;
	pr_info("%s: Got page fault RDMA read request\n", __FUNCTION__);

	cb = sswap_rdma_get_cb(smp_processor_id() % numprocs, QP_READ_SYNC);
	cb->state = RDMA_REQUESTED;

	sswap_page_fault_format_send(cb, cb->start_dma_addr, roffset);
	if (cb->state == ERROR) {
		pr_info("sswap_page_fault_format_send() failed\n");
		return cb->state;
	}
	pr_info("%s: page_evict_format_send() is set\n", __FUNCTION__);

	ret = ib_post_send(cb->qp, &cb->sq_pf_wr, &bad_wr);
	if (ret) {
		pr_info("post send error %d\n", ret);
		return ret;
	}
	pr_info("%s: ibv_post_send() is done\n", __FUNCTION__);

	// Spin wait for send completion
	while ((ret = ib_poll_cq(cb->cq, 1, &wc) == 0));
	if (ret < 0) {
		pr_info("poll error %d\n", ret);
		return ret;
	}

	if (wc.status) {
		pr_info("send completion error %d\n", wc.status);
		return wc.status;
	}
	pr_info("%s: Waiting to receive RDMA Response\n", __FUNCTION__);

	while (cb->state != RDMA_RECEIVED) {
		sswap_cq_event_handler(cb->cq, cb);
		if (cb->state == ERROR) {
			return -1;
		}
	}
	pr_info("%s: Received RDMA response, now writing to local page\n", __FUNCTION__);

	// Actually write to the page
	memcpy((void*)page_address(page), (void*)((u64)&(cb->recv_buf.page_content)), PAGE_SIZE);
	SetPageUptodate(page);
	unlock_page(page);

	return 0;
}
EXPORT_SYMBOL(sswap_new_rdma_read_sync);

static void sswap_setup_wr(struct sswap_cb *cb)
{
	cb->recv_sgl.addr = cb->recv_dma_addr;
	cb->recv_sgl.length = sizeof cb->recv_buf;
	cb->recv_sgl.lkey = cb->pd->local_dma_lkey;
	cb->rq_wr.sg_list = &cb->recv_sgl;
	cb->rq_wr.num_sge = 1;

	cb->send_sgl.addr = cb->send_dma_addr;
	cb->send_sgl.length = sizeof cb->send_buf;
  	cb->send_sgl.lkey = cb->pd->local_dma_lkey;

	cb->sq_wr.opcode = IB_WR_SEND;
	cb->sq_wr.send_flags = IB_SEND_SIGNALED;
	cb->sq_wr.sg_list = &cb->send_sgl;
	cb->sq_wr.num_sge = 1;

	cb->send_pf_sgl.addr = cb->send_pf_dma_addr;
	cb->send_pf_sgl.length = sizeof cb->send_pf_buf;
  	cb->send_pf_sgl.lkey = cb->pd->local_dma_lkey;

	cb->sq_pf_wr.opcode = IB_WR_SEND;
	cb->sq_pf_wr.send_flags = IB_SEND_SIGNALED;
	cb->sq_pf_wr.sg_list = &cb->send_pf_sgl;
	cb->sq_pf_wr.num_sge = 1;

	cb->send_evict_sgl.addr = cb->send_evict_dma_addr;
	cb->send_evict_sgl.length = sizeof cb->send_evict_buf;
  	cb->send_evict_sgl.lkey = cb->pd->local_dma_lkey;

	cb->sq_evict_wr.opcode = IB_WR_SEND;
	cb->sq_evict_wr.send_flags = IB_SEND_SIGNALED;
	cb->sq_evict_wr.sg_list = &cb->send_evict_sgl;
	cb->sq_evict_wr.num_sge = 1;

	cb->rdma_sgl.addr = cb->rdma_dma_addr;
	cb->rdma_sq_wr.wr.send_flags = IB_SEND_SIGNALED;
	cb->rdma_sq_wr.wr.sg_list = &cb->rdma_sgl;
	cb->rdma_sq_wr.wr.num_sge = 1;

  	/* 
	 * A chain of 2 WRs, INVALDATE_MR + REG_MR.
	 * both unsignaled.  The client uses them to reregister
	 * the rdma buffers with a new key each iteration.
	 */
	cb->reg_mr_wr.wr.opcode = IB_WR_REG_MR;
	cb->reg_mr_wr.mr = cb->reg_mr;

	cb->invalidate_wr.next = &cb->reg_mr_wr.wr;
	cb->invalidate_wr.opcode = IB_WR_LOCAL_INV;
}

static void sswap_free_qp(struct sswap_cb *cb)
{
	ib_destroy_qp(cb->qp);
	ib_destroy_cq(cb->cq);
	ib_dealloc_pd(cb->pd);
}

static void sswap_free_buffers(struct sswap_cb *cb)
{	
	if (cb->dma_mr)
		ib_dereg_mr(cb->dma_mr);
	if (cb->rdma_mr)
		ib_dereg_mr(cb->rdma_mr);
	if (cb->start_mr)
		ib_dereg_mr(cb->start_mr);
	if (cb->reg_mr)
		ib_dereg_mr(cb->reg_mr);

	dma_unmap_single(cb->pd->device->dma_device,
			 dma_unmap_addr(cb, recv_mapping),
			 sizeof(cb->recv_buf), DMA_BIDIRECTIONAL);
	dma_unmap_single(cb->pd->device->dma_device,
			 dma_unmap_addr(cb, send_mapping),
			 sizeof(cb->send_buf), DMA_BIDIRECTIONAL);
	dma_unmap_single(cb->pd->device->dma_device,
			 dma_unmap_addr(cb, send_pf_mapping),
			 sizeof(cb->send_pf_buf), DMA_BIDIRECTIONAL);
	dma_unmap_single(cb->pd->device->dma_device,
			 dma_unmap_addr(cb, send_evict_mapping),
			 sizeof(cb->send_evict_buf), DMA_BIDIRECTIONAL);

	ib_dma_free_coherent(cb->pd->device, cb->size, cb->rdma_buf,
			     cb->rdma_dma_addr);

	if (cb->start_buf) {
		ib_dma_free_coherent(cb->pd->device, cb->size, cb->start_buf,
				     cb->start_dma_addr);
	}
}

static int sswap_connect_client(struct sswap_cb *cb)
{
	struct rdma_conn_param conn_param;
	int ret;

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.retry_count = 10;

	ret = rdma_connect(cb->cm_id, &conn_param);
	if (ret) {
		pr_info("rdma_connect error %d\n", ret);
		return ret;
	}

	wait_event_interruptible(cb->sem, cb->state >= CONNECTED);
	if (cb->state == ERROR) {
		pr_info("wait for CONNECTED state %d\n", cb->state);
		return -1;
	}

	pr_info("rdma_connect successful\n");
	return 0;
}

static void sswap_run_client(struct sswap_cb *cb)
{
	struct ib_recv_wr *bad_wr;
	int ret;

	ret = sswap_bind_client(cb);
	if (ret) {
		pr_info("bind client failed\n");
	}

	// ret = sswap_connect_client(cb);
	// if (ret) {
	// 	pr_info("connect error %d\n", ret);
	// 	goto err2;
	// }
	// pr_info("sswap_connect_client() done\n");

	// ret = sswap_new_rdma_wait_for_cm(cb);
  	// if (ret) {
    // 	pr_err("sswap_rdma_wait_for_cm failed\n");    	
  	// }

	// rdma_disconnect(cb->cm_id);
	return;

 err2:
	sswap_free_buffers(cb);
 err1:
	sswap_free_qp(cb);
}

static int sswap_rdma_init_queue(struct sswap_rdma_ctrl *ctrl,
    int idx)
{
	struct rdma_queue *queue;
	int ret;
	struct sswap_cb *cb;
	struct ib_recv_wr *bad_wr;

	cb = &ctrl->cbs[idx];

	cb->server = 0;
	cb->state = IDLE;
	cb->size = 64;
	cb->txdepth = 64;
	cb->addr_str = serverip;
	init_waitqueue_head(&cb->sem);

	pr_info("start: %s\n", __FUNCTION__);

	//   queue = &ctrl->queues[idx];
	//   queue->ctrl = ctrl;
	//   init_completion(&queue->cm_done);
	//   atomic_set(&queue->pending, 0);
	//   spin_lock_init(&queue->cq_lock);
	//   queue->qp_type = get_queue_type(idx);

	cb->cm_id = rdma_create_id(&init_net, sswap_rdma_cm_handler, cb,
		RDMA_PS_TCP, IB_QPT_RC);
	if (IS_ERR(cb->cm_id)) {
	  pr_err("failed to create cm id: %ld\n", PTR_ERR(cb->cm_id));
		return -ENODEV;
	}

	sswap_run_client(cb);
	pr_info("sswap_run_client() done\n");

	sswap_setup_wr(cb);
	pr_info("sswap_setup_wr() done.\n");

	if (cb->qp == NULL) {
		pr_info("qp is NULL\n");
	}	
	pr_info("Calling ib_post_recv() with qp = %p, rq_wr = %p\n", cb->qp, &(cb->rq_wr));

	ret = ib_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	if (ret) {
		pr_info("ib_post_recv() failed\n");
	}
	pr_info("ib_post_recv() done\n");

	/*
	queue->cm_error = -ETIMEDOUT;

	ret = rdma_resolve_addr(queue->cm_id, &ctrl->srcaddr, &ctrl->addr,
		CONNECTION_TIMEOUT_MS);
	if (ret) {
		pr_err("rdma_resolve_addr failed: %d\n", ret);
		goto out_destroy_cm_id;
	}

	ret = sswap_rdma_wait_for_cm(queue);
	if (ret) {
		pr_err("sswap_rdma_wait_for_cm failed\n");
		goto out_destroy_cm_id;
	}
	*/

	return 0;

	// out_destroy_cm_id:
	//   	rdma_destroy_id(queue->cm_id);
	//   	return ret;
}

static void sswap_rdma_stop_queue(struct sswap_cb *cb)
{
	rdma_disconnect(cb->cm_id);
}

static void sswap_rdma_free_queue(struct sswap_cb *cb)
{
  rdma_destroy_qp(cb->cm_id);
  ib_free_cq(cb->cq);
  rdma_destroy_id(cb->cm_id);
}

static int sswap_rdma_init_queues(struct sswap_rdma_ctrl *ctrl)
{
	int ret, i;
	pr_info("num queues = %d\n", numqueues);
	for (i = 0; i < numqueues; ++i) {
		ret = sswap_rdma_init_queue(ctrl, i);
		if (ret) {
		pr_err("failed to initialized queue: %d\n", i);
		goto out_free_queues;
		}
	}

	return 0;

out_free_queues:
	for (i--; i >= 0; i--) {
		sswap_rdma_stop_queue(&ctrl->cbs[i]);
		sswap_rdma_free_queue(&ctrl->cbs[i]);
	}

	return ret;
}

static void sswap_rdma_stopandfree_queues(struct sswap_rdma_ctrl *ctrl)
{
  int i;
  for (i = 0; i < numqueues; ++i) {
    sswap_rdma_stop_queue(&ctrl->cbs[i]);
    sswap_rdma_free_queue(&ctrl->cbs[i]);
  }
}

static int sswap_rdma_parse_ipaddr(struct sockaddr_in *saddr, char *ip)
{
  u8 *addr = (u8 *)&saddr->sin_addr.s_addr;
  size_t buflen = strlen(ip);

  pr_info("start: %s\n", __FUNCTION__);

  if (buflen > INET_ADDRSTRLEN)
    return -EINVAL;
  if (in4_pton(ip, buflen, addr, '\0', NULL) == 0)
    return -EINVAL;
  saddr->sin_family = AF_INET;
  return 0;
}

static int sswap_rdma_create_ctrl(struct sswap_rdma_ctrl **c)
{
  int ret;
  struct sswap_rdma_ctrl *ctrl;
  pr_info("will try to connect to %s:%d\n", serverip, serverport);

  *c = kzalloc(sizeof(struct sswap_rdma_ctrl), GFP_KERNEL);
  if (!*c) {
    pr_err("no mem for ctrl\n");
    return -ENOMEM;
  }
  ctrl = *c;

  ctrl->cbs = kzalloc(sizeof(struct sswap_cb) * numqueues, GFP_KERNEL);  
  ctrl->queues = kzalloc(sizeof(struct rdma_queue) * numqueues, GFP_KERNEL);
  ret = sswap_rdma_parse_ipaddr(&(ctrl->addr_in), serverip);
  if (ret) {
    pr_err("sswap_rdma_parse_ipaddr failed: %d\n", ret);
    return -EINVAL;
  }
  ctrl->addr_in.sin_port = cpu_to_be16(serverport);

  ret = sswap_rdma_parse_ipaddr(&(ctrl->srcaddr_in), clientip);
  if (ret) {
    pr_err("sswap_rdma_parse_ipaddr failed: %d\n", ret);
    return -EINVAL;
  }
  /* no need to set the port on the srcaddr */

  return sswap_rdma_init_queues(ctrl);
}

static void __exit sswap_rdma_cleanup_module(void)
{
	sswap_rdma_stopandfree_queues(gctrl);
	ib_unregister_client(&sswap_rdma_ib_client);
	kfree(gctrl);
	gctrl = NULL;
	if (req_cache) {
		kmem_cache_destroy(req_cache);
	}
	fastswap_print_timing_stats();
}

static void sswap_rdma_write_done(struct ib_cq *cq, struct ib_wc *wc)
{
  struct rdma_req *req =
    container_of(wc->wr_cqe, struct rdma_req, cqe);
  struct rdma_queue *q = cq->cq_context;
  struct ib_device *ibdev = q->ctrl->rdev->dev;

  if (unlikely(wc->status != IB_WC_SUCCESS)) {
    pr_err("sswap_rdma_write_done status is not success, it is=%d\n", wc->status);
    //q->write_error = wc->status;
  }
  ib_dma_unmap_page(ibdev, req->dma, PAGE_SIZE, DMA_TO_DEVICE);

  atomic_dec(&q->pending);
  kmem_cache_free(req_cache, req);
}

static void sswap_rdma_read_done(struct ib_cq *cq, struct ib_wc *wc)
{
  struct rdma_req *req =
    container_of(wc->wr_cqe, struct rdma_req, cqe);
  struct rdma_queue *q = cq->cq_context;
  struct ib_device *ibdev = q->ctrl->rdev->dev;

  if (unlikely(wc->status != IB_WC_SUCCESS))
    pr_err("sswap_rdma_read_done status is not success, it is=%d\n", wc->status);

  ib_dma_unmap_page(ibdev, req->dma, PAGE_SIZE, DMA_FROM_DEVICE);

  SetPageUptodate(req->page);
  unlock_page(req->page);
  complete(&req->done);
  atomic_dec(&q->pending);
  kmem_cache_free(req_cache, req);
}

int sswap_rdma_post_rdma(struct rdma_queue *q, struct rdma_req *qe,
  struct ib_sge *sge, u64 roffset, enum ib_wr_opcode op)
{
  struct ib_send_wr *bad_wr;
  struct ib_rdma_wr rdma_wr = {};
  int ret;

  BUG_ON(qe->dma == 0);

  sge->addr = qe->dma;
  sge->length = PAGE_SIZE;
  sge->lkey = q->ctrl->rdev->pd->local_dma_lkey;

  /* TODO: add a chain of WR, we already have a list so should be easy
   * to just post requests in batches */
  rdma_wr.wr.next    = NULL;
  rdma_wr.wr.wr_cqe  = &qe->cqe;
  rdma_wr.wr.sg_list = sge;
  rdma_wr.wr.num_sge = 1;
  rdma_wr.wr.opcode  = op;
  rdma_wr.wr.send_flags = IB_SEND_SIGNALED;
  rdma_wr.remote_addr = q->ctrl->servermr.baseaddr + roffset;
  rdma_wr.rkey = q->ctrl->servermr.key;

  atomic_inc(&q->pending);
  ret = ib_post_send(q->qp, &rdma_wr.wr, &bad_wr);
  if (unlikely(ret)) {
    pr_err("ib_post_send failed: %d\n", ret);
  }

  return ret;
}

static void sswap_rdma_recv_remotemr_done(struct ib_cq *cq, struct ib_wc *wc)
{
  struct rdma_req *qe =
    container_of(wc->wr_cqe, struct rdma_req, cqe);
  struct rdma_queue *q = cq->cq_context;
  struct sswap_rdma_ctrl *ctrl = q->ctrl;
  struct ib_device *ibdev = q->ctrl->rdev->dev;

  if (unlikely(wc->status != IB_WC_SUCCESS)) {
    pr_err("sswap_rdma_recv_done status is not success\n");
    return;
  }
  ib_dma_unmap_single(ibdev, qe->dma, sizeof(struct sswap_rdma_memregion),
		      DMA_FROM_DEVICE);
  pr_info("servermr baseaddr=%llx, key=%u\n", ctrl->servermr.baseaddr,
	  ctrl->servermr.key);
  complete_all(&qe->done);
}

static int sswap_rdma_post_recv(struct rdma_queue *q, struct rdma_req *qe,
  size_t bufsize)
{
  struct ib_recv_wr *bad_wr;
  struct ib_recv_wr wr = {};
  struct ib_sge sge;
  int ret;

  sge.addr = qe->dma;
  sge.length = bufsize;
  sge.lkey = q->ctrl->rdev->pd->local_dma_lkey;

  wr.next    = NULL;
  wr.wr_cqe  = &qe->cqe;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  ret = ib_post_recv(q->qp, &wr, &bad_wr);
  if (ret) {
    pr_err("ib_post_recv failed: %d\n", ret);
  }
  return ret;
}

/* allocates a sswap rdma request, creates a dma mapping for it in
 * req->dma, and synchronizes the dma mapping in the direction of
 * the dma map.
 * Don't touch the page with cpu after creating the request for it!
 * Deallocates the request if there was an error */
int get_req_for_page(struct rdma_req **req, struct ib_device *dev,
				struct page *page, enum dma_data_direction dir)
{
  int ret;

  ret = 0;
  *req = kmem_cache_alloc(req_cache, GFP_ATOMIC);
  if (unlikely(!req)) {
    pr_err("no memory for req\n");
    ret = -ENOMEM;
    goto out;
  }

  (*req)->page = page;
  init_completion(&(*req)->done);

  (*req)->dma = ib_dma_map_page(dev, page, 0, PAGE_SIZE, dir);
  if (unlikely(ib_dma_mapping_error(dev, (*req)->dma))) {
    pr_err("ib_dma_mapping_error\n");
    ret = -ENOMEM;
    kmem_cache_free(req_cache, req);
    goto out;
  }

  ib_dma_sync_single_for_device(dev, (*req)->dma, PAGE_SIZE, dir);
out:
  return ret;
}

/* the buffer needs to come from kernel (not high memory) */
inline static int get_req_for_buf(struct rdma_req **req, struct ib_device *dev,
				void *buf, size_t size,
				enum dma_data_direction dir)
{
  int ret;

  ret = 0;
  *req = kmem_cache_alloc(req_cache, GFP_ATOMIC);
  if (unlikely(!req)) {
    pr_err("no memory for req\n");
    ret = -ENOMEM;
    goto out;
  }

  init_completion(&(*req)->done);

  (*req)->dma = ib_dma_map_single(dev, buf, size, dir);
  if (unlikely(ib_dma_mapping_error(dev, (*req)->dma))) {
    pr_err("ib_dma_mapping_error\n");
    ret = -ENOMEM;
    kmem_cache_free(req_cache, req);
    goto out;
  }

  ib_dma_sync_single_for_device(dev, (*req)->dma, size, dir);
out:
  return ret;
}

inline static void sswap_rdma_wait_completion(struct ib_cq *cq,
					      struct rdma_req *qe)
{
  ndelay(1000);
  while (!completion_done(&qe->done)) {
    ndelay(250);
    ib_process_cq_direct(cq, 1);
  }
}

/* polls queue until we reach target completed wrs or qp is empty */
static inline int poll_target(struct rdma_queue *q, int target)
{
  unsigned long flags;
  int completed = 0;

  while (completed < target && atomic_read(&q->pending) > 0) {
    spin_lock_irqsave(&q->cq_lock, flags);
    completed += ib_process_cq_direct(q->cq, target - completed);
    spin_unlock_irqrestore(&q->cq_lock, flags);
    cpu_relax();
  }

  return completed;
}

static inline int drain_queue(struct rdma_queue *q)
{
  unsigned long flags;

  while (atomic_read(&q->pending) > 0) {
    spin_lock_irqsave(&q->cq_lock, flags);
    ib_process_cq_direct(q->cq, 16);
    spin_unlock_irqrestore(&q->cq_lock, flags);
    cpu_relax();
  }

  return 1;
}

int write_queue_add(struct rdma_queue *q, struct page *page,
				  u64 roffset)
{
  struct rdma_req *req;
  struct ib_device *dev = q->ctrl->rdev->dev;
  struct ib_sge sge = {};
  int ret, inflight;

  while ((inflight = atomic_read(&q->pending)) >= QP_MAX_SEND_WR - 8) {
    BUG_ON(inflight > QP_MAX_SEND_WR);
    poll_target(q, 2048);
    pr_info_ratelimited("back pressure writes");
  }

  if (dev == NULL)
	BUG();

  if (page == NULL)
	BUG();

  ret = get_req_for_page(&req, dev, page, DMA_TO_DEVICE);
  if (unlikely(ret))
    return ret;

  req->cqe.done = sswap_rdma_write_done;
  ret = sswap_rdma_post_rdma(q, req, &sge, roffset, IB_WR_RDMA_WRITE);

  return ret;
}

static inline int begin_read(struct rdma_queue *q, struct page *page,
			     u64 roffset)
{
  struct rdma_req *req;
  struct ib_device *dev = q->ctrl->rdev->dev;
  struct ib_sge sge = {};
  int ret, inflight;

  /* back pressure in-flight reads, can't send more than
   * QP_MAX_SEND_WR at a time */
  while ((inflight = atomic_read(&q->pending)) >= QP_MAX_SEND_WR) {
    BUG_ON(inflight > QP_MAX_SEND_WR); /* only valid case is == */
    poll_target(q, 8);
    pr_info_ratelimited("back pressure happened on reads");
  }

  ret = get_req_for_page(&req, dev, page, DMA_TO_DEVICE);
  if (unlikely(ret))
    return ret;

  req->cqe.done = sswap_rdma_read_done;
  ret = sswap_rdma_post_rdma(q, req, &sge, roffset, IB_WR_RDMA_READ);
  return ret;
}

int sswap_new_rdma_write(struct page *page, u64 roffset)
{
	struct ib_send_wr *bad_wr;
	struct ib_wc wc;
	int ret;
	struct sswap_cb *cb;

	cb = sswap_rdma_get_cb(smp_processor_id() % numprocs, QP_WRITE_SYNC);
	BUG_ON(cb == NULL);
	pr_info("%s: got cb = %p\n", __FUNCTION__, cb);

	cb->state = RDMA_READ_ADV;

	sswap_page_evict_format_send(cb, cb->start_dma_addr, roffset, page);
	if (cb->state == ERROR) {
		pr_info("sswap_page_fault_format_send() failed\n");
		return cb->state;
	}
	pr_info("%s: page_evict_format_send() is set\n", __FUNCTION__);

	ret = ib_post_send(cb->qp, &cb->sq_evict_wr, &bad_wr);
	if (ret) {
		pr_info("post send error %d\n", ret);
		return ret;
	}
	pr_info("%s: ib_post_send() is done\n", __FUNCTION__);
	
	/* Wait for server to ACK */
	// wait_event_interruptible(cb->sem, cb->state >= RDMA_RECEIVED);
	// if (cb->state != RDMA_RECEIVED) {
	// 	pr_info("wait for RDMA_RECEIVED state %d\n",
	// 			cb->state);
	// 	ret = -1;
	// 	return ret;
	// }

	// Spin wait for send completion
	while ((ret = ib_poll_cq(cb->cq, 1, &wc) == 0));
	if (ret < 0) {
		pr_info("poll error %d\n", ret);
		return ret;
	}

	if (wc.status) {
		pr_info("send completion error %d\n", wc.status);
		return wc.status;
	}
	pr_info("Sent request sending complete\n");

	while (cb->state != RDMA_RECEIVED) {
		sswap_cq_event_handler(cb->cq, cb);
		if (cb->state == ERROR) {
			pr_info("sswap_cq_event_handler returned ERROR\n");
			return -1;
		}
	}

	pr_info("Received RDMA meaning that the remote server has reflected the eviction\n");

	return 0;
}
EXPORT_SYMBOL(sswap_new_rdma_write);

int sswap_rdma_write(struct page *page, u64 roffset)
{
  int ret;
  struct rdma_queue *q;

  timing_t write_time;

  FASTSWAP_START_TIMING(write_t, write_time);

  VM_BUG_ON_PAGE(!PageSwapCache(page), page);

  q = sswap_rdma_get_queue(smp_processor_id() % numprocs, QP_WRITE_SYNC);
  BUG_ON(q == NULL);
  ret = write_queue_add(q, page, roffset);
  BUG_ON(ret);
  drain_queue(q);

  FASTSWAP_END_TIMING(write_t, write_time);
  return ret;
}
EXPORT_SYMBOL(sswap_rdma_write);

static int sswap_rdma_recv_remotemr(struct sswap_rdma_ctrl *ctrl)
{
  struct rdma_req *qe;
  int ret;
  struct ib_device *dev;

  pr_info("start: %s\n", __FUNCTION__);
  dev = ctrl->rdev->dev;

  ret = get_req_for_buf(&qe, dev, &(ctrl->servermr), sizeof(ctrl->servermr),
			DMA_FROM_DEVICE);
  if (unlikely(ret))
    goto out;

  qe->cqe.done = sswap_rdma_recv_remotemr_done;

  ret = sswap_rdma_post_recv(&(ctrl->queues[0]), qe, sizeof(struct sswap_rdma_memregion));

  if (unlikely(ret))
    goto out_free_qe;

  /* this delay doesn't really matter, only happens once */
  sswap_rdma_wait_completion(ctrl->queues[0].cq, qe);

out_free_qe:
  kmem_cache_free(req_cache, qe);
out:
  return ret;
}

/* page is unlocked when the wr is done.
 * posts an RDMA read on this cpu's qp */
int sswap_rdma_read_async(struct page *page, u64 roffset)
{
  struct rdma_queue *q;
  int ret;

  timing_t read_async_time;

  FASTSWAP_START_TIMING(read_async_t, read_async_time);

  VM_BUG_ON_PAGE(!PageSwapCache(page), page);
  VM_BUG_ON_PAGE(!PageLocked(page), page);
  VM_BUG_ON_PAGE(PageUptodate(page), page);

  q = sswap_rdma_get_queue(smp_processor_id() % numprocs, QP_READ_ASYNC);
  ret = begin_read(q, page, roffset);
  return ret;

  FASTSWAP_END_TIMING(read_async_t, read_async_time);
}
EXPORT_SYMBOL(sswap_rdma_read_async);

int sswap_rdma_read_sync(struct page *page, u64 roffset)
{
  struct rdma_queue *q;
  int ret;

  timing_t read_time;

  FASTSWAP_START_TIMING(read_t, read_time);

  VM_BUG_ON_PAGE(!PageSwapCache(page), page);
  VM_BUG_ON_PAGE(!PageLocked(page), page);
  VM_BUG_ON_PAGE(PageUptodate(page), page);

  q = sswap_rdma_get_queue(smp_processor_id() % numprocs, QP_READ_SYNC);
  ret = begin_read(q, page, roffset);

  FASTSWAP_END_TIMING(read_t, read_time);
  return ret;
}
EXPORT_SYMBOL(sswap_rdma_read_sync);

int sswap_rdma_poll_load(int cpu)
{
  struct rdma_queue *q = sswap_rdma_get_queue(cpu, QP_READ_SYNC);
  return drain_queue(q);
}
EXPORT_SYMBOL(sswap_rdma_poll_load);

/* idx is absolute id (i.e. > than number of cpus) */
inline enum qp_type get_queue_type(unsigned int idx)
{
  // numcpus = 8
  if (idx < numcpus)
    return QP_READ_SYNC;
  else if (idx < numcpus * 2)
    return QP_READ_ASYNC;
  else if (idx < numcpus * 3)
    return QP_WRITE_SYNC;

  BUG();
  return QP_READ_SYNC;
}

inline struct rdma_queue *sswap_rdma_get_queue(unsigned int cpuid,
					       enum qp_type type)
{
  BUG_ON(gctrl == NULL);

  switch (type) {
    case QP_READ_SYNC:
      return &gctrl->queues[cpuid];
    case QP_READ_ASYNC:
      return &gctrl->queues[cpuid + numcpus];
    case QP_WRITE_SYNC:
      return &gctrl->queues[cpuid + numcpus * 2];
    default:
      BUG();
  };
}

int measure_timing = 1;

const char *Timingstring[TIMING_NUM] = {
	"================ Reads ================",
	"read",
	"read_async",

	"================ Writes ===============",
	"write",
};

u64 Timingstats[TIMING_NUM];
DEFINE_PER_CPU(u64[TIMING_NUM], Timingstats_percpu);
u64 Countstats[TIMING_NUM];
DEFINE_PER_CPU(u64[TIMING_NUM], Countstats_percpu);

void fastswap_get_timing_stats(void)
{
	int i;
	int cpu;

	for (i = 0; i < TIMING_NUM; i++) {
		Timingstats[i] = 0;
		Countstats[i] = 0;
		for_each_possible_cpu(cpu) {
			Timingstats[i] += per_cpu(Timingstats_percpu[i], cpu);
			Countstats[i] += per_cpu(Countstats_percpu[i], cpu);
		}
	}
}

void fastswap_print_timing_stats(void)
{
	int i;

	fastswap_get_timing_stats();

	pr_info("=============== Fastswap Timing Stats ================\n");
	for (i = 0; i < TIMING_NUM; i++) {
		if (Timingstring[i][0] == '=') {
			pr_info("\n%s\n", Timingstring[i]);
			continue;
		}

		if (measure_timing || Timingstats[i]) {
			pr_info("%s: count %llu, timing %llu, average %llu\n",
					Timingstring[i],
					Countstats[i],
					Timingstats[i],
					Countstats[i] ?
					Timingstats[i] / Countstats[i] : 0);
		} else {
			pr_info("%s: count %llu\n",
					Timingstring[i],
					Countstats[i]);
		}
	}

	pr_info("\n");
}

void fastswap_clear_timing_stats(void)
{
	int i;
	int cpu;

	for (i = 0; i < TIMING_NUM; i++) {
		Countstats[i] = 0;
		Timingstats[i] = 0;
		for_each_possible_cpu(cpu) {
			per_cpu(Timingstats_percpu[i], cpu) = 0;
			per_cpu(Countstats_percpu[i], cpu) = 0;
		}
	}
}

int receiver_loop(void *idx) {
	unsigned int i = 0;
	int t_id = * (int *)idx;

	// kthread_should_stop call is important
	while (!kthread_should_stop()) {
		pr_info("Thread %d is still running...! %d secs\n", t_id, i);
		i++;
		if (i == 30)
			break;
		msleep(1000);
	}
	pr_info ("Thread %d stopped\n", t_id);
	return 0;
}


static int __init sswap_rdma_init_module(void)
{
  int ret;
  char receiver_thread_name[20];
  int receiver_thread_idx = 0;
  struct task_struct *receiver_thread = NULL;

  pr_info("start: %s\n", __FUNCTION__);
  pr_info("* RDMA BACKEND *\n");

  numcpus = numprocs;
  numqueues = numcpus * 3;

  pr_info("** NUM QUEUES = %d **\n", numqueues);
  req_cache = kmem_cache_create("sswap_req_cache", sizeof(struct rdma_req), 0,
                      SLAB_TEMPORARY | SLAB_HWCACHE_ALIGN, NULL);

  if (!req_cache) {
    pr_err("no memory for cache allocation\n");
    return -ENOMEM;
  }

  snprintf(receiver_thread_name, strlen(receiver_thread_name), "kthread_%d", 1);
  receiver_thread = kthread_create(receiver_loop, &receiver_thread_idx, (const char *)receiver_thread_name);
  if (receiver_thread != NULL) {
	  wake_up_process(receiver_thread);
	  pr_info("%s is running\n", receiver_thread_name);
  } else {
	  pr_info("kthread %s could not be created\n", receiver_thread_name);
  }

  ib_register_client(&sswap_rdma_ib_client);
  ret = sswap_rdma_create_ctrl(&gctrl);
  if (ret) {
    pr_err("could not create ctrl\n");
    ib_unregister_client(&sswap_rdma_ib_client);
    return -ENODEV;
  }

  /*
  ret = sswap_rdma_recv_remotemr(gctrl);
  if (ret) {
    pr_err("could not setup remote memory region\n");
    ib_unregister_client(&sswap_rdma_ib_client);
    return -ENODEV;
  }
  */

  fastswap_clear_timing_stats();
  pr_info("ctrl is ready for reqs\n");
  return 0;
}

module_init(sswap_rdma_init_module);
module_exit(sswap_rdma_cleanup_module);

MODULE_LICENSE("GPL v2");
MODULE_DESCRIPTION("Experiments");
