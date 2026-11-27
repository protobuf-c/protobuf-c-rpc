/* Tests for the prepare functions of ProtobufCRPCDispatch. */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <protobuf-c-rpc/protobuf-c-rpc-dispatch.h>

static void *
test_alloc (void *data, size_t size)
{
  (void) data;
  return malloc (size);
}
static void
test_free (void *data, void *ptr)
{
  (void) data;
  free (ptr);
}
static ProtobufCAllocator test_allocator = { test_alloc, test_free, NULL };
#define NEW_DISPATCH() protobuf_c_rpc_dispatch_new (&test_allocator)

typedef struct {
  ProtobufCRPCDispatch *d;
  ProtobufCRPCDispatchPrepare *self;
  int calls;
  int timer_fired;
  int remove_after;
} State;

static void
count_prepare (ProtobufCRPCDispatch *d, void *data)
{
  State *s = data;
  (void) d;
  s->calls++;
  if (s->remove_after && s->calls == s->remove_after)
    protobuf_c_rpc_dispatch_remove_prepare (s->self);
}

static void
timer_func (ProtobufCRPCDispatch *d, void *data)
{
  State *s = data;
  (void) d;
  s->timer_fired++;
}

/* A prepare function runs once per iteration, even with nothing to do, and
 * does not make the dispatch spin: with a timer pending, the wait lasts for
 * the timer. */
static void
test_runs_once_per_iteration (void)
{
  State s = { NEW_DISPATCH (), NULL, 0, 0, 0 };
  s.self = protobuf_c_rpc_dispatch_add_prepare (s.d, count_prepare, &s);
  protobuf_c_rpc_dispatch_add_timer_millis (s.d, 50, timer_func, &s);
  while (!s.timer_fired)
    protobuf_c_rpc_dispatch_run (s.d);
  /* one iteration to wait for the timer, one more at most (poll waking
     slightly early); a spinning loop would have counted thousands */
  assert (s.calls >= 1 && s.calls <= 3);
  protobuf_c_rpc_dispatch_free (s.d);
}

/* A prepare function may remove itself, and stops being called. */
static void
test_remove_self (void)
{
  State s = { NEW_DISPATCH (), NULL, 0, 0, 2 };
  int i;
  s.self = protobuf_c_rpc_dispatch_add_prepare (s.d, count_prepare, &s);
  for (i = 0; i < 5; i++)
    {
      protobuf_c_rpc_dispatch_add_timer_millis (s.d, 1, timer_func, &s);
      protobuf_c_rpc_dispatch_run (s.d);
    }
  assert (s.calls == 2);
  protobuf_c_rpc_dispatch_free (s.d);
}

/* Removing a prepare function before it has run, outside a run. */
static void
test_remove_outside (void)
{
  State s = { NEW_DISPATCH (), NULL, 0, 0, 0 };
  s.self = protobuf_c_rpc_dispatch_add_prepare (s.d, count_prepare, &s);
  protobuf_c_rpc_dispatch_remove_prepare (s.self);
  protobuf_c_rpc_dispatch_add_timer_millis (s.d, 1, timer_func, &s);
  protobuf_c_rpc_dispatch_run (s.d);
  assert (s.calls == 0);
  protobuf_c_rpc_dispatch_free (s.d);
}

/* A prepare function can arrange work for this very iteration: here it adds
 * an idle function, so the poll must not block (no timer or fd exists, so it
 * would block forever otherwise). */
typedef struct { int idle_ran; int added; } IdleState;

static void
idle_func (ProtobufCRPCDispatch *d, void *data)
{
  IdleState *s = data;
  (void) d;
  s->idle_ran = 1;
}

static void
adds_idle (ProtobufCRPCDispatch *d, void *data)
{
  IdleState *s = data;
  if (!s->added)
    {
      s->added = 1;
      protobuf_c_rpc_dispatch_add_idle (d, idle_func, s);
    }
}

static void
test_prepare_adds_idle (void)
{
  IdleState s = { 0, 0 };
  ProtobufCRPCDispatch *d = NEW_DISPATCH ();
  protobuf_c_rpc_dispatch_add_prepare (d, adds_idle, &s);
  protobuf_c_rpc_dispatch_run (d);
  assert (s.idle_ran);
  protobuf_c_rpc_dispatch_free (d);
}

/* Order of calls; a function added by a prepare function waits a turn; a
 * prepare function may remove another. */
typedef struct {
  ProtobufCRPCDispatch *d;
  char log[16];
  unsigned n;
  ProtobufCRPCDispatchPrepare *b;
  int added_c;
} OrderState;

static void
log_a (ProtobufCRPCDispatch *d, void *data)
{
  OrderState *s = data;
  (void) d;
  s->log[s->n++] = 'a';
}
static void
log_c (ProtobufCRPCDispatch *d, void *data)
{
  OrderState *s = data;
  (void) d;
  s->log[s->n++] = 'c';
}
static void
log_b (ProtobufCRPCDispatch *d, void *data)
{
  OrderState *s = data;
  s->log[s->n++] = 'b';
  if (!s->added_c)
    {
      s->added_c = 1;
      protobuf_c_rpc_dispatch_add_prepare (d, log_c, s);
    }
}

static void
test_order (void)
{
  OrderState s = { NEW_DISPATCH (), {0}, 0, NULL, 0 };
  protobuf_c_rpc_dispatch_add_prepare (s.d, log_a, &s);
  s.b = protobuf_c_rpc_dispatch_add_prepare (s.d, log_b, &s);
  protobuf_c_rpc_dispatch_run_prepares (s.d);   /* a b (c added) */
  protobuf_c_rpc_dispatch_run_prepares (s.d);   /* a b c */
  assert (s.n == 5 && s.log[0] == 'a' && s.log[1] == 'b'
          && s.log[2] == 'a' && s.log[3] == 'b' && s.log[4] == 'c');
  protobuf_c_rpc_dispatch_remove_prepare (s.b);
  protobuf_c_rpc_dispatch_run_prepares (s.d);   /* a c */
  assert (s.n == 7 && s.log[5] == 'a' && s.log[6] == 'c');
  protobuf_c_rpc_dispatch_free (s.d);
}

/* One iteration of protobuf_c_rpc_dispatch_run() calls them exactly once. */
static void
test_once_in_run (void)
{
  State s = { NEW_DISPATCH (), NULL, 0, 0, 0 };
  s.self = protobuf_c_rpc_dispatch_add_prepare (s.d, count_prepare, &s);
  protobuf_c_rpc_dispatch_add_timer_millis (s.d, 1, timer_func, &s);
  protobuf_c_rpc_dispatch_run (s.d);
  assert (s.calls == 1);
  protobuf_c_rpc_dispatch_free (s.d);
}

/* A caller that drives protobuf_c_rpc_dispatch_dispatch() itself and knows
 * nothing about prepare functions still gets them, once per dispatch; one
 * that calls run_prepares() before waiting does not get them twice. */
static void
test_embedded (void)
{
  State s = { NEW_DISPATCH (), NULL, 0, 0, 0 };
  s.self = protobuf_c_rpc_dispatch_add_prepare (s.d, count_prepare, &s);

  protobuf_c_rpc_dispatch_dispatch (s.d, 0, NULL);
  assert (s.calls == 1);
  protobuf_c_rpc_dispatch_dispatch (s.d, 0, NULL);
  assert (s.calls == 2);

  protobuf_c_rpc_dispatch_run_prepares (s.d);
  assert (s.calls == 3);
  protobuf_c_rpc_dispatch_dispatch (s.d, 0, NULL);
  assert (s.calls == 3);
  protobuf_c_rpc_dispatch_run_prepares (s.d);
  protobuf_c_rpc_dispatch_dispatch (s.d, 0, NULL);
  assert (s.calls == 4);
  /* and back to relying on dispatch_dispatch */
  protobuf_c_rpc_dispatch_dispatch (s.d, 0, NULL);
  assert (s.calls == 5);
  protobuf_c_rpc_dispatch_free (s.d);
}

int
main (void)
{
  test_runs_once_per_iteration ();
  test_remove_self ();
  test_remove_outside ();
  test_prepare_adds_idle ();
  test_order ();
  test_once_in_run ();
  test_embedded ();
  puts ("dispatch: ok");
  return 0;
}
