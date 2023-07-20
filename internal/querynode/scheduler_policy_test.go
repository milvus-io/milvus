package querynode

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/crypto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestScheduler_newReadScheduleTaskPolicy(t *testing.T) {
	policy := newReadScheduleTaskPolicy(scheduleReadPolicyNameFIFO)
	assert.IsType(t, policy, &fifoScheduleReadPolicy{})
	policy = newReadScheduleTaskPolicy("")
	assert.IsType(t, policy, &fifoScheduleReadPolicy{})
	policy = newReadScheduleTaskPolicy(scheduleReadPolicyNameUserTaskPolling)
	assert.IsType(t, policy, &userTaskPollingScheduleReadPolicy{})
	assert.Panics(t, func() {
		newReadScheduleTaskPolicy("other")
	})
}

func TestScheduler_fifoPolicyMinNum(t *testing.T) {
	policy := newFIFOScheduleReadPolicy()
	for i := 1; i <= 10; i++ {
		t := mockReadTask{
			cpuUsage: 20,
		}
		policy.addTask(&t)
	}
	maxNum := int32(4)

	targetUsage := int32(0)
	minNum := int32(1)

	// allow 1 task even if out of cpu
	tasks, cost := policy.schedule(targetUsage, maxNum, minNum)
	assert.Equal(t, int32(20), cost)
	assert.Equal(t, int32(1), int32(len(tasks)))

	targetUsage = 20
	minNum = 2

	// allow 2 tasks even if remaining cpu has room for only 1 task
	tasks, cost = policy.schedule(targetUsage, maxNum, minNum)
	assert.Equal(t, int32(40), cost)
	assert.Equal(t, int32(2), int32(len(tasks)))

	targetUsage = 60
	minNum = 2

	// allow more than minNum tasks when we have sufficient cpu
	tasks, cost = policy.schedule(targetUsage, maxNum, minNum)
	assert.Equal(t, int32(60), cost)
	assert.Equal(t, int32(3), int32(len(tasks)))

}

func TestScheduler_defaultScheduleReadPolicy(t *testing.T) {
	policy := newFIFOScheduleReadPolicy()
	testBasicScheduleReadPolicy(t, policy)

	for i := 1; i <= 100; i++ {
		t := mockReadTask{
			cpuUsage: int32(i * 10),
		}
		policy.addTask(&t)
	}

	targetUsage := int32(100)
	maxNum := int32(2)

	tasks, cur := policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, int32(30), cur)
	assert.Equal(t, int32(2), int32(len(tasks)))

	targetUsage = 300
	maxNum = 0
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, int32(0), cur)
	assert.Equal(t, 0, len(tasks))

	targetUsage = 0
	maxNum = 0
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, int32(0), cur)
	assert.Equal(t, 0, len(tasks))

	targetUsage = 0
	maxNum = 300
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, int32(0), cur)
	assert.Equal(t, 0, len(tasks))

	actual := int32(180)     // sum(3..6) * 10   3 + 4 + 5 + 6
	targetUsage = int32(190) // > actual
	maxNum = math.MaxInt32
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, actual, cur)
	assert.Equal(t, 4, len(tasks))

	actual = 340 // sum(7..10) * 10 ,  7+ 8 + 9 + 10
	targetUsage = 340
	maxNum = 4
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, actual, cur)
	assert.Equal(t, 4, len(tasks))

	actual = 4995 * 10 // sum(11..100)
	targetUsage = actual
	maxNum = 90
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, actual, cur)
	assert.Equal(t, 90, len(tasks))
	assert.Equal(t, 0, policy.len())
}

func TestScheduler_userTaskPollingScheduleReadPolicy(t *testing.T) {
	policy := newUserTaskPollingScheduleReadPolicy(time.Minute)
	testBasicScheduleReadPolicy(t, policy)

	for i := 1; i <= 100; i++ {
		policy.addTask(&mockReadTask{
			cpuUsage: int32(i * 10),
			mockTask: mockTask{
				baseTask: baseTask{
					ctx: getContextWithAuthorization(context.Background(), fmt.Sprintf("user%d:123456", i%10)),
				},
			},
		})
	}
	targetUsage := int32(100)
	maxNum := int32(2)

	tasks, cur := policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, int32(30), cur)
	assert.Equal(t, int32(2), int32(len(tasks)))
	assert.Equal(t, 98, policy.len())

	targetUsage = 300
	maxNum = 0
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, int32(0), cur)
	assert.Equal(t, 0, len(tasks))
	assert.Equal(t, 98, policy.len())

	targetUsage = 0
	maxNum = 0
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, int32(0), cur)
	assert.Equal(t, 0, len(tasks))
	assert.Equal(t, 98, policy.len())

	targetUsage = 0
	maxNum = 300
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, int32(0), cur)
	assert.Equal(t, 0, len(tasks))
	assert.Equal(t, 98, policy.len())

	actual := int32(180)     // sum(3..6) * 10   3 + 4 + 5 + 6
	targetUsage = int32(190) // > actual
	maxNum = math.MaxInt32
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, actual, cur)
	assert.Equal(t, 4, len(tasks))
	assert.Equal(t, 94, policy.len())

	actual = 340 // sum(7..10) * 10 ,  7+ 8 + 9 + 10
	targetUsage = 340
	maxNum = 4
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, actual, cur)
	assert.Equal(t, 4, len(tasks))
	assert.Equal(t, 90, policy.len())

	actual = 4995 * 10 // sum(11..100)
	targetUsage = actual
	maxNum = 90
	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, actual, cur)
	assert.Equal(t, 90, len(tasks))
	assert.Equal(t, 0, policy.len())

	time.Sleep(time.Minute + time.Second)
	policy.addTask(&mockReadTask{
		cpuUsage: int32(1),
		mockTask: mockTask{
			baseTask: baseTask{
				ctx: getContextWithAuthorization(context.Background(), fmt.Sprintf("user%d:123456", 11)),
			},
		},
	})

	tasks, cur = policy.schedule(targetUsage, maxNum, 0)
	assert.Equal(t, int32(1), cur)
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, 0, policy.len())
	policyInner := policy.(*userTaskPollingScheduleReadPolicy)
	assert.Equal(t, 1, len(policyInner.route))
	assert.Equal(t, 1, policyInner.checkpoint.Len())
}

func Test_userBasedTaskQueue(t *testing.T) {
	n := 50
	q := newUserBasedTaskQueue("test_user")
	for i := 1; i <= n; i++ {
		q.push(&mockReadTask{
			cpuUsage: int32(i * 10),
			mockTask: mockTask{
				baseTask: baseTask{
					ctx: getContextWithAuthorization(context.Background(), "default:123456"),
				},
			},
		})
		assert.Equal(t, q.len(), i)
		assert.Equal(t, q.expire(time.Second), false)
	}

	for i := 0; i < n; i++ {
		q.pop()
		assert.Equal(t, q.len(), n-(i+1))
		assert.Equal(t, q.expire(time.Second), false)
	}

	time.Sleep(time.Second)
	assert.Equal(t, q.expire(time.Second), true)
}

func testBasicScheduleReadPolicy(t *testing.T, policy scheduleReadPolicy) {
	// test, push and schedule.
	for i := 1; i <= 50; i++ {
		cpuUsage := int32(i * 10)
		id := 1

		policy.addTask(&mockReadTask{
			cpuUsage: cpuUsage,
			mockTask: mockTask{
				baseTask: baseTask{
					ctx: getContextWithAuthorization(context.Background(), "default:123456"),
					id:  UniqueID(id),
				},
			},
		})
		assert.Equal(t, policy.len(), 1)
		task, cost := policy.schedule(cpuUsage, 1, 0)
		assert.Equal(t, cost, cpuUsage)
		assert.Equal(t, len(task), 1)
		assert.Equal(t, task[0].ID(), int64(id))
	}

	// test, can not merge and schedule.
	cpuUsage := int32(100)
	notMergeTask := &mockReadTask{
		cpuUsage: cpuUsage,
		canMerge: false,
		mockTask: mockTask{
			baseTask: baseTask{
				ctx: getContextWithAuthorization(context.Background(), "default:123456"),
			},
		},
	}
	assert.False(t, policy.mergeTask(notMergeTask))
	policy.addTask(notMergeTask)
	assert.Equal(t, policy.len(), 1)
	task2 := &mockReadTask{
		cpuUsage: cpuUsage,
		canMerge: false,
		mockTask: mockTask{
			baseTask: baseTask{
				ctx: getContextWithAuthorization(context.Background(), "default:123456"),
			},
		},
	}
	assert.False(t, policy.mergeTask(task2))
	assert.Equal(t, policy.len(), 1)
	policy.addTask(notMergeTask)
	assert.Equal(t, policy.len(), 2)
	task, cost := policy.schedule(2*cpuUsage, 1, 0)
	assert.Equal(t, cost, cpuUsage)
	assert.Equal(t, len(task), 1)
	assert.Equal(t, policy.len(), 1)
	assert.False(t, task[0].(*mockReadTask).merged)
	task, cost = policy.schedule(2*cpuUsage, 1, 0)
	assert.Equal(t, cost, cpuUsage)
	assert.Equal(t, len(task), 1)
	assert.Equal(t, policy.len(), 0)
	assert.False(t, task[0].(*mockReadTask).merged)

	// test, can merge and schedule.
	mergeTask := &mockReadTask{
		cpuUsage: cpuUsage,
		canMerge: true,
		mockTask: mockTask{
			baseTask: baseTask{
				ctx: getContextWithAuthorization(context.Background(), "default:123456"),
			},
		},
	}
	policy.addTask(mergeTask)
	task2 = &mockReadTask{
		cpuUsage: cpuUsage,
		mockTask: mockTask{
			baseTask: baseTask{
				ctx: getContextWithAuthorization(context.Background(), "default:123456"),
			},
		},
	}
	assert.True(t, policy.mergeTask(task2))
	assert.Equal(t, policy.len(), 1)
	task, cost = policy.schedule(cpuUsage, 1, 0)
	assert.Equal(t, cost, cpuUsage)
	assert.Equal(t, len(task), 1)
	assert.Equal(t, policy.len(), 0)
	assert.True(t, task[0].(*mockReadTask).merged)
}

func getContextWithAuthorization(ctx context.Context, originValue string) context.Context {
	authKey := strings.ToLower(util.HeaderAuthorize)
	authValue := crypto.Base64Encode(originValue)
	contextMap := map[string]string{
		authKey: authValue,
	}
	md := metadata.New(contextMap)
	return metadata.NewIncomingContext(ctx, md)
}
