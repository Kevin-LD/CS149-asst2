#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <queue>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <unordered_set>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        struct bulkLaunch {
            TaskID lauch_id;
            IRunnable* runnable;
            int num_total_tasks;
            int num_unresolved_deps;
            int next_task;
            int num_tasks_finished;
            bool finished;
            // bulkLauches depending on curent bulkLaunch
            std::vector<bulkLaunch*> dependents;
            bulkLaunch(TaskID lauch_id, IRunnable* runnable, int num_total_tasks, int num_unresolved_deps)
                : lauch_id(lauch_id), runnable(runnable), num_total_tasks(num_total_tasks), num_unresolved_deps(num_unresolved_deps), next_task(0), num_tasks_finished(0), finished(false) {}
        };
        std::queue<bulkLaunch*> ready_queue;
        // 希望通过 launch_id 找到 launch 做完成 deps 的通知
        std::vector<bulkLaunch*> all_launched;
        int num_threads;
        // 不在 ready_queue 上，但还有 thread 在工作的 bulkLaunch 数量
        int waiting_to_finish;
        std::atomic<bool> thread_pool_survive;
        std::thread* thread_pool;
        void worker();
        std::mutex mtx;
        std::condition_variable cv_done;
        std::condition_variable cv_ready;
        TaskID next_bulk_launch_id;
        int check_deps(const std::vector<TaskID>& deps);
};

#endif
