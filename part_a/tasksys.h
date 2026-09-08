#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <mutex>
#include <thread>
#include <condition_variable>

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
    private:
        int num_threads;

        // static assignment
        // void worker(IRunnable* runnable, int num_total_tasks, int start, int end);
        // dynamic assignment
        void worker(IRunnable* runnable, int num_total_tasks, std::atomic<int> *counter);
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
    private:
        int num_threads;
        std::atomic<bool> thread_pool_survive;
        std::thread *thread_pool;
        // next task id
        int counter;
        
        int num_total_tasks;
        int num_tasks_finished;
        // 由于 counter 和 num_total_tasks 总是同时访问，所以可以只用一个锁
        std::mutex counter_num_total_tasks_mtx;
        std::mutex num_tasks_finished_mtx;
        IRunnable* cur_bulk_runnable;
        // worker function
        void worker(bool is_main);
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
// 
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
        int num_threads;
        std::atomic<bool> thread_pool_survive;
        std::thread *thread_pool;
        // next task id
        int counter;
        
        int num_total_tasks;
        int num_tasks_finished;
        // 这里非常神秘：实验发现 Sleeping 版本使用 2 个 mutex 时，
        // 性能显著差于使用 1 个 mutex。
        // 但对于 Spinning 版本，实验结果却相反：2 个 mutex 反而更快。
        //
        // 因此，mutex 数量本身似乎不能直接解释性能差异。
        // 猜想可能与 condition variable、mutex 以及线程唤醒/重新竞争锁
        // 之间的交互产生的 synchronization overhead 有关，
        // 但尚未进一步实验验证。
        std::mutex mtx;
        IRunnable* cur_bulk_runnable;
        // worker function
        void worker();

        std::condition_variable cv_work_arrive;
        std::condition_variable cv_work_done;
};

#endif
