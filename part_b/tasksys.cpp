#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), num_threads(num_threads), waiting_to_finish(0), thread_pool_survive(true), thread_pool(new std::thread[num_threads]), next_bulk_launch_id(0) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int t = 0; t < num_threads; t++) {
        thread_pool[t] = std::thread(&TaskSystemParallelThreadPoolSleeping::worker, this); 
    }
    all_launched.reserve(4000);
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    thread_pool_survive = false;
    cv_ready.notify_all();
    for (int t = 0; t < num_threads; t++) {
        thread_pool[t].join();
    }
    delete[] thread_pool;
    for (bulkLaunch* bl : all_launched) {
        delete bl;
    }
}


void TaskSystemParallelThreadPoolSleeping::worker() {
    while (true) {
        bool was_empty = false;
        std::unique_lock<std::mutex> lock(mtx);
        cv_ready.wait(lock, [this] { return ready_queue.size() != 0 || !thread_pool_survive; });
        if (!thread_pool_survive) {
            return;
        }
        bulkLaunch* cur_bl = ready_queue.front();
        lock.unlock();
        int cur_task_id = cur_bl->next_task.fetch_add(1);
        if (cur_task_id >= cur_bl->num_total_tasks) {
            continue;
        }
        if (cur_task_id == cur_bl->num_total_tasks - 1) {
            lock.lock();
            ready_queue.pop();
            waiting_to_finish++;
            lock.unlock();
        }
        cur_bl->runnable->runTask(cur_task_id, cur_bl->num_total_tasks);
        
        int num_finished = cur_bl->num_tasks_finished.fetch_add(1)+1;

        if (num_finished == cur_bl->num_total_tasks) {
            lock.lock();
            waiting_to_finish--;
            cur_bl->finished = true;
            for (auto notified_launch : cur_bl->dependents) {
                notified_launch->num_unresolved_deps--;
                if (notified_launch->num_unresolved_deps == 0) {
                    ready_queue.push(notified_launch);
                    if (ready_queue.size() == 1) {
                        was_empty = true;
                    }
                }
            }
            if (waiting_to_finish == 0 && ready_queue.size() == 0) {
                cv_done.notify_one();
            }
            lock.unlock();
            if (was_empty) {
                // 这里采取锁外提交，可以避免唤醒线程和持锁线程的 contention。
                cv_ready.notify_all();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    std::vector<TaskID> noDeps;
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();

    // Rm. 一定需要主线程 run() 和 runAsyncWithDeps() 不混用的假设才可以这样实现 run()
    // 因为 run() 的语义是完成当前 bulk 就返回，但是 sync() 会等待所有之前的 runAsyncWithDeps()，不止当前的 bulk。
    // 例如 runAsyncWithDeps(A, ...); run(B, ...); 这样 run() 会在 A, B 都完成时返回，而不是我们希望的，在 B 完成时就返回。

}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    
    
    bulkLaunch* bl = new bulkLaunch(next_bulk_launch_id, runnable, num_total_tasks, 0);
    next_bulk_launch_id++;

    std::lock_guard<std::mutex> lock(mtx);
    for (TaskID id : deps) {
        bulkLaunch* predecessor = all_launched[id];
        if (!predecessor->finished) {
            bl->num_unresolved_deps++;
            predecessor->dependents.push_back(bl);
        }
    }

    all_launched.push_back(bl);

    if (bl->num_unresolved_deps == 0) {
        ready_queue.push(bl);
        if (ready_queue.size() == 1) {
            // 这里采用锁内 notify，是因为锁内 notify 有利于 main thread 再次得到锁，
            // 可以一次提交大量任务到 all_launch 中，猜想这会更加内存友好。
            // 实验发现，如果改成锁外提交会 ！大幅！ 降低速度。
            cv_ready.notify_all();
        }
        return bl->lauch_id;
    }
    return bl->lauch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    std::unique_lock<std::mutex> lock(mtx);
    cv_done.wait(lock, [this] { return waiting_to_finish == 0 && ready_queue.size() == 0; });
    return;
}
