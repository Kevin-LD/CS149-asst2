#include "tasksys.h"
#include <thread>
#include <math.h>

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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

// static assignment
// void TaskSystemParallelSpawn::worker(IRunnable* runnable, int num_total_tasks, int start, int end) {
//     for (int i = start; i < end; i++) {
//         runnable->runTask(i, num_total_tasks);
//     }
// }

// dynamic assignment
// counter 表示 next task id
void TaskSystemParallelSpawn::worker(IRunnable* runnable, int num_total_tasks, std::atomic<int> *counter) {
    int cur_task;
    while ((cur_task = counter->fetch_add(1)) < num_total_tasks) {
        runnable->runTask(cur_task, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }


    // static assignment

    // std::thread *threads = new std::thread[num_threads];
    // int start;
    // int end;
    // int chunck_size = (num_total_tasks+num_threads-1) / num_threads;
    // for (int t = 0; t < num_threads; t++) {
    //     start = t*chunck_size;
    //     end = std::min(start + chunck_size, num_total_tasks);
    //     threads[t] = std::thread(&TaskSystemParallelSpawn::worker, this, runnable, num_total_tasks, start, end);
    // }

    // // 加上主线程实际上由 num_threads + 1 个线程，但是主线程没做计算，大体上不会因为 context switch 损失太多性能
    // // 其实也可以让主线程做一个 chunck 的任务，但 in the spirit of doing the simplest thing first，我们先不考虑这种实现

    // for (int t = 0; t < num_threads; t++) {
    //     threads[t].join();
    // }
    
    // delete[] threads;


    // dynamic assignment
    std::atomic<int> counter(0);

    // 加上主线程一共 num_threads 个
    std::thread *threads = new std::thread[num_threads-1];

    for (int t = 0; t < num_threads-1; t++) {
        threads[t] = std::thread(&TaskSystemParallelSpawn::worker, this, runnable, num_total_tasks, &counter);
    }
    worker(runnable, num_total_tasks, &counter);

    for (int t = 0; t < num_threads-1; t++) {
        threads[t].join();
    }

    delete[] threads;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads(num_threads), thread_pool_survive(true), thread_pool(new std::thread[num_threads-1]), counter(0), num_total_tasks(0), num_tasks_finished(0), cur_bulk_runnable(nullptr) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    for (int t = 0; t < num_threads-1; t++) {
        thread_pool[t] = std::thread(&TaskSystemParallelThreadPoolSpinning::worker, this, false); 
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    thread_pool_survive = false;
    for (int t = 0; t < num_threads-1; t++) {
        thread_pool[t].join();
    }
    delete[] thread_pool;
}

void TaskSystemParallelThreadPoolSpinning::worker(bool is_main) {
    bool assigned_work = false;
    int task_id;
    std::unique_lock<std::mutex> counter_num_total_tasks_lock(counter_num_total_tasks_mtx, std::defer_lock);
    std::unique_lock<std::mutex> num_tasks_finished_lock(num_tasks_finished_mtx, std::defer_lock);
    while (thread_pool_survive) {
        counter_num_total_tasks_lock.lock();
        if (counter < num_total_tasks) {
            task_id = counter;
            counter++;
            assigned_work = true;
        }
        counter_num_total_tasks_lock.unlock();

        if (assigned_work) {
            // 在 assined_work == true 时，runnable 一定已经更新了
            cur_bulk_runnable->runTask(task_id, num_total_tasks);
            assigned_work = false;
            num_tasks_finished_lock.lock();
            num_tasks_finished++;
            num_tasks_finished_lock.unlock();
        }

        if (is_main) {
            num_tasks_finished_lock.lock();
            if (num_tasks_finished == num_total_tasks) {
                // unique_lock unlock automatically
                return;
            }
            num_tasks_finished_lock.unlock();
        }
    }
}


void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    std::unique_lock<std::mutex> counter_num_total_tasks_lock(counter_num_total_tasks_mtx, std::defer_lock);
    std::unique_lock<std::mutex> num_tasks_finished_lock(num_tasks_finished_mtx, std::defer_lock);

    std::lock(counter_num_total_tasks_lock, num_tasks_finished_lock);

    counter = 0;
    this->num_total_tasks = num_total_tasks;
    num_tasks_finished = 0;
    cur_bulk_runnable = runnable;

    counter_num_total_tasks_lock.unlock();
    num_tasks_finished_lock.unlock();

    worker(true);
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), num_threads(num_threads), thread_pool_survive(true), thread_pool(new std::thread[num_threads]), counter(0), num_total_tasks(0), num_tasks_finished(0), cur_bulk_runnable(nullptr) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int t = 0; t < num_threads; t++) {
        thread_pool[t] = std::thread(&TaskSystemParallelThreadPoolSleeping::worker, this); 
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    thread_pool_survive = false;
    cv_work_arrive.notify_all();
    for (int t = 0; t < num_threads; t++) {
        thread_pool[t].join();
    }
    delete[] thread_pool;
}

void TaskSystemParallelThreadPoolSleeping::worker() {
    int task_id;
    std::unique_lock<std::mutex> lock(mtx);
    while (true) {
        // wait until work arrives, or thread pool destruction
        cv_work_arrive.wait(lock, [=] { return counter < num_total_tasks || !thread_pool_survive; });
        if (!thread_pool_survive) {
            return;
        }
        task_id = counter;
        counter++;
        lock.unlock();

        cur_bulk_runnable->runTask(task_id, num_total_tasks);
        lock.lock();
        num_tasks_finished++;
        if (num_tasks_finished == num_total_tasks) {
            cv_work_done.notify_one();
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
    std::unique_lock<std::mutex> lock(mtx);
    counter = 0;
    this->num_total_tasks = num_total_tasks;
    num_tasks_finished = 0;
    cur_bulk_runnable = runnable;

    lock.unlock();

    cv_work_arrive.notify_all();

    lock.lock();
    cv_work_done.wait(lock, [=] { return num_tasks_finished == num_total_tasks; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
