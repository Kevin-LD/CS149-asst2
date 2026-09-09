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
}

// return number of deps left unresolved in deps
int TaskSystemParallelThreadPoolSleeping::check_deps(const std::vector<TaskID>& deps) {
    int num_unresolved_deps = deps.size();
    for (auto iter = deps.begin(); iter != deps.end(); iter++) {
        if (finished_launch.count(*iter)) {
            num_unresolved_deps--;
        }
    }
    return num_unresolved_deps;
}

void TaskSystemParallelThreadPoolSleeping::worker() {
    while (true) {
        std::unique_lock<std::mutex> lock(mtx);
        cv_ready.wait(lock, [this] { return ready_queue.size() != 0 || !thread_pool_survive; });
        if (!thread_pool_survive) {
            return;
        }
        bulkLaunch* cur_bl = ready_queue.front();
        int cur_task_id = cur_bl->next_task;
        cur_bl->next_task++;
        if (cur_bl->next_task == cur_bl->num_total_tasks) {
            ready_queue.pop();
            waiting_to_finish++;
        }
        lock.unlock();
        cur_bl->runnable->runTask(cur_task_id, cur_bl->num_total_tasks);
        lock.lock();
        cur_bl->num_tasks_finished++;
        if (cur_bl->num_tasks_finished == cur_bl->num_total_tasks) {
            waiting_to_finish--;
            TaskID finished_launch_id = cur_bl->lauch_id;
            finished_launch.insert(finished_launch_id);
            if (finished_launch_id < (TaskID)dep_graph.size()) {
                for (auto iter = dep_graph[finished_launch_id].begin(); iter != dep_graph[finished_launch_id].end(); iter++) {
                    bulkLaunch* notified_launch = all_launched[*iter];
                    notified_launch->num_unresolved_deps--;
                    if (notified_launch->num_unresolved_deps == 0) {
                        waiting_launch.erase(notified_launch);
                        ready_queue.push(notified_launch);
                    }
                }
            }
            delete cur_bl;
            if (waiting_to_finish == 0 && waiting_launch.size() == 0 && ready_queue.size() == 0) {
                cv_done.notify_one();
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

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    
    std::lock_guard<std::mutex> lock(mtx);
    int num_unresolved_deps = check_deps(deps);
    bulkLaunch* bl = new bulkLaunch(next_bulk_launch_id, runnable, num_total_tasks, num_unresolved_deps);
    next_bulk_launch_id++;
    
    TaskID end = bl->lauch_id;
    for (auto iter = deps.begin(); iter != deps.end(); iter++) {
        TaskID start = *iter;
        // 虽然测试里好像没有，但也支持对后来 launch 的 dep
        TaskID max_vertex = std::max(start, end);
        if (max_vertex >= (TaskID)dep_graph.size()) {
            dep_graph.resize(max_vertex + 1);
        }
        dep_graph[start].push_back(end);
    }

    all_launched.push_back(bl);

    if (bl->num_unresolved_deps == 0) {
        ready_queue.push(bl);
        if (ready_queue.size() == 1) {
            cv_ready.notify_all();
        }
        return bl->lauch_id;
    }
    waiting_launch.insert(bl);
    return bl->lauch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    std::unique_lock<std::mutex> lock(mtx);
    cv_done.wait(lock, [this] { return waiting_to_finish == 0 && waiting_launch.size() == 0 && ready_queue.size() == 0; });
    return;
}
