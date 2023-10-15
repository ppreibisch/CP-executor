#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include "err.h"
#include "utils.h"

enum constants {
    MAX_COMMAND_LENGTH = 511,
    MAX_OUTPUT_LENGTH = 1022,
    MAX_TASKS_NUMBER = 4096
};

typedef struct Task {
    char currentOutput[MAX_OUTPUT_LENGTH];
    char currentError[MAX_OUTPUT_LENGTH];
    int outputs[2];
    char **args;
    int taskId;
    pid_t pid;
    pthread_mutex_t mutex;
} Task;


typedef struct RunPrintLock {
    pthread_cond_t runners, printers;
    pthread_mutex_t mutex;
    int waitForPrint;
    bool running;
} RunPrintLock;

RunPrintLock runPrintLock;

pthread_t threads[MAX_TASKS_NUMBER];
Task *tasks[MAX_TASKS_NUMBER];
int tasksNumber = 0;
sem_t runMutex;

void initBuffer(char **buffer) {
    *buffer = malloc(MAX_COMMAND_LENGTH * sizeof(char));
}

void initRunPrintLock() {
    ASSERT_ZERO(pthread_mutex_init(&runPrintLock.mutex, NULL));
    ASSERT_ZERO(pthread_cond_init(&runPrintLock.runners, NULL));
    ASSERT_ZERO(pthread_cond_init(&runPrintLock.printers, NULL));
    runPrintLock.waitForPrint = 0;
    runPrintLock.running = false;
}

void destroyRunPrintLock() {
    ASSERT_ZERO(pthread_mutex_destroy(&runPrintLock.mutex));
    ASSERT_ZERO(pthread_cond_destroy(&runPrintLock.runners));
    ASSERT_ZERO(pthread_cond_destroy(&runPrintLock.printers));
}

void freeTasks() {
    for (int taskId = 0; taskId < tasksNumber; ++taskId) {
        ASSERT_ZERO(pthread_mutex_destroy(&tasks[taskId]->mutex));
        free(tasks[taskId]);
    }
}

void joinThreads() {
    for (int threadId = 0; threadId < tasksNumber; ++threadId)
        ASSERT_ZERO(pthread_join(threads[threadId], NULL));
}

void *writeOutToBuffer(void *data) {
    Task *task = data;
    char *buffer;
    initBuffer(&buffer);

    FILE *file = fdopen(task->outputs[0], "r");
    while (read_line(buffer, MAX_OUTPUT_LENGTH, file)) {
        ASSERT_ZERO(pthread_mutex_lock(&task->mutex));
        size_t length = strlen(buffer);
        memcpy(task->currentOutput, buffer, length + 1);
        ASSERT_ZERO(pthread_mutex_unlock(&task->mutex));
    }
    ASSERT_SYS_OK(close(task->outputs[0]));
    free(buffer);
    return NULL;
}

void *writeErrToBuffer(void *data) {
    Task *task = data;
    char *buffer;
    initBuffer(&buffer);

    FILE *file = fdopen(task->outputs[1], "r");
    while (read_line(buffer, MAX_OUTPUT_LENGTH, file)) {
        ASSERT_ZERO(pthread_mutex_lock(&task->mutex));
        size_t length = strlen(buffer);
        memcpy(task->currentError, buffer, length + 1);
        ASSERT_ZERO(pthread_mutex_unlock(&task->mutex));
    }
    ASSERT_SYS_OK(close(task->outputs[1]));
    free(buffer);
    return NULL;
}

void *runTask(void *data) {
    Task *task = data;

    char *programName = task->args[1];
    char **programArguments = &task->args[1];

    int output_pipes[2][2];
    ASSERT_SYS_OK(pipe(output_pipes[0])); // output
    ASSERT_SYS_OK(pipe(output_pipes[1])); // error

    task->outputs[0] = output_pipes[0][0];
    task->outputs[1] = output_pipes[1][0];

    pid_t pid;
    ASSERT_SYS_OK(pid = fork());

    if (!pid) {
        for (int i = 0; i < 2; ++i) {
            ASSERT_SYS_OK(close(output_pipes[i][0]));
            ASSERT_SYS_OK(dup2(output_pipes[i][1], i + 1));
            ASSERT_SYS_OK(close(output_pipes[i][1]));
        }
        execvp(programName, programArguments);
    }

    ASSERT_SYS_OK(close(output_pipes[0][1]));
    ASSERT_SYS_OK(close(output_pipes[1][1]));
    set_close_on_exec(output_pipes[0][0], true);
    set_close_on_exec(output_pipes[1][0], true);

    task->pid = pid;

    pthread_t thread_out, thread_err;
    ASSERT_ZERO(pthread_create(&thread_out, NULL, writeOutToBuffer, task));
    ASSERT_ZERO(pthread_create(&thread_err, NULL, writeErrToBuffer, task));

    printf("Task %d started: pid %d.\n", task->taskId, pid);
    sem_post(&runMutex);

    ASSERT_ZERO(pthread_join(thread_out, NULL));
    ASSERT_ZERO(pthread_join(thread_err, NULL));

    ASSERT_SYS_OK(pthread_mutex_lock(&runPrintLock.mutex));
    ++runPrintLock.waitForPrint;
    while (runPrintLock.running)
        ASSERT_ZERO(pthread_cond_wait(&runPrintLock.printers, &runPrintLock.mutex));
    ASSERT_SYS_OK(pthread_mutex_unlock(&runPrintLock.mutex));

    int status;
    waitpid(pid, &status, 0);
    if (WIFSIGNALED(status)) {
        printf("Task %d ended: signalled.\n", task->taskId);
    } else {
        printf("Task %d ended: status %d.\n", task->taskId, WEXITSTATUS(status));
    }

    ASSERT_SYS_OK(pthread_mutex_lock(&runPrintLock.mutex));
    --runPrintLock.waitForPrint;
    if (runPrintLock.waitForPrint > 0)
        ASSERT_ZERO(pthread_cond_signal(&runPrintLock.printers));
    else
        ASSERT_ZERO(pthread_cond_signal(&runPrintLock.runners));
    ASSERT_SYS_OK(pthread_mutex_unlock(&runPrintLock.mutex));

    return NULL;
}

void run(char **args) {
    Task *task = malloc(sizeof(Task));
    task->args = args;
    task->taskId = tasksNumber;
    ASSERT_ZERO(pthread_mutex_init(&task->mutex, NULL));
    task->currentOutput[0] = '\0';
    task->currentError[0] = '\0';

    tasks[tasksNumber] = task;
    ASSERT_ZERO(pthread_create(&threads[tasksNumber], NULL, runTask, task));
    ++tasksNumber;
}

void printOutput(int taskId) {
    ASSERT_SYS_OK(pthread_mutex_lock(&tasks[taskId]->mutex));
    printf("Task %d stdout: '%s'.\n", taskId, tasks[taskId]->currentOutput);
    ASSERT_SYS_OK(pthread_mutex_unlock(&tasks[taskId]->mutex));
}

void printError(int taskId) {
    ASSERT_SYS_OK(pthread_mutex_lock(&tasks[taskId]->mutex));
    printf("Task %d stderr: '%s'.\n", taskId, tasks[taskId]->currentError);
    ASSERT_SYS_OK(pthread_mutex_unlock(&tasks[taskId]->mutex));
}

void quit() {
    for (int taskId = 0; taskId < tasksNumber; ++taskId)
        kill(tasks[taskId]->pid, SIGKILL);
}

void startCommand() {
    ASSERT_SYS_OK(pthread_mutex_lock(&runPrintLock.mutex));
    while (runPrintLock.waitForPrint > 0)
        ASSERT_ZERO(pthread_cond_wait(&runPrintLock.runners, &runPrintLock.mutex));
    runPrintLock.running = true;
    ASSERT_SYS_OK(pthread_mutex_unlock(&runPrintLock.mutex));
}

void endCommand() {
    ASSERT_SYS_OK(pthread_mutex_lock(&runPrintLock.mutex));
    runPrintLock.running = false;
    if (runPrintLock.waitForPrint > 0)
        ASSERT_ZERO(pthread_cond_signal(&runPrintLock.printers));
    ASSERT_SYS_OK(pthread_mutex_unlock(&runPrintLock.mutex));
}

int main() {
    char *buffer;
    initBuffer(&buffer);
    initRunPrintLock();
    ASSERT_ZERO(sem_init(&runMutex, 0, 0));

    while (read_line(buffer, MAX_COMMAND_LENGTH, stdin)) {
        startCommand();
        char **task = split_string(buffer);
        char *command = task[0];

        if (strcmp(command, "run") == 0) {
            run(task);
            sem_wait(&runMutex);
        } else if (strcmp(command, "out") == 0) {
            int taskId = atoi(task[1]);
            printOutput(taskId);
        } else if (strcmp(command, "err") == 0) {
            int taskId = atoi(task[1]);
            printError(taskId);
        } else if (strcmp(command, "kill") == 0) {
            int taskId = atoi(task[1]);
            kill(tasks[taskId]->pid, SIGINT);
        } else if (strcmp(command, "sleep") == 0) {
            int time = atoi(task[1]);
            usleep(time * 1000);
        } else if (strcmp(command, "quit") == 0) {
            quit();
            endCommand();
            free_split_string(task);
            break;
        } else if (strcmp(command, "") == 0) {
            endCommand();
            free_split_string(task);
            continue;
        }
        endCommand();
        free_split_string(task);
    }
    quit();
    joinThreads();
    freeTasks();
    destroyRunPrintLock();
    ASSERT_SYS_OK(sem_destroy(&runMutex));
    free(buffer);

    return 0;
}

