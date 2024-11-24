#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <errno.h>
#include <math.h>

#define BROADCAST_PORT 9000
#define BROADCAST_MSG "DISCOVER"
#define BUFFER_SIZE 1024
#define MAX_WORKERS 3
#define MAX_TASKS 10000
#define TIMEOUT_SEC 5


typedef struct {
    int socket;
    struct sockaddr_in address;
    int busy;       // 0 - доступен, 1 - выполняет задание, -1 - недоступен
    double start;   // Начало текущего диапазона
    double end;     // Конец текущего диапазона
} Worker;

typedef struct {
    double start;
    double end;
    double result;
    int completed;  // 0 - не выполнено, 1 - выполнено
    int busy; // 0 - не занят, 1 - занят
} Task;

Worker workers[MAX_WORKERS];
Task tasks[MAX_TASKS];
int worker_count = 0;
int task_count = MAX_TASKS;
int completed_tasks = 0;
int worker_ports[MAX_WORKERS];

// Широковещательная рассылка для поиска рабочих узлов
void broadcast_discovery(int udp_socket, struct sockaddr_in *broadcast_addr) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, BUFFER_SIZE, "%s", BROADCAST_MSG);

    if (sendto(udp_socket, buffer, strlen(buffer), 0,
               (struct sockaddr *)broadcast_addr, sizeof(*broadcast_addr)) < 0) {
        perror("Broadcast send failed");
        exit(EXIT_FAILURE);
    }

    printf("Broadcast message sent.\n");
}

// Прием откликов от рабочих узлов
void accept_workers(int udp_socket) {
    struct sockaddr_in worker_addr;
    socklen_t addr_len = sizeof(worker_addr);
    char buffer[BUFFER_SIZE];

    while (1) {
        int bytes = recvfrom(udp_socket, buffer, BUFFER_SIZE, MSG_DONTWAIT,
                             (struct sockaddr *)&worker_addr, &addr_len);
        if (bytes > 0) {
            buffer[bytes] = '\0';

            int tcp_port = 0;
            if (sscanf(buffer, "WORKER_AVAILABLE %d", &tcp_port) == 1) {
                printf("Worker reports TCP port: %d\n", tcp_port);
                worker_ports[worker_count] = tcp_port;

                if (worker_count < MAX_WORKERS) {
                    workers[worker_count].socket = socket(AF_INET, SOCK_STREAM, 0);
                    if (workers[worker_count].socket < 0) {
                        perror("Worker socket creation failed");
                        continue;
                    }

                    worker_addr.sin_port = htons(tcp_port);

                    if (connect(workers[worker_count].socket,
                                (struct sockaddr *)&worker_addr, sizeof(worker_addr)) < 0) {
                        perror("Connection to worker failed");
                        close(workers[worker_count].socket);
                        continue;
                    }

                    workers[worker_count].address = worker_addr;
                    workers[worker_count].busy = 0;
                    worker_count++;
                    printf("Connected to worker %d on port %d\n", worker_count - 1, tcp_port);
                } else {
                    printf("Maximum worker limit reached, ignoring new workers.\n");
                }
            } else {
                fprintf(stderr, "Invalid response from worker: %s\n", buffer);
            }
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break; // Нет новых ответов
            } else {
                perror("Receive error");
            }
        }
    }
}

// Функция для пересылки задания узлу
int send_task(Worker *worker, double start, double end, int task_index) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, BUFFER_SIZE, "%.6f %.6f", start, end);
    if (send(worker->socket, buffer, strlen(buffer), 0) < 0) {
        perror("Task send failed");
        worker->busy = -1;
        return -1;
    } else {
        printf("Sending task [%.6f, %.6f] to worker %d on port %d\n", start, end, (int)(worker - workers), ntohs(worker->address.sin_port));
    }
    worker->busy = 1;
    worker->start = start;
    worker->end = end;
    return 0;
}

// Функция для перераспределения задач
void redistribute_tasks() {
    for (int i = 0; i < task_count; ++i) {
        if ((tasks[i].completed != 1) && (tasks[i].busy == 1)) {
            for (int j = 0; j < worker_count; ++j) {
                if (workers[j].busy == 0) {
                    if (send_task(&workers[j], tasks[i].start, tasks[i].end, i) == 0) {
                        break;
                    }
                }
            }
        }
    }
}

// Функция для сбора результатов
void collect_results(fd_set *read_fds) {
    char buffer[BUFFER_SIZE];
    double total_result = 0;

    for (int i = 0; i < worker_count; ++i) {
        if (strstr(buffer, "PONG") != NULL) {
            printf("Worker %d sent PONG, marking as not busy\n", i);
            workers[i].busy = 0; 
            continue; 
        } else if (FD_ISSET(workers[i].socket, read_fds)) {
            int bytes = recv(workers[i].socket, buffer, BUFFER_SIZE, 0);
            if (bytes > 0) {
                buffer[bytes] = '\0';
                double partial_result = atof(buffer);
                printf("Worker %d completed task: %.6f\n", i, partial_result);
                total_result += partial_result;

                for (int t = 0; t < task_count; ++t) {
                    if (tasks[t].start == workers[i].start && tasks[t].end == workers[i].end) {
                        if (tasks[t].completed == 0) {
                            tasks[t].result = partial_result;
                            tasks[t].completed = 1; 
                            completed_tasks++;
                        }
                        break;
                    }
                }

                workers[i].busy = 0;
            } else if (bytes == 0) {
                printf("Worker %d closed the connection\n", i);
                workers[i].busy = -1;
            } else {
                perror("Receive failed");
                workers[i].busy = -1;
            }
        }
    }
}

void ping_worker(Worker *worker) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, BUFFER_SIZE, "PING");

    // Отправка пинга
    if (send(worker->socket, buffer, strlen(buffer), 0) < 0) {
        perror("Ping send failed");
        worker->busy = -1;
        return;
    }
}

// Основная обработка задач
void process_tasks(double start, double end) {
    double step = (end - start) / task_count;

    // Инициализация задач
    for (int i = 0; i < task_count; ++i) {
        tasks[i].start = start + i * step;
        tasks[i].end = tasks[i].start + step;
        tasks[i].completed = 0;
        tasks[i].busy = 0;
        tasks[i].result = 0.0;
    }

    int next_task = 0; // Индекс следующей задачи для отправки

    while (completed_tasks < task_count) {
        fd_set read_fds;
        FD_ZERO(&read_fds);

        int max_fd = 0;

        // Добавляем в набор файловых дескрипторов сокеты воркеров
        for (int i = 0; i < worker_count; ++i) {
            FD_SET(workers[i].socket, &read_fds);
            if (workers[i].socket > max_fd) {
                max_fd = workers[i].socket;
            }
        }

        if (next_task != 0) {
            struct timeval timeout = {TIMEOUT_SEC, 0};
            int activity = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);

            if (activity < 0) {
                perror("Select error");
                break;
            } else if (activity == 0) {
                printf("Timeout reached, redistributing tasks...\n");
                printf("Worker states:\n");
                for (int i = 0; i < worker_count; ++i) {
                    printf("Worker %d: socket=%d, busy=%d\n", i, workers[i].socket, workers[i].busy);
                }

                for (int i = 0; i < worker_count; ++i) {
                    if (workers[i].busy == 1) {
                        workers[i].busy = -1;
                    }
                }

                for (int i = 0; i < worker_count; ++i) {
                    if (workers[i].busy == -1) {
                        ping_worker(&workers[i]);
                    }
                }

                redistribute_tasks();
            } else {
                for (int i = 0; i < worker_count; ++i) {
                    if (workers[i].busy == -1) {
                        ping_worker(&workers[i]);
                    }
                }
                
                collect_results(&read_fds);
            }
        }

        // Проверяем, есть ли свободные воркеры для выполнения оставшихся задач
        for (int i = 0; i < worker_count && next_task < task_count; ++i) {
            if (workers[i].busy == 0) {
                if (tasks[next_task].busy == 0) {
                    send_task(&workers[i], tasks[next_task].start, tasks[next_task].end, next_task);
                }
                next_task++;
            }
        }
    }

    // Подсчёт итогового результата
    double total_result = 0.0;
    for (int i = 0; i < task_count; ++i) {
        // printf("Task result %f, task id %d, task completed %d\n", tasks[i].result, i, tasks[i].completed);
        total_result += tasks[i].result;
    }

    printf("Task completed: %d\n", completed_tasks);

    printf("Final result: %.6f\n", total_result);
}

int main() {
    int udp_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_socket < 0) {
        perror("UDP socket creation failed");
        exit(EXIT_FAILURE);
    }

    int broadcast_enable = 1;
    setsockopt(udp_socket, SOL_SOCKET, SO_BROADCAST, &broadcast_enable, sizeof(broadcast_enable));

    struct sockaddr_in broadcast_addr;
    memset(&broadcast_addr, 0, sizeof(broadcast_addr));
    broadcast_addr.sin_family = AF_INET;
    broadcast_addr.sin_port = htons(BROADCAST_PORT);
    broadcast_addr.sin_addr.s_addr = htonl(INADDR_BROADCAST);

    broadcast_discovery(udp_socket, &broadcast_addr);
    sleep(2); // Подождать отклики

    accept_workers(udp_socket);
    printf("Discovered workers:\n");
    for (int i = 0; i < worker_count; ++i) {
        printf("Worker %d: IP %s, Port %d\n", i,
            inet_ntoa(workers[i].address.sin_addr), worker_ports[i]);
    }
    close(udp_socket);

    if (worker_count == 0) {
        printf("No workers found. Exiting.\n");
        exit(EXIT_FAILURE);
    }

    // Обработка задач
    double start = 0, end = M_PI;
    process_tasks(start, end);

    // Завершение работы
    for (int i = 0; i < worker_count; ++i) {
        close(workers[i].socket);
    }

    return 0;
}

