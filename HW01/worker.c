#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <math.h>
#include <errno.h>

#define BROADCAST_PORT 9000
#define BUFFER_SIZE 1024


// Функция для вычисления интеграла от sin(x) методом левых прямоугольников
double calculate_integral(double start, double end) {
    double steps = 1000;
    double step = (end - start) / steps;
    double result = 0;
    for (int i = 0; i < steps; ++i) {
        double x = start + i * step;
        result += sin(x) * step;
    }
    return result;
}

// Функция для ответа на широковещательное сообщение мастера
void respond_to_master(int udp_socket, struct sockaddr_in *master_addr, int tcp_port) {
    char response[BUFFER_SIZE];
    snprintf(response, BUFFER_SIZE, "WORKER_AVAILABLE %d", tcp_port);

    if (sendto(udp_socket, response, strlen(response), 0,
               (struct sockaddr *)master_addr, sizeof(*master_addr)) < 0) {
        perror("Failed to send response to master");
        exit(EXIT_FAILURE);
    }

    printf("Response sent to master at %s:%d\n",
           inet_ntoa(master_addr->sin_addr), ntohs(master_addr->sin_port));
}

// Функция обработки задачи в отдельном потоке
void *calculate_and_respond(void *arg) {
    int client_socket = *(int *)arg;
    free(arg);

    char buffer[BUFFER_SIZE];
    while (1) {
        int bytes = recv(client_socket, buffer, BUFFER_SIZE - 1, 0);
        if (bytes <= 0) {
            if (bytes == 0) {
                printf("Master closed the connection.\n");
            } else {
                perror("Failed to receive data");
            }
            break;
        }

        buffer[bytes] = '\0';

        if (strstr(buffer, "PING") != NULL) {
            printf("PING received, sending PONG\n");
            if (send(client_socket, "PONG", 4, 0) < 0) {
                perror("Failed to send PONG");
            }
            continue;
        }

        double start, end;
        if (sscanf(buffer, "%lf %lf", &start, &end) != 2) {
            fprintf(stderr, "Invalid task format received: %s\n", buffer);
            continue;
        }

        printf("Task received: [%.6f, %.6f]\n", start, end);

        // Вычисляем интеграл
        double result = calculate_integral(start, end);

        // Отправляем результат мастеру
        snprintf(buffer, BUFFER_SIZE, "%.6f", result);
        if (send(client_socket, buffer, strlen(buffer), 0) < 0) {
            perror("Failed to send result to master");
            break;
        }

        printf("Result sent: %.6f\n", result);
    }

    close(client_socket); // Закрываем клиентский сокет после завершения работы
    return NULL;
}


int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <tcp_port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int tcp_port = atoi(argv[1]);
    if (tcp_port <= 0 || tcp_port > 65535) {
        fprintf(stderr, "Invalid port number: %d\n", tcp_port);
        exit(EXIT_FAILURE);
    }

    // Создаем UDP сокет для приема широковещательных сообщений
    int udp_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_socket < 0) {
        perror("UDP socket creation failed");
        exit(EXIT_FAILURE);
    }

    int reuse = 1;
    setsockopt(udp_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in broadcast_addr;
    memset(&broadcast_addr, 0, sizeof(broadcast_addr));
    broadcast_addr.sin_family = AF_INET;
    broadcast_addr.sin_port = htons(BROADCAST_PORT);
    broadcast_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(udp_socket, (struct sockaddr *)&broadcast_addr, sizeof(broadcast_addr)) < 0) {
        perror("Binding UDP socket failed");
        close(udp_socket);
        exit(EXIT_FAILURE);
    }

    printf("Worker is listening for broadcast messages on port %d\n", BROADCAST_PORT);

    // Создаем TCP сокет для приема задач
    int tcp_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_socket < 0) {
        perror("TCP socket creation failed");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in tcp_addr;
    memset(&tcp_addr, 0, sizeof(tcp_addr));
    tcp_addr.sin_family = AF_INET;
    tcp_addr.sin_port = htons(tcp_port);
    tcp_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(tcp_socket, (struct sockaddr *)&tcp_addr, sizeof(tcp_addr)) < 0) {
        perror("TCP bind failed");
        close(tcp_socket);
        exit(EXIT_FAILURE);
    }

    if (listen(tcp_socket, 10) < 0) {
        perror("TCP listen failed");
        close(tcp_socket);
        exit(EXIT_FAILURE);
    }

    printf("Worker is ready to receive tasks on port %d\n", tcp_port);

    while (1) {
        struct sockaddr_in master_addr;
        socklen_t addr_len = sizeof(master_addr);
        char buffer[BUFFER_SIZE];

        // Обработка UDP сообщений
        int bytes = recvfrom(udp_socket, buffer, BUFFER_SIZE - 1, 0,
                             (struct sockaddr *)&master_addr, &addr_len);
        if (bytes > 0) {
            buffer[bytes] = '\0';
            if (strcmp(buffer, "DISCOVER") == 0) {
                printf("Discover message received from master.\n");
                respond_to_master(udp_socket, &master_addr, tcp_port);
            }
        }

        // Принимаем задачу через TCP
        int client_socket = accept(tcp_socket, NULL, NULL);
        if (client_socket < 0) {
            perror("Accept failed");
            continue;
        }

        pthread_t task_thread;
        int *client_socket_ptr = malloc(sizeof(int));
        *client_socket_ptr = client_socket;
        if (pthread_create(&task_thread, NULL, calculate_and_respond, client_socket_ptr) != 0) {
            perror("Failed to create task thread");
            close(client_socket);
        }
    }

    close(udp_socket); // Закрытие UDP сокета
    close(tcp_socket); // Закрытие TCP сокета
    return 0;
}
