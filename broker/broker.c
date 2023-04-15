#include <stdio.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>
#include <sys/uio.h>
#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>

#include "comun.h"
#include "map.h"
#include "queue.h"

//---------------------- Estructuras & Variables ----------------------

typedef struct topic {
    char *name;
    queue *messages;
} topic;

typedef struct message {
    int size;
    void *msg;
} message;

map *topics;
int err = 0;
char *offset_folder;


//------------------------ Aplicaciones del API -----------------------

void create_topic(int socket){
    // luego llega el string, que viene precedido por su longitud
    int topic_longitud;
    if (recv(socket, &topic_longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    topic_longitud = ntohl(topic_longitud);
    char *topic_name = malloc(topic_longitud+1); // +1 para el carácter nulo
    // ahora sí llega el string
    if (recv(socket, topic_name, topic_longitud, MSG_WAITALL)!=topic_longitud)
        return;
    topic_name[topic_longitud]='\0';       // añadimos el carácter nulo
    printf("Recibido string: %s\n", topic_name);

    topic *t = malloc(sizeof(topic));
    t->name = malloc(strlen(topic_name)+1);
    strcpy(t-> name, topic_name);
    t->messages=queue_create(0); // sin cerrojos

    int res = map_put(topics, t->name, t);

    free(topic_name);

    // envía un entero como respuesta
    res = htonl(res);
    write(socket, &res, sizeof(int)); 

}

void ntopics(int socket){
    int res = htonl(map_size(topics));
    // envía un entero como respuesta
    write(socket, &res, sizeof(int));
}

void send_msg(int socket){
    // luego llega el string, que viene precedido por su longitud
    int topic_longitud;
    if (recv(socket, &topic_longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    topic_longitud = ntohl(topic_longitud);
    char *topic_name = malloc(topic_longitud+1); // +1 para el carácter nulo
    // ahora sí llega el string
    if (recv(socket, topic_name, topic_longitud, MSG_WAITALL)!=topic_longitud)
        return;
    topic_name[topic_longitud]='\0';       // añadimos el carácter nulo
    printf("Recibido string: %s\n", topic_name);

    // luego llega el mensaje, que viene precedido por su longitud
    int msg_size;
    if (recv(socket, &msg_size, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    msg_size = ntohl(msg_size);

    void *msg = malloc(msg_size);
    // ahora sí llega el mensaje
    if (recv(socket, msg, msg_size, MSG_WAITALL)!=msg_size)
        return;

    int res;
    topic *t = map_get(topics, topic_name, &err);
    if (err != -1){
        if (msg_size==0) return; // o que retorna????????????
        message *m = malloc(sizeof(message));
        m->size=msg_size;
        m->msg=msg;
        res = queue_append(t->messages, m);
    } else
        res = -1;

    free(topic_name);

    // envía un entero como respuesta
    res = htonl(res);
    write(socket, &res, sizeof(int)); 

}

void msg_length(int socket){
    // luego llega el string, que viene precedido por su longitud
    int topic_longitud;
    if (recv(socket, &topic_longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    topic_longitud = ntohl(topic_longitud);
    char *topic_name = malloc(topic_longitud+1); // +1 para el carácter nulo
    // ahora sí llega el string
    if (recv(socket, topic_name, topic_longitud, MSG_WAITALL)!=topic_longitud)
        return;
    topic_name[topic_longitud]='\0';       // añadimos el carácter nulo
    printf("Recibido string: %s\n", topic_name);
    // ahora llega el offset
    int offset;
    if (recv(socket, &offset, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    offset = ntohl(offset);

    int res;
    topic *t = map_get(topics, topic_name, &err);
    if (err != -1){
        message *m = queue_get(t->messages, offset, &err);
        if (err != -1)
            res = m->size;
        else
            res = 0;
    } else 
        res = -1;

    free(topic_name);

    // envía un entero como respuesta
    res = htonl(res);
    write(socket, &res, sizeof(int));
}

void end_offset(int socket){
    // luego llega el string, que viene precedido por su longitud
    int topic_longitud;
    if (recv(socket, &topic_longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    topic_longitud = ntohl(topic_longitud);
    char *topic_name = malloc(topic_longitud+1); // +1 para el carácter nulo
    // ahora sí llega el string
    if (recv(socket, topic_name, topic_longitud, MSG_WAITALL)!=topic_longitud)
        return;
    topic_name[topic_longitud]='\0';       // añadimos el carácter nulo
    printf("Recibido string: %s\n", topic_name);

    int res;
    topic *t = map_get(topics, topic_name, &err);
    if (err != -1) res = queue_size(t->messages);
    else res = -1;

    free(topic_name);

    // envía un entero como respuesta
    res = htonl(res);
    write(socket, &res, sizeof(int));
}

void poll(int socket){
    // luego llega el string, que viene precedido por su longitud
    int topic_longitud;
    if (recv(socket, &topic_longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    topic_longitud = ntohl(topic_longitud);
    char * topic_name = malloc(topic_longitud+1); // +1 para el carácter nulo
    // ahora sí llega el string
    if (recv(socket, topic_name, topic_longitud, MSG_WAITALL)!=topic_longitud)
        return;
    topic_name[topic_longitud]='\0';       // añadimos el carácter nulo
    printf("Recibido nome do tema: %s\n", topic_name);
    // ahora llega el offset
    int offset;
    if (recv(socket, &offset, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    offset = ntohl(offset);
    
    int res;
    message *m = NULL;
    topic *t = map_get(topics, topic_name, &err);
    if(err!=-1){
        m = queue_get(t->messages, offset, &err);
        if(err!=-1){
            printf("aaaa\n");
            struct iovec iov[2]; // hay que enviar 3 elementos
            int nelem = 0;

            // preparo el envío del mensaje mandando antes su longitud
            int longitud_str = m->size; // no incluye el carácter nulo
            int longitud_str_net = htonl(longitud_str);
            iov[nelem].iov_base=&longitud_str_net;
            iov[nelem++].iov_len=sizeof(int);
            iov[nelem].iov_base=m->msg; // no se usa & porque ya es un puntero
            iov[nelem++].iov_len=longitud_str;
            writev(socket, iov, 2);
            return;
        } else{
            res = htonl(0);
            printf("offset %d", offset);
        }
    } else 
        res = htonl(-1);

    free(topic_name);

    // envía un entero como respuesta
    write(socket, &res, sizeof(int));
}

void commit(int socket){
    int client_longitud;
    // luego llega el string, que viene precedido por su longitud
    if (recv(socket, &client_longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    client_longitud = ntohl(client_longitud);
    char *client = malloc(client_longitud+1); // +1 para el carácter nulo
    // ahora sí llega el string
    if (recv(socket, client, client_longitud, MSG_WAITALL)!=client_longitud)
        return;
    client[client_longitud]='\0';       // añadimos el carácter nulo
    printf("Recibido string: %s\n", client);

    int topic_longitud;
    // luego llega el string, que viene precedido por su longitud
    if (recv(socket, &topic_longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    topic_longitud = ntohl(topic_longitud);
    char *topic_name = malloc(topic_longitud+1); // +1 para el carácter nulo
    // ahora sí llega el string
    if (recv(socket, topic_name, topic_longitud, MSG_WAITALL)!=topic_longitud)
        return;
    topic_name[topic_longitud]='\0';       // añadimos el carácter nulo
    printf("Recibido string: %s\n", topic_name);

    int offset;
    if (recv(socket, &offset, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    offset = ntohl(offset);

    // crear directorio si no existe
    char dir_name[strlen(offset_folder) + strlen(client) + 1];
    sprintf(dir_name, "%s/%s", offset_folder, client);

    DIR* dir = opendir(dir_name);
    if(dir){
        closedir(dir);
    } else if(ENOENT == errno){
        mkdir(dir_name, 0777);
    } else{
        return;
    }

    // crear fichero
    char filename[strlen(dir_name)+strlen(topic_name)+1];
    sprintf(filename, "%s/%s.txt", dir_name, topic_name);

    FILE* fp = fopen(filename, "w");
    fprintf(fp, "%d\n", offset);
    fclose(fp);

    free(client);
    free(topic_name);

    // envía un entero como respuesta
    int res = htonl(0);
    write(socket, &res, sizeof(int));
}

void commited(int socket){

    int client_longitud;
    // luego llega el string, que viene precedido por su longitud
    if (recv(socket, &client_longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    client_longitud = ntohl(client_longitud);
    char *client = malloc(client_longitud+1); // +1 para el carácter nulo
    // ahora sí llega el string
    if (recv(socket, client, client_longitud, MSG_WAITALL)!=client_longitud)
        return;
    client[client_longitud]='\0';       // añadimos el carácter nulo
    printf("Recibido string: %s\n", client);

    int topic_longitud;
    // luego llega el string, que viene precedido por su longitud
    if (recv(socket, &topic_longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return;
    topic_longitud = ntohl(topic_longitud);
    char *topic_name = malloc(topic_longitud+1); // +1 para el carácter nulo
    // ahora sí llega el string
    if (recv(socket, topic_name, topic_longitud, MSG_WAITALL)!=topic_longitud)
        return;
    topic_name[topic_longitud]='\0';       // añadimos el carácter nulo
    printf("Recibido string: %s\n", topic_name);

    //char dir_name[strlen(offset_folder) + strlen(id_cliente) + 1];
    //sprintf(dir_name, "%s/%s", offset_folder, id_cliente);

    //DIR* dir = opendir(dir_name);
    //if(dir){
    //    closedir(dir);
    //} else{
    //    return -1;
    //}

    char filename[strlen(offset_folder) + strlen(client) + strlen(topic_name)+1];
    sprintf(filename, "%s/%s/%s.txt", offset_folder, client, topic_name);
    int offset;

    FILE* fp = fopen(filename, "r");
    if(fp != NULL){
        fscanf(fp, "%d", &offset);
        fclose(fp);
    } else
        offset = -1;

    free(client);
    free(topic_name);

    // envía un entero como respuesta
    offset = htonl(offset);
    write(socket, &offset, sizeof(int));
}




//------------------------- Sockets & threads -------------------------

// información que se la pasa el thread creado
typedef struct thread_info {
    int socket; // añadir los campos necesarios
} thread_info;

// función del thread
void *servicio(void *arg){
    int entero;
    thread_info *thinf = arg; // argumento recibido

    // si recv devuelve <=0 el cliente ha cortado la conexión;
    // recv puede devolver menos datos de los solicitados
    // (misma semántica que el "pipe"), pero con MSG_WAITALL espera hasta que
    // se hayan recibido todos los datos solicitados o haya habido un error.
    while (1) {
        // cada "petición" comienza con un entero
        if (recv(thinf->socket, &entero, sizeof(int), MSG_WAITALL)!=sizeof(int))
            break;
        entero = ntohl(entero);
        printf("Recibido entero: %d\n", entero);

        switch(entero){
            case CREATE_TOPIC:
                create_topic(thinf->socket);
                break;
            case NTOPICS:
                ntopics(thinf->socket);
                break;
            case SEND_MSG:
                send_msg(thinf->socket);
                break;
            case MSG_LENGTH:
                msg_length(thinf->socket);
                break;
            case END_OFFSET:
                end_offset(thinf->socket);
                break;
            case POLL:
                poll(thinf->socket);
                break;
            case COMMIT:
                commit(thinf->socket);
                break;
            case COMMITED:
                commited(thinf->socket);
                break;
        }
    }
    close(thinf->socket);
    return NULL;
}


// inicializa el socket y lo prepara para aceptar conexiones
static int init_socket_server(const char * port) {
    int s;
    struct sockaddr_in dir;
    int opcion=1;
    // socket stream para Internet: TCP
    if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("error creando socket");
        return -1;
    }
    // Para reutilizar puerto inmediatamente si se rearranca el servidor
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion))<0){
        perror("error en setsockopt");
        return -1;
    }
    // asocia el socket al puerto especificado
    dir.sin_addr.s_addr=INADDR_ANY;
    dir.sin_port=htons(atoi(port));
    dir.sin_family=PF_INET;
    if (bind(s, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
        perror("error en bind");
        close(s);
        return -1;
    }
    // establece el nº máx. de conexiones pendientes de aceptar
    if (listen(s, 5) < 0) {
        perror("error en listen");
        close(s);
        return -1;
    }
    return s;
}


//------------------------------- Main --------------------------------


int main(int argc, char *argv[]) {
    int s, s_conec;
    unsigned int tam_dir;
    struct sockaddr_in dir_cliente;

    if (argc!=2 && argc!=3) {
        fprintf(stderr, "Uso: %s puerto [dir_commited]\n", argv[0]);
        return 1;
    }
    if(argc == 3){
        offset_folder = malloc(strlen(argv[2]));
        strcpy(offset_folder, argv[2]);
    }

    // crea el mapa de temas
    topics = map_create(key_string, 0);

    // inicializa el socket y lo prepara para aceptar conexiones
    if ((s=init_socket_server(argv[1])) < 0) return -1;

    // prepara atributos adecuados para crear thread "detached"
    pthread_t thid;
    pthread_attr_t atrib_th;
    pthread_attr_init(&atrib_th); // evita pthread_join
    pthread_attr_setdetachstate(&atrib_th, PTHREAD_CREATE_DETACHED);

    while(1) {
        tam_dir=sizeof(dir_cliente);
        // acepta la conexión
        if ((s_conec=accept(s, (struct sockaddr *)&dir_cliente, &tam_dir))<0){
            perror("error en accept");
            close(s);
            return -1;
        }
        // crea el thread de servicio
        thread_info *thinf = malloc(sizeof(thread_info));
        thinf->socket=s_conec;
        pthread_create(&thid, &atrib_th, servicio, thinf);
    }
    close(s); // cierra el socket general

    return 0;
}


