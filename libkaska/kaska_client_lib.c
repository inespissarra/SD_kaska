#include "comun.h"
#include "kaska.h"
#include "map.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

int s;
int first = 1;
map *subscribed_topics = NULL;
map_position *pos;

typedef struct subscribed_topic {
    char *name;
    int offset;
} subscribed_topic;

static int init_socket_client() {
    const char *host_server = getenv("BROKER_HOST");
    const char *port = getenv("BROKER_PORT");

    struct addrinfo *res;
    // socket stream para Internet: TCP
    if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("error creando socket");
        return -1;
    }
    // obtiene la dirección TCP remota
    if (getaddrinfo(host_server, port, NULL, &res)!=0) {
        perror("error en getaddrinfo");
        close(s);
        return -1;
    }
    // realiza la conexión
    if (connect(s, res->ai_addr, res->ai_addrlen) < 0) {
        perror("error en connect");
        close(s);
        return -1;
    }
    freeaddrinfo(res);
    return s;
}

static void free_subscribed_topic(void *entry, void *st){ //?????????
    free(((subscribed_topic*)st)->name);
    free(st);
}

//-------------------------------------------------------------------
//                        Funciones del API
//-------------------------------------------------------------------


// Crea el tema especificado.
// Devuelve 0 si OK y un valor negativo en caso de error.
int create_topic(char *topic) {
    if(first){
        init_socket_client();
        first = 0;
    }

    struct iovec iov[3]; // hay que enviar 3 elementos
    int nelem = 0;
    // preparo el envío del entero convertido a formato de red
    int entero_net = htonl(CREATE_TOPIC); // código de la operación
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // preparo el envío del string mandando antes su longitud
    int longitud_str = strlen(topic); // no incluye el carácter nulo
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; // no se usa & porque ya es un puntero
    iov[nelem++].iov_len=longitud_str;

    // modo de operación de los sockets asegura que si no hay error
    // se enviará todo (misma semántica que los "pipes")
    if (writev(s, iov, 3)<0) {
        perror("error en writev");
        close(s);
        return -1;
    }
    int res;
    // recibe un entero como respuesta
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("error en recv");
        close(s);
        return -1;
    }
    return ntohl(res);
}


// Devuelve cuántos temas existen en el sistema y un valor negativo
// en caso de error.
int ntopics(void) {
    if(first){
        init_socket_client();
        first = 0;
    }

    struct iovec iov[1]; // hay que enviar 1 elemento
    int nelem = 0;
    // preparo el envío del entero convertido a formato de red
    int entero_net = htonl(NTOPICS); // código de la operación
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // modo de operación de los sockets asegura que si no hay error
    // se enviará todo (misma semántica que los "pipes")
    if (writev(s, iov, 1)<0) {
        perror("error en writev");
        close(s);
        return -1;
    }
    int res;
    // recibe un entero como respuesta
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("error en recv");
        close(s);
        return -1;
    }
    return ntohl(res);
}


// SEGUNDA FASE: PRODUCIR/PUBLICAR

// Envía el mensaje al tema especificado; nótese la necesidad
// de indicar el tamaño ya que puede tener un contenido de tipo binario.
// Devuelve el offset si OK y un valor negativo en caso de error.
int send_msg(char *topic, int msg_size, void *msg) {
    if(first){
        init_socket_client();
        first = 0;
    }

    struct iovec iov[5]; // hay que enviar 5 elementos
    int nelem = 0;

    // preparo el envío del entero convertido a formato de red
    int entero_net = htonl(SEND_MSG); // código de la operación
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // preparo el envío del string mandando antes su longitud
    int longitud_str = strlen(topic); // no incluye el carácter nulo
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; // no se usa & porque ya es un puntero
    iov[nelem++].iov_len=longitud_str;

    // preparo el envío del mensaje mandando antes su longitud
    int longitud_msg_net = htonl(msg_size);
    iov[nelem].iov_base=&longitud_msg_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=msg; // no se usa & porque ya es un puntero
    iov[nelem++].iov_len=msg_size;

    // modo de operación de los sockets asegura que si no hay error
    // se enviará todo (misma semántica que los "pipes")
    if (writev(s, iov, 5)<0) {
        perror("error en writev");
        close(s);
        return -1;
    }
    int res;
    // recibe un entero como respuesta
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("error en recv");
        close(s);
        return -1;
    }
    return ntohl(res);
}
// Devuelve la longitud del mensaje almacenado en ese offset del tema indicado
// y un valor negativo en caso de error.
int msg_length(char *topic, int offset) {
    if(first){
        init_socket_client();
        first = 0;
    }

    struct iovec iov[4]; // hay que enviar 4 elementos
    int nelem = 0;
    // preparo el envío del entero convertido a formato de red
    int entero_net = htonl(MSG_LENGTH); // código de la operación
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // preparo el envío del string mandando antes su longitud
    int longitud_str = strlen(topic); // no incluye el carácter nulo
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; // no se usa & porque ya es un puntero
    iov[nelem++].iov_len=longitud_str;

    // preparo el envío del entero convertido a formato de red
    int entero_net2 = htonl(offset);
    iov[nelem].iov_base=&entero_net2;
    iov[nelem++].iov_len=sizeof(int);

    // modo de operación de los sockets asegura que si no hay error
    // se enviará todo (misma semántica que los "pipes")
    if (writev(s, iov, 4)<0) {
        perror("error en writev");
        close(s);
        return -1;
    }
    int res;
    // recibe un entero como respuesta
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("error en recv");
        close(s);
        return -1;
    }
    return ntohl(res);
}
// Obtiene el último offset asociado a un tema en el broker, que corresponde
// al del último mensaje enviado más uno y, dado que los mensajes se
// numeran desde 0, coincide con el número de mensajes asociados a ese tema.
// Devuelve ese offset si OK y un valor negativo en caso de error.
int end_offset(char *topic) {
    if(first){
        init_socket_client();
        first = 0;
    }

    struct iovec iov[3]; // hay que enviar 3 elementos
    int nelem = 0;
     // preparo el envío del entero convertido a formato de red
    int entero_net = htonl(END_OFFSET); // código de la operación
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // preparo el envío del string mandando antes su longitud
    int longitud_str = strlen(topic); // no incluye el carácter nulo
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; // no se usa & porque ya es un puntero
    iov[nelem++].iov_len=longitud_str;


    // modo de operación de los sockets asegura que si no hay error
    // se enviará todo (misma semántica que los "pipes")
    if (writev(s, iov, 3)<0) {
        perror("error en writev");
        close(s);
        return -1;
    }
    int res;
    // recibe un entero como respuesta
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("error en recv");
        close(s);
        return -1;
    }
    return ntohl(res);
}


// TERCERA FASE: SUBSCRIPCIÓN

// Se suscribe al conjunto de temas recibidos. No permite suscripción
// incremental: hay que especificar todos los temas de una vez.
// Si un tema no existe o está repetido en la lista simplemente se ignora.
// Devuelve el número de temas a los que realmente se ha suscrito
// y un valor negativo solo si ya estaba suscrito a algún tema.
int subscribe(int ntopics, char **topics) {
    if(first){
        init_socket_client();
        first = 0;
    }

    if(subscribed_topics!=NULL) return -1;

    subscribed_topics = map_create(key_string, 0);
    // hay que habilitar una variable para guardar
    // la posición donde comienzan y terminan las iteraciones.
    pos = map_alloc_position(subscribed_topics);

    int count = 0;

    for (int i = 0; i < ntopics; i++) {
        int res = end_offset(topics[i]);
        int err = 0;
        map_get(subscribed_topics, topics[i], &err);
        if(res>=0 && err==-1){
            subscribed_topic *st = malloc(sizeof(subscribed_topic));
            st->name = malloc(strlen(topics[i]) + 1);
            strcpy(st->name, topics[i]);
            st->offset = res;
            map_put(subscribed_topics, st->name, st);
            count = count + 1;
        }
    }
    return count;
}

// Se da de baja de todos los temas suscritos.
// Devuelve 0 si OK y un valor negativo si no había suscripciones activas.
int unsubscribe(void) {
    if(first){
        init_socket_client();
        first = 0;
    }
    map_free_position(pos);
    int res = map_destroy(subscribed_topics, free_subscribed_topic);
    subscribed_topics = NULL;

    return res;
}


// Devuelve el offset del cliente para ese tema y un número negativo en
// caso de error.
int position(char *topic) {
    if(first){
        init_socket_client();
        first = 0;
    }
    int err = 0;
    subscribed_topic *st = map_get(subscribed_topics, topic, &err);
    if(err==-1) return -1;
    return st->offset;
}

// Modifica el offset del cliente para ese tema.
// Devuelve 0 si OK y un número negativo en caso de error.
int seek(char *topic, int offset) {
    if(first){
        init_socket_client();
        first = 0;
    }
    int err = 0;
    subscribed_topic *st = map_get(subscribed_topics, topic, &err);
    if(err==-1) return -1;
    st->offset = offset;
    return 0;
}

// CUARTA FASE: LEER MENSAJES

// Obtiene el siguiente mensaje destinado a este cliente; los dos parámetros
// son de salida.
// Devuelve el tamaño del mensaje (0 si no había mensaje)
// y un número negativo en caso de error.
int poll(char **topic, void **msg) {
    if(first){
        init_socket_client();
        first = 0;
    }
    map_iter *it;
    subscribed_topic *st;
    int found;
    int res = 0;

    if(subscribed_topics==NULL) return -1;

    it = map_iter_init(subscribed_topics, pos);
    for (found=0; it && !found && map_iter_has_next(it); map_iter_next(it)) {
        map_iter_value(it, NULL, (void **) &st);
        struct iovec iov[4]; // hay que enviar 4 elementos
        int nelem = 0;

        // preparo el envío del entero convertido a formato de red
        int entero_net = htonl(POLL); // código de la operación
        iov[nelem].iov_base=&entero_net;
        iov[nelem++].iov_len=sizeof(int);

        // preparo el envío del string mandando antes su longitud
        int longitud_str = strlen(st->name); // no incluye el carácter nulo
        int longitud_str_net = htonl(longitud_str);
        iov[nelem].iov_base=&longitud_str_net;
        iov[nelem++].iov_len=sizeof(int);
        iov[nelem].iov_base=st->name; // no se usa & porque ya es un puntero
        iov[nelem++].iov_len=longitud_str;

        // preparo el envío del entero convertido a formato de red
        int entero_net2 = htonl(st->offset); // código de la operación
        iov[nelem].iov_base=&entero_net2;
        iov[nelem++].iov_len=sizeof(int);

        // modo de operación de los sockets asegura que si no hay error
        // se enviará todo (misma semántica que los "pipes")
        if (writev(s, iov, 4)<0) {
            perror("error en writev");
            close(s);
            return -1;
        }
        // recibe un entero como respuesta
        if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
            perror("error en recv");
            close(s);
            return -1;
        }
        res = ntohl(res);
        if(res>0){
            *msg = malloc(res);
            // ahora sí llega el string
            if (recv(s, *msg, res, MSG_WAITALL)!=res) {
                perror("error en recv");
                close(s);
                return -1;
            }
            *topic = malloc(strlen(st->name) + 1);
            strcpy(*topic, st->name);
            found=1;
            st->offset += 1;
        }
    }

    // liberamos el iterador previo guardando su posición que será justo la
    // siguiente a la encontrada ya que se ha ejecutado después "map_iter_next"
    pos = map_iter_exit(it);

    if(found) return res;
    return 0;
}

// QUINTA FASE: COMMIT OFFSETS

// Cliente guarda el offset especificado para ese tema.
// Devuelve 0 si OK y un número negativo en caso de error.
int commit(char *client, char *topic, int offset) {
    if(first){
        init_socket_client();
        first = 0;
    }

    struct iovec iov[6]; // hay que enviar 4 elementos
    int nelem = 0;
    // preparo el envío del entero convertido a formato de red
    int entero_net = htonl(COMMIT); // código de la operación
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // preparo el envío del string mandando antes su longitud
    int longitud_str = strlen(client); // no incluye el carácter nulo
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=client; // no se usa & porque ya es un puntero
    iov[nelem++].iov_len=longitud_str;

    // preparo el envío del string mandando antes su longitud
    int longitud_str2 = strlen(topic); // no incluye el carácter nulo
    int longitud_str_net2 = htonl(longitud_str2);
    iov[nelem].iov_base=&longitud_str_net2;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; // no se usa & porque ya es un puntero
    iov[nelem++].iov_len=longitud_str2;

    // preparo el envío del entero convertido a formato de red
    int entero_net2 = htonl(offset); // código de la operación
    iov[nelem].iov_base=&entero_net2;
    iov[nelem++].iov_len=sizeof(int);

    // modo de operación de los sockets asegura que si no hay error
    // se enviará todo (misma semántica que los "pipes")
    if (writev(s, iov, 6)<0) {
        perror("error en writev");
        close(s);
        return -1;
    }

    int res;
    // recibe un entero como respuesta
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("error en recv");
        close(s);
        return -1;
    }

    return ntohl(res);
}

// Cliente obtiene el offset guardado para ese tema.
// Devuelve el offset y un número negativo en caso de error.
int commited(char *client, char *topic) {
    if(first){
        init_socket_client();
        first = 0;
    }

    struct iovec iov[5]; // hay que enviar 4 elementos
    int nelem = 0;
    // preparo el envío del entero convertido a formato de red
    int entero_net = htonl(COMMITED); // código de la operación
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // preparo el envío del string mandando antes su longitud
    int longitud_str = strlen(client); // no incluye el carácter nulo
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=client; // no se usa & porque ya es un puntero
    iov[nelem++].iov_len=longitud_str;

    // preparo el envío del string mandando antes su longitud
    int longitud_str2 = strlen(topic); // no incluye el carácter nulo
    int longitud_str_net2 = htonl(longitud_str2);
    iov[nelem].iov_base=&longitud_str_net2;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; // no se usa & porque ya es un puntero
    iov[nelem++].iov_len=longitud_str2;

    // modo de operación de los sockets asegura que si no hay error
    // se enviará todo (misma semántica que los "pipes")
    if (writev(s, iov, 5)<0) {
        perror("error en writev");
        close(s);
        return -1;
    }
    
    int res;
    // recibe un entero como respuesta
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("error en recv");
        close(s);
        return -1;
    }

    return ntohl(res);
}