/*
 * Incluya en este fichero todas las definiciones que pueden
 * necesitar compartir el broker y la biblioteca, si es que las hubiera.
 */

#ifndef _COMUN_H
#define _COMUN_H        1

#include <sys/uio.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/uio.h>

#define CREATE_TOPIC 0
#define NTOPICS 1
#define SEND_MSG 2
#define MSG_LENGTH 3
#define END_OFFSET 4
#define POLL 5
#define COMMIT 6
#define COMMITED 7

void send_prep_int(struct iovec *iov, int *nelem, int *entero_net);
void send_prep_arr(struct iovec *iov, int *nelem, int *longitud, char *arr);

#endif // _COMUN_H
