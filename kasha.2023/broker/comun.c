/*
 * Incluya en este fichero todas las implementaciones que pueden
 * necesitar compartir el broker y la biblioteca, si es que las hubiera.
 */
#include "comun.h"

void send_prep_int(struct iovec *iov, int *nelem, int *entero_net){
    *entero_net = htonl(*entero_net);
    iov[*nelem].iov_base=entero_net;
    iov[(*nelem)++].iov_len=sizeof(int);
}

void send_prep_arr(struct iovec *iov, int *nelem, int *longitud, char *arr){
    int longitud_int = *longitud;
    send_prep_int(iov, nelem, longitud);
    iov[*nelem].iov_base=arr; // no se usa & porque ya es un puntero
    iov[(*nelem)++].iov_len=longitud_int;
}