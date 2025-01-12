#ifndef COMMON_PROTOCOL_H
#define COMMON_PROTOCOL_H

// Opcodes for client-server communication
// estes opcodes sao usados num switch case para determinar o que fazer com a
// mensagem recebida no server usam estes opcodes tambem nos clientes quando
// enviam mensagens para o server
enum {
  OP_CODE_CONNECT = 1,
  OP_CODE_DISCONNECT = 2,
  OP_CODE_SUBSCRIBE = 3,
  OP_CODE_UNSUBSCRIBE = 4,

};

#define OP_CODE_ERROR_CDU 1 // erro de connect, disconnect, unsubscribe
#define OP_CODE_ERROR_S 0 // erro de subscribe
#define OP_CODE_OK_CDU 0 // sucesso de connect, disconnect, unsubscribe
#define OP_CODE_OK_S 1 // sucesso de subscribe

#endif // COMMON_PROTOCOL_H
