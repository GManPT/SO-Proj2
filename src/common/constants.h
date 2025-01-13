// constantes partilhadas entre cliente e servidor
#define MAX_SESSION_COUNT 8 // Número max de sessões permitidas no sistema.
#define STATE_ACCESS_DELAY_US   // delay a aplicar no server
#define MAX_PIPE_PATH_LENGTH 40 // tamanho max do caminho do pipe
#define MAX_STRING_SIZE 40 // Tamanho max de uma string (usado para chaves ou valores).
#define MAX_NUMBER_SUB 10 // Número max de chaves que um cliente pode subscrever.
#define TEMP_FOLDER "/tmp/" // Caminho para a pasta temporária do sistema.
#define BUFFER_SIZE 122 // tamanho do buffer
#define MAX_RESPONSE_SIZE 3 // tamanho max da resposta
#define MAX_SIZE_OPCODE 2 // tamanho max do opcode
#define BUFFER_SIZE_UNS 43 // tamanho do buffer para unsubscribe
#define MAX_WRITE_SIZE_RESPONSE 301 // MAX_WRITE_SIZE + MAX_STRING_SIZE + , + ( + ) + \0